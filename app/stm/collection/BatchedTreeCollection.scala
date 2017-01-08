/*
 * Copyright (c) 2017 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package stm.collection

import stm._
import stm.collection.BatchedTreeCollection._
import storage.Restm
import storage.Restm.PointerType
import storage.types.KryoValue
import util.Util

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Random, Try}


object BatchedTreeCollection {
  private implicit def executionContext = StmPool.executionContext

  def apply[T]()(implicit ctx: STMTxnCtx) =
    new BatchedTreeCollection(STMPtr.dynamicSync[TreeCollectionNode[T]](null))

  def create[T]()(implicit ctx: STMTxnCtx): Future[BatchedTreeCollection[T]] =
    STMPtr.dynamic[TreeCollectionNode[T]](null).map(new BatchedTreeCollection(_))

  case class TreeCollectionNode[T]
  (
    parent: Option[STMPtr[TreeCollectionNode[T]]],
    value: STMPtr[KryoValue[List[T]]],
    left: Option[STMPtr[TreeCollectionNode[T]]] = None,
    right: Option[STMPtr[TreeCollectionNode[T]]] = None
  ) {

    def getCursorBlock(self: STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx): Future[(Long, KryoValue[List[T]])] = {
      val nextNodeFuture = nextNode(self)
      val valueFuture = value.read()
      valueFuture.flatMap(list => {
        nextNodeFuture.flatMap((nextNodeOptPtr: Option[STMPtr[TreeCollectionNode[T]]]) => {
          nextNodeOptPtr.map((nextPtr: STMPtr[TreeCollectionNode[T]]) => {
            nextPtr.read().flatMap(next => next.getTreeId(nextPtr))
              .map((nextId: Long) => nextId -> list)
          }).getOrElse(Future.successful(-1l -> list))
        })
      })
    }


    def apxSize(implicit ctx: STMTxnCtx): Future[Long] = {
      val child = if (Random.nextBoolean()) left.orElse(right) else right.orElse(left)
      child.map(_.read().flatMap(_.apxSize).map(_ * 2)).getOrElse(value.read().map(_.deserialize()).map(_.size))
    }

    def get(self: STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx): Future[List[T]] = {
      val (a, b) = if (Random.nextBoolean()) (left, right) else (right, left)
      a.map(ptr => {
        ptr.read.flatMap(_.get(ptr))
      }).orElse(b.map(ptr => {
        ptr.read.flatMap(_.get(ptr))
      })).getOrElse({
        unlinkParent(self)
          .flatMap(_ => self.delete())
          .flatMap(_ => value.read.map(_.deserialize().get))
      })
    }

    def add(newValue: List[T], self: STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx): Future[Unit] = {
      //println(s"Write ${newValue.size} items to $self")
      if (Random.nextBoolean()) {
        left.map(leftPtr => {
          leftPtr.read.flatMap(_.add(newValue, leftPtr))
        }).getOrElse({
          STMPtr.dynamic(KryoValue(newValue))
            .flatMap(ptr => STMPtr.dynamic(TreeCollectionNode(Some(self), ptr)))
            .flatMap(x => self.write(this.copy(left = Option(x))))
        })
      } else {
        right.map(rightPtr => {
          rightPtr.read.flatMap(_.add(newValue, rightPtr))
        }).getOrElse({
          STMPtr.dynamic(KryoValue(newValue))
            .flatMap(ptr => STMPtr.dynamic(TreeCollectionNode(Some(self), ptr)))
            .flatMap(x => self.write(this.copy(right = Option(x))))
        })
      }
    }

    override def hashCode(): Int = equalityFields.hashCode()

    override def equals(obj: scala.Any): Boolean = obj match {
      case x: TreeCollectionNode[_] => x.equalityFields == equalityFields
      case _ => false
    }

    private def equalityFields = List(value, left, right)

    def leftChild(self: STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx): Future[STMPtr[TreeCollectionNode[T]]] = {
      left.map(left => {
        left.read().flatMap(_.leftChild(left))
      }).getOrElse(Future.successful(self))
    }

    def rightParent(self: STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx): Future[Option[STMPtr[TreeCollectionNode[T]]]] = {
      parent.map(parentPtr => {
        parentPtr.read().flatMap(parentValue => {
          if (parentValue.left == Option(self)) {
            Future.successful(parent)
          } else {
            parentValue.rightParent(parentPtr)
          }
        })
      }).getOrElse(Future.successful(None))
    }

    def nextNode(self: STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx): Future[Option[STMPtr[TreeCollectionNode[T]]]] = {
      right.map(rightPtr => rightPtr.read().flatMap(_.leftChild(rightPtr).map(Option(_)))).getOrElse(rightParent(self))
    }

    private[BatchedTreeCollection] def getByTreePath(self: STMPtr[TreeCollectionNode[T]], path: List[Int])(implicit ctx: STMTxnCtx): Future[STMPtr[TreeCollectionNode[T]]] = {
      if (path.isEmpty) Future.successful(self)
      else {
        path.head match {
          case 0 =>
            left.get.read().flatMap(_.getByTreePath(left.get, path.tail))
          case 1 =>
            right.get.read().flatMap(_.getByTreePath(right.get, path.tail))
        }
      }
    }

    def getByTreeId(cursor: Long, self: STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx): Future[STMPtr[TreeCollectionNode[T]]] = {
      require(0 <= cursor)
      val path = Util.toDigits(cursor, 2).tail
      val fromTry: Future[STMPtr[TreeCollectionNode[T]]] = Future.fromTry(Try {
        getByTreePath(self, path)
      }).flatMap(x⇒x)
      fromTry.recoverWith({
        case e : NoSuchElementException ⇒ Future.failed(new RuntimeException(s"Cannot locate node $cursor", e))
      })
    }

    def getTreeBit(node: STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx): Int = {
      if (left.exists(_ == node)) 0
      else if (right.exists(_ == node)) 1
      else throw new RuntimeException()
    }

    def getTreeId(self: STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx): Future[Long] = {
      parent.map(parentPtr => parentPtr.read().flatMap(parentNode => {
        parentNode.getTreeId(parent.get).map(parentId => {
          val bit: Int = parentNode.getTreeBit(self)
          parentId * 2 + bit
        })
      })).getOrElse(Future.successful(1l)).map((id: Long) => {
        if (id < 0) throw new RuntimeException("Node is too deep to calculate id")
        id
      })
    }

    private def unlinkParent(self: STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx): Future[Unit] = {
      parent.map(parentPtr => {
        parentPtr.read().flatMap(parentValue => {
          if (parentValue.left == Option(self)) {
            parentPtr.write(parentValue.copy(left = None))
          } else if (parentValue.right == Option(self)) {
            parentPtr.write(parentValue.copy(right = None))
          } else {
            throw new RuntimeException("Child Link Not Found")
          }
        })
      }).getOrElse(Future.successful(Unit))
    }


  }

}

class BatchedTreeCollection[T](val rootPtr: STMPtr[TreeCollectionNode[T]]) {
  private implicit def executionContext = StmPool.executionContext

  def this(ptr: PointerType) = this(new STMPtr[TreeCollectionNode[T]](ptr))

  def atomic(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm) = new AtomicApi(priority, maxRetries)

  def sync(duration: Duration) = new SyncApi(duration)

  def add(value: List[T])(implicit ctx: STMTxnCtx): Future[Unit] = {
    rootPtr.readOpt().flatMap(rootOpt => {
      rootOpt.map(root => root.add(value, rootPtr))
        .getOrElse({
          STMPtr.dynamic(KryoValue(value))
            .map(new TreeCollectionNode[T](None, _))
            .flatMap(
              rootPtr.write)
        })
    })
  }

  def nextBlock(cursor: Long)(implicit ctx: STMTxnCtx): Future[(Long, KryoValue[List[T]])] = {
    if (cursor < 0) {
      Future.successful((cursor - 1) -> KryoValue.empty)
    } else {
      rootPtr.readOpt().flatMap(rootOpt => {
        rootOpt.map(root => {
          if (cursor == 0) {
            root.leftChild(rootPtr).flatMap((nodePtr: STMPtr[TreeCollectionNode[T]]) => {
              nodePtr.read().flatMap(node => {
                node.getCursorBlock(nodePtr)
              })
            })
          } else {
            root.getByTreeId(cursor, rootPtr).flatMap((nodePtr: STMPtr[TreeCollectionNode[T]]) => {
              nodePtr.read().flatMap(node => {
                node.getCursorBlock(nodePtr)
              })
            })
          }
        }).getOrElse({
          Future.successful(-1l -> KryoValue.empty)
        })
      })
    }
  }

  def get()(implicit ctx: STMTxnCtx): Future[Option[List[T]]] = {
    rootPtr.readOpt().flatMap(rootOpt => {
      rootOpt.map(root => root.get(rootPtr).map(Option(_)))
        .getOrElse(Future.successful(None))
    })
  }

  def apxSize()(implicit ctx: STMTxnCtx): Future[Long] = {
    rootPtr.readOpt().flatMap(_.map(_.apxSize).getOrElse(Future.successful(0)))
  }

  def size()(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Long] = {
    Future.successful(stream().size)
  }

  def stream()(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Stream[T] = {
    Stream.iterate((0l, KryoValue.empty[List[T]]))(t => sync.nextBlock(t._1))
      .flatMap(x ⇒ x._2.deserialize().map(x._1 → _)).takeWhile(_._1 > -2).flatMap(_._2)
  }

  def sync = new SyncApi(30.seconds)

  //noinspection ScalaUnusedSymbol
  private def this() = this(new PointerType)

  class AtomicApi(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm) extends AtomicApiBase(priority, maxRetries) {

    def sync(duration: Duration) = new SyncApi(duration)

    def size()(implicit classTag: ClassTag[T]): Future[Long] = atomic {
      BatchedTreeCollection.this.size()(_, classTag)
    }

    def get(): Future[Option[List[T]]] = atomic {
      BatchedTreeCollection.this.get()(_)
    }

    def add(value: List[T]): Future[Unit] = atomic {
      BatchedTreeCollection.this.add(value)(_)
    }

    def apxSize(): Future[Long] = atomic {
      BatchedTreeCollection.this.apxSize()(_)
    }

    def nextBlock(cursor: Long): Future[(Long, KryoValue[List[T]])] = atomic {
      BatchedTreeCollection.this.nextBlock(cursor)(_)
    }

    def stream()(implicit classTag: ClassTag[T]): Stream[T] = {
      rawStream.flatMap(x⇒x.deserialize().getOrElse(List.empty))
    }

    def rawStream()(implicit classTag: ClassTag[T]) = {
      Stream.iterate((0l, KryoValue.empty[List[T]]))(t => sync.nextBlock(t._1)).takeWhile(_._1 > -2).map(_._2)
    }

    def sync = new SyncApi(10.seconds)

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def get(): Option[List[T]] = sync {
        AtomicApi.this.get()
      }

      def size()(implicit classTag: ClassTag[T]): Long = sync {
        AtomicApi.this.size()
      }

      def apxSize(): Long = sync {
        AtomicApi.this.apxSize()
      }

      def nextBlock(cursor: Long): (Long, KryoValue[List[T]]) = sync {
        AtomicApi.this.nextBlock(cursor)
      }

      def add(value: List[T]): Unit = sync {
        AtomicApi.this.add(value)
      }

      def stream()(implicit classTag: ClassTag[T]): Stream[T] = AtomicApi.this.stream()
    }

  }

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def get()(implicit ctx: STMTxnCtx): Option[List[T]] = sync {
      BatchedTreeCollection.this.get()
    }

    def apxSize()(implicit ctx: STMTxnCtx): Long = sync {
      BatchedTreeCollection.this.apxSize()
    }

    def size()(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Long = sync {
      BatchedTreeCollection.this.size()
    }

    def stream()(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Stream[T] = BatchedTreeCollection.this.stream()

    def add(value: List[T])(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Unit = sync {
      BatchedTreeCollection.this.add(value)
    }

    def nextBlock(cursor: Long)(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): (Long, KryoValue[List[T]]) = sync {
      BatchedTreeCollection.this.nextBlock(cursor)
    }
  }

}

