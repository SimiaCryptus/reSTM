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
import stm.collection.TreeCollection.TreeCollectionNode
import stm.task.Task.{TaskResult, TaskSuccess}
import stm.task.{StmExecutionQueue, Task}
import storage.Restm.PointerType
import storage.{Restm, TransactionConflict}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Random


object TreeCollection {

  def apply[T]()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new TreeCollection(STMPtr.dynamicSync[Option[TreeCollectionNode[T]]](None))

  case class TreeCollectionNode[T]
  (
    value: T,
    left: Option[STMPtr[TreeCollectionNode[T]]] = None,
    right: Option[STMPtr[TreeCollectionNode[T]]] = None
  ) {

    def apxSize(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Long] = {
      if (right.isEmpty && left.isEmpty) {
        Future.successful(1)
      } else if (right.isDefined && left.isDefined) {
        val childs = (if (Random.nextBoolean()) List(left, right) else List(right, left)).map(_.get)
        try {
          childs(0).read().flatMap(_.apxSize).map(_ * 2)
        } catch {
          case _: TransactionConflict =>
            childs(1).read().flatMap(_.apxSize).map(_ * 2)
        }
      } else {
        left.orElse(right).map(_.read().flatMap(_.apxSize).map(_ * 2)).get
      }
    }

    def min()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[T] = {
      right.map(_.read.flatMap(_.min)).getOrElse(Future.successful(value))
    }

    def max()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[T] = {
      left.map(_.read.flatMap(_.max)).getOrElse(Future.successful(value))
    }

    def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[(Option[TreeCollectionNode[T]], T)] = {
      val childResult: Option[Future[(Option[TreeCollectionNode[T]], T)]] = if (Random.nextBoolean()) {
        left.map(ptr => {
          ptr.read.flatMap(_.get()).flatMap(f => {
            val (newVal, revVal) = f
            val result: Future[(Option[TreeCollectionNode[T]], T)] = newVal.map(newVal => {
              ptr.write(newVal).map(_ => (Option(TreeCollectionNode.this), revVal))
            }).getOrElse({
              Future.successful((Option(TreeCollectionNode.this.copy(left = None)), revVal))
            })
            result
          })
        }).orElse(right.map(ptr => {
          val result: Future[(Option[TreeCollectionNode[T]], T)] = ptr.read.flatMap(_.get()).flatMap(f => {
            val (newVal, revVal) = f
            newVal.map(newVal => {
              ptr.write(newVal).map(_ => (Option(TreeCollectionNode.this), revVal))
            }).getOrElse({
              Future.successful((Option(TreeCollectionNode.this.copy(right = None)), revVal))
            })
          })
          result
        }))
      } else {
        right.map(ptr => {
          val result: Future[(Option[TreeCollectionNode[T]], T)] = ptr.read.flatMap(_.get()).flatMap(f => {
            val (newVal, revVal) = f
            newVal.map(newVal => {
              ptr.write(newVal).map(_ => (Option(TreeCollectionNode.this), revVal))
            }).getOrElse({
              Future.successful((Option(TreeCollectionNode.this.copy(right = None)), revVal))
            })
          })
          result
        }).orElse(left.map(ptr => {
          val result = ptr.read.flatMap(_.get()).flatMap(f => {
            val (newVal, revVal) = f
            newVal.map(newVal => {
              ptr.write(newVal).map(_ => (Option(TreeCollectionNode.this), revVal))
            }).getOrElse({
              Future.successful((Option(TreeCollectionNode.this.copy(left = None)), revVal))
            })
          })
          result
        }))
      }
      childResult.getOrElse(Future.successful((None, value)))
    }

    def +=(newValue: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[TreeCollectionNode[T]] = {
      if (Random.nextBoolean()) {
        left.map(leftPtr => {
          leftPtr.read.flatMap(_ += newValue).flatMap(leftPtr.write).map(_ => TreeCollectionNode.this)
        }).getOrElse({
          STMPtr.dynamic(TreeCollectionNode(newValue)).map(x => this.copy(left = Option(x)))
        })
      } else {
        right.map(rightPtr => {
          rightPtr.read.flatMap(_ += newValue).flatMap(rightPtr.write).map(_ => TreeCollectionNode.this)
        }).getOrElse({
          STMPtr.dynamic(TreeCollectionNode(newValue)).map(x => this.copy(right = Option(x)))
        })
      }
    }

    override def hashCode(): Int = equalityFields.hashCode()

    private def equalityFields = List(value, left, right)

    override def equals(obj: scala.Any): Boolean = obj match {
      case x: TreeCollectionNode[_] => x.equalityFields == equalityFields
      case _ => false
    }

    def sortTask(cluster: Restm, executionContext: ExecutionContext)(implicit ordering: Ordering[T]): Task[SimpleLinkedList[T]] = {
      implicit val _cluster = cluster
      implicit val _executionContext = executionContext
      StmExecutionQueue.get().atomic.sync.add(sort())
    }

    def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Stream[T]] = {
      Future.sequence(List(
        right.map(_.read().flatMap[Stream[T]](x => x.stream()(ctx, executionContext, classTag))).getOrElse(Future.successful(Stream.empty)),
        Future.successful(Stream(value)),
        left.map(_.read().flatMap[Stream[T]](x => x.stream()(ctx, executionContext, classTag))).getOrElse(Future.successful(Stream.empty))
      )).map(_.reduce(_ ++ _))
    }

    def sort()(cluster: Restm, executionContext: ExecutionContext)(implicit ordering: Ordering[T]): TaskResult[SimpleLinkedList[T]] = {
      val tasks: List[Task[SimpleLinkedList[T]]] = {
        implicit val _cluster = cluster
        implicit val _executionContext = executionContext
        val leftList: Option[Task[SimpleLinkedList[T]]] = left.flatMap(_.atomic.sync.readOpt).map(_.sortTask(cluster, executionContext))
        val rightList: Option[Task[SimpleLinkedList[T]]] = right.flatMap(_.atomic.sync.readOpt).map(_.sortTask(cluster, executionContext))
        List(leftList, rightList).filter(_.isDefined).map(_.get)
      }
      Task.TaskContinue(newFunction = (cluster, executionContext) => {
        implicit val _cluster = cluster
        implicit val _executionContext = executionContext
        val sources = tasks.map(_.atomic().sync.result())

        def read(list: SimpleLinkedList[T]): Option[(T, Option[SimpleLinkedList[T]])] = list.atomic().sync.remove().map(_ -> Option(list))

        var cursors = sources.map(list => read(list)).filter(_.isDefined).map(_.get) ++ List(value -> None)
        val result = SimpleLinkedList.static[T](new PointerType)
        while (cursors.nonEmpty) {
          val (nextValue, optList) = cursors.minBy(_._1)
          result.atomic().sync.add(nextValue)
          cursors = cursors.filterNot(_._2 == optList)
          cursors = optList.flatMap(list => read(list)).map(cursors ++ List(_)).getOrElse(cursors)
        }
        TaskSuccess(result)
      }, queue = StmExecutionQueue.get(), newTriggers = tasks)
    }

  }

}

class TreeCollection[T](val rootPtr: STMPtr[Option[TreeCollectionNode[T]]]) {

  def this(ptr: PointerType) = this(new STMPtr[Option[TreeCollectionNode[T]]](ptr))

  def atomic(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority, maxRetries)

  def sync(duration: Duration) = new SyncApi(duration)

  def sync = new SyncApi(10.seconds)

  def add(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
    rootPtr.readOpt().map(_.flatten).flatMap(prev => {
      prev.map(r => r += value).getOrElse(Future.successful(new TreeCollectionNode[T](value)))
    }).flatMap(newRootData => rootPtr.write(Option(newRootData)))
  }

  def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
    rootPtr.readOpt().map(_.flatten).flatMap(value => {
      value.map(_.get()).map(_.flatMap(newRootData => {
        rootPtr.write(newRootData._1).map(_ => Option(newRootData._2))
      })).getOrElse(Future.successful(None))
    })
  }

  def min(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Object] = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(_.min()).getOrElse(None)
    })
  }

  def max(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Object] = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(_.max()).getOrElse(None)
    })
  }

  def sort()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, ordering: Ordering[T]): Future[Task[SimpleLinkedList[T]]] = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(_.sortTask(ctx.cluster, executionContext)(ordering)).get
    })
  }

  def apxSize()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Long] = {
    rootPtr.readOpt().map(_.flatten).flatMap(_.map(_.apxSize).getOrElse(Future.successful(0)))
  }

  def size()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Int] = {
    rootPtr.readOpt().map(_.flatten).flatMap(_.map(_.stream().map(_.size)).getOrElse(Future.successful(0)))
  }

  def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Stream[T]] = {
    rootPtr.readOpt().map(_.flatten).flatMap(x => {
      x.map(_.stream()).getOrElse(Future.successful(Stream.empty))
    })
  }

  private def this() = this(new PointerType)

  class AtomicApi(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority, maxRetries) {

    def sync(duration: Duration) = new SyncApi(duration)

    def sync = new SyncApi(10.seconds)

    def add(key: T): Future[Unit.type] = atomic {
      TreeCollection.this.add(key)(_, executionContext).map(_ => Unit)
    }

    def get(): Future[Option[T]] = atomic {
      TreeCollection.this.get()(_, executionContext)
    }

    def apxSize(): Future[Long] = atomic {
      TreeCollection.this.apxSize()(_, executionContext)
    }

    def sort()(implicit ordering: Ordering[T]): Future[Task[SimpleLinkedList[T]]] = atomic {
      TreeCollection.this.sort()(_, executionContext, ordering)
    }

    def toList()(implicit classTag: ClassTag[T]): Future[List[T]] = {
      atomic { (ctx: STMTxnCtx) => {
        TreeCollection.this.stream()(ctx, executionContext, classTag).map(_.toList)
      }
      }
    }

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(key: T): Unit.type = sync {
        AtomicApi.this.add(key)
      }

      def get(): Option[T] = sync {
        AtomicApi.this.get()
      }

      def toList()(implicit classTag: ClassTag[T]): List[T] = sync {
        AtomicApi.this.toList()
      }

      def apxSize(): Long = sync {
        AtomicApi.this.apxSize()
      }

      def sort()(implicit ordering: Ordering[T]): Task[SimpleLinkedList[T]] = sync {
        AtomicApi.this.sort()
      }
    }

  }

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def add(key: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Unit = sync {
      TreeCollection.this.add(key)
    }

    def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Option[T] = sync {
      TreeCollection.this.get()
    }

    def apxSize()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Long = sync {
      TreeCollection.this.apxSize()
    }

    def size()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Int = sync {
      TreeCollection.this.size()
    }

    def sort()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, ordering: Ordering[T]): Task[SimpleLinkedList[T]] = sync {
      TreeCollection.this.sort()
    }

    def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Stream[T] = sync {
      TreeCollection.this.stream()
    }
  }

}

