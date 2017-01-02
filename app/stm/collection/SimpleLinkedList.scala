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

import stm.{SyncApiBase, _}
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object SimpleLinkedList {
  def create[T](implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[SimpleLinkedList[T]] =
    STMPtr.dynamic[SimpleLinkedListHead[T]](new SimpleLinkedListHead[T]()).map(new SimpleLinkedList(_))

  def static[T](id: PointerType) = new SimpleLinkedList(new STMPtr[SimpleLinkedListHead[T]](id))
}

class SimpleLinkedList[T](rootPtr: STMPtr[SimpleLinkedListHead[T]]) {

  def id: String = rootPtr.id.toString

  def atomic(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority, maxRetries)

  def sync(duration: Duration)(implicit executionContext: ExecutionContext) = new SyncApi(duration)

  def sync(implicit executionContext: ExecutionContext) = new SyncApi(10.seconds)

  def size()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Int] = {
    stream().map(_.size)
  }

  def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Stream[T]] = {
    rootPtr.readOpt.map(_
      .flatMap(_.tail)
      .map(_.sync.readOpt.map(node => node.value -> node.next))
      .map(seed =>
        Stream.iterate(seed)((prev: Option[(T, Option[STMPtr[SimpleLinkedListNode[T]]])]) =>
          prev.get._2.flatMap((node: STMPtr[SimpleLinkedListNode[T]]) =>
            node.sync.readOpt.map(node => node.value -> node.next)))
          .takeWhile(_.isDefined).map(_.get._1))
      .getOrElse(Stream.empty))
  }

  def add(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
    rootPtr.readOpt().map(_.getOrElse(new SimpleLinkedListHead)).flatMap(_.add(value, rootPtr))
  }

  def remove()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
    rootPtr.readOpt().flatMap(_.map(head => head.remove(rootPtr)).getOrElse(Future.successful(None)))
  }

  def lock()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Boolean] = {
    rootPtr.lock()
  }

  class AtomicApi(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority, maxRetries) {
    def add(value: T): Future[Unit] = atomic { (ctx: STMTxnCtx) => SimpleLinkedList.this.add(value)(ctx, executionContext) }

    def remove(): Future[Option[T]] = atomic { (ctx: STMTxnCtx) => SimpleLinkedList.this.remove()(ctx, executionContext) }

    def size(): Future[Int] = atomic { (ctx: STMTxnCtx) => SimpleLinkedList.this.size()(ctx, executionContext) }

    def stream(timeout: Duration = 30.seconds)(implicit cluster: Restm, executionContext: ExecutionContext): Future[Stream[T]] = {
      rootPtr.atomic.readOpt.map(_
        .flatMap(_.tail)
        .map(tail => tail.atomic.sync.readOpt.map(node => node.value -> node.next))
        .map(seed => {
          Stream.iterate(seed)((prev: Option[(T, Option[STMPtr[SimpleLinkedListNode[T]]])]) => {
            prev.get._2.flatMap((node: STMPtr[SimpleLinkedListNode[T]]) =>
              node.atomic.sync(timeout).readOpt.map(node => node.value -> node.next))
          }).takeWhile(_.isDefined).map(_.get._1)
        }).getOrElse(Stream.empty))
    }

    def sync(duration: Duration) = new SyncApi(duration)

    def sync = new SyncApi(30.seconds)

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(value: T): Unit = sync {
        AtomicApi.this.add(value)
      }

      def remove(): Option[T] = sync {
        AtomicApi.this.remove()
      }

      def stream(timeout: Duration = 30.seconds): Stream[T] = sync {
        AtomicApi.this.stream(duration)
      }

      def size(): Int = sync {
        AtomicApi.this.size()
      }
    }

  }

  class SyncApi(duration: Duration)(implicit executionContext: ExecutionContext) extends SyncApiBase(duration) {
    def add(value: T)(implicit ctx: STMTxnCtx): Unit = sync {
      SimpleLinkedList.this.add(value)
    }

    def remove()(implicit ctx: STMTxnCtx): Option[T] = sync {
      SimpleLinkedList.this.remove()
    }

    def stream()(implicit ctx: STMTxnCtx): Stream[T] = sync {
      SimpleLinkedList.this.stream()
    }

    def size()(implicit ctx: STMTxnCtx): Int = sync {
      SimpleLinkedList.this.size()
    }
  }

}

private case class SimpleLinkedListHead[T]
(
  head: Option[STMPtr[SimpleLinkedListNode[T]]] = None,
  tail: Option[STMPtr[SimpleLinkedListNode[T]]] = None
) {
  def add(newValue: T, self: STMPtr[SimpleLinkedListHead[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
    val ifDefinedFuture: Option[Future[Unit]] = head.map(nodePtr => {
      nodePtr.read.flatMap(currentValue => {
        require(currentValue.next.isEmpty)
        val node = SimpleLinkedListNode(newValue, prev = Option(nodePtr), next = None)
        STMPtr.dynamic(node).flatMap(newPtr => {
          nodePtr.write(currentValue.copy(next = Option(newPtr)))
            .flatMap(_ => {
              self.write(SimpleLinkedListHead.this.copy(head = Option(newPtr)))
            })
        })
      })
    })
    ifDefinedFuture.getOrElse({
      require(tail.isEmpty)
      val ptrFuture = STMPtr.dynamic(SimpleLinkedListNode(newValue))
      ptrFuture.flatMap(newNode => self.write(copy(head = Option(newNode), tail = Option(newNode))))
    })
  }

  def remove(self: STMPtr[SimpleLinkedListHead[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
    if (tail.isDefined) {
      val tailPtr: STMPtr[SimpleLinkedListNode[T]] = tail.get
      tailPtr.read.flatMap(tailValue => {
        require(tailValue.prev.isEmpty)
        if (tailValue.next.isDefined) {
          val nextPtr = tailValue.next.get
          val writeFuture: Future[Unit] = nextPtr.read.map(nextValue => {
            require(nextValue.prev == tail)
            nextValue.copy(prev = None)
          }).flatMap(nextPtr.write)
            .flatMap(_ => self.write(copy(tail = Option(nextPtr))))
          writeFuture.map(_ => Option(tailValue.value))
        } else {
          require(tail == head, "List header seems to be corrupt")
          self.write(copy(tail = None, head = None)).map(_ => Option(tailValue.value))
        }
      })
    } else {
      require(head.isEmpty)
      Future.successful(None)
    }
  }
}

private case class SimpleLinkedListNode[T]
(
  value: T,
  next: Option[STMPtr[SimpleLinkedListNode[T]]] = None,
  prev: Option[STMPtr[SimpleLinkedListNode[T]]] = None
) {

  override def hashCode(): Int = equalityFields.hashCode()

  private def equalityFields = List(value, next, prev)

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: SimpleLinkedListNode[_] => x.equalityFields == equalityFields
    case _ => false
  }
}
