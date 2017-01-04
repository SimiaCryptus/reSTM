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

package stm.task

import stm._
import stm.collection.{LinkedList, ScalarArray, TreeSet}
import stm.task.TaskQueue.MultiQueueData
import storage.Restm.PointerType
import storage.{Restm, TransactionConflict}

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.Random


trait Identifiable {
  def id: String
}

object TaskQueue {
  private implicit def executionContext = StmPool.executionContext

  def createSync[T <: Identifiable](size: Int)(implicit cluster: Restm): TaskQueue[T] =
    Await.result(new STMTxn[TaskQueue[T]] {
      override def txnLogic()(implicit ctx: STMTxnCtx): Future[TaskQueue[T]] = {
        create[T](size)
      }
    }.txnRun(cluster), 60.seconds)

  def create[T <: Identifiable](size: Int)(implicit ctx: STMTxnCtx): Future[TaskQueue[T]] = {
    createInnerData[T](size).flatMap(STMPtr.dynamic(_)).map(new TaskQueue(_))
  }

  private def createInnerData[T <: Identifiable](size: Int)(implicit ctx: STMTxnCtx): Future[MultiQueueData[T]] = {
    Future.sequence((1 to size).map(_ => LinkedList.create[T])).flatMap(queues => {
      ScalarArray.create(size).flatMap((counter: ScalarArray) => {
        TreeSet.create[String]().map((index: TreeSet[String]) => {
          new TaskQueue.MultiQueueData[T](counter, index, queues.toList)
        })
      })
    })
  }

  case class MultiQueueData[T <: Identifiable]
  (
    size: ScalarArray,
    index: TreeSet[String],
    queues: List[LinkedList[T]] = List.empty
  ) {
    def add(value: T, self: STMPtr[TaskQueue.MultiQueueData[T]])(implicit ctx: STMTxnCtx): Future[Unit] = {
      val shuffledLists = queues.map(_ -> Random.nextDouble()).sortBy(_._2).map(_._1)

      def add(list: Seq[LinkedList[T]] = shuffledLists): Future[Unit] = {
        if (list.isEmpty) throw new TransactionConflict("Could not lock any queue") else {
          val head = list.head
          val tail: Seq[LinkedList[T]] = list.tail
          head.lock().flatMap(locked => {
            if (locked) {
              head.add(value)
            } else {
              add(tail)
            }
          })
        }
      }

      add()
    }

    def take(self: STMPtr[TaskQueue.MultiQueueData[T]])(implicit ctx: STMTxnCtx): Future[Option[T]] = {
      take(self, queues.size)
    }

    def take(self: STMPtr[TaskQueue.MultiQueueData[T]], minEmpty: Int)(implicit ctx: STMTxnCtx): Future[Option[T]] = {
      require(queues.size >= minEmpty)
      val shuffledLists = queues.map(_ -> Random.nextDouble()).sortBy(_._2).map(_._1)

      def take(list: List[LinkedList[T]] = shuffledLists, _minEmpty: Int = minEmpty): Future[Option[T]] = {
        if (list.isEmpty) {
          throw new TransactionConflict(s"Could not confirm emptiness of $minEmpty")
        } else {
          list.head.lock().flatMap(locked => {
            if (locked) {
              list.head.remove().flatMap((opt: Option[T]) => {
                opt.map(_ => Future.successful(opt)).getOrElse({
                  if (_minEmpty <= 1) {
                    Future.successful(None)
                  } else {
                    take(list.tail, _minEmpty - 1)
                  }
                })
              })
            } else {
              take(list.tail, _minEmpty)
            }
          })
        }
      }

      take()
    }

    def grow(self: STMPtr[TaskQueue.MultiQueueData[T]])(implicit ctx: STMTxnCtx): Future[Unit] = {
      LinkedList.create[T].map((newList: LinkedList[T]) => copy(queues = queues ++ List(newList))).flatMap(self.write)
    }
  }

}

class TaskQueue[T <: Identifiable](rootPtr: STMPtr[TaskQueue.MultiQueueData[T]]) {
  private implicit def executionContext = StmPool.executionContext
  def id: String = rootPtr.id.toString

  def this(ptr: PointerType) = this(new STMPtr[TaskQueue.MultiQueueData[T]](ptr))

  def atomic(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm) = new AtomicApi(priority, maxRetries)

  def sync(duration: Duration) = new SyncApi(duration)

  def sync = new SyncApi(10.seconds)

  def add(value: T)(implicit ctx: STMTxnCtx): Future[Unit] = {
    getInner().flatMap(inner => {
//      inner.index.contains(value.id).map(x=>require(!x)).flatMap(_=>{})
      inner.add(value, rootPtr)
        .flatMap(x => inner.size.add(1.0).map(_ => x))
        .flatMap(x => inner.index.add(value.id).map(_ => x))
    })
  }

  def take(minEmpty: Int)(implicit ctx: STMTxnCtx): Future[Option[T]] = {
    getInner().flatMap(inner => {
      inner.take(rootPtr, minEmpty).flatMap(x => {
        if (x.isDefined) {
          inner.size.add(-1.0)
            .flatMap(_ => inner.index.remove(x.get.id))
            .map(_ => x)
        } else {
          Future.successful(x)
        }
      })
    })
  }

  def take()(implicit ctx: STMTxnCtx): Future[Option[T]] = {
    getInner().flatMap(inner => {
      inner.take(rootPtr).flatMap(x => {
        if (x.isDefined) {
          inner.size.add(-1.0).flatMap(_ => inner.index.remove(x.get.id).map(require).map(_ => x))
        } else {
          Future.successful(x)
        }
      })
    })
  }

  def grow()(implicit ctx: STMTxnCtx): Future[Unit] = {
    getInner().flatMap(inner => {
      inner.grow(rootPtr)
    })
  }

  def size()(implicit ctx: STMTxnCtx): Future[Int] = {
    getInner().flatMap(inner => {
      inner.size.get().map(_.toInt)
    })
  }

  private def getInner()(implicit ctx: STMTxnCtx): Future[MultiQueueData[T]] = {
    rootPtr.readOpt().flatMap(_.map(Future.successful).getOrElse(TaskQueue.createInnerData[T](8)
      .flatMap((x: MultiQueueData[T]) => rootPtr.write(x).map(_ => x))))
  }

  def contains(id: String)(implicit ctx: STMTxnCtx): Future[Boolean] = {
    getInner().flatMap(inner => {
      inner.index.contains(id)
    })
  }

  def stream()(implicit ctx: STMTxnCtx): Future[Stream[T]] = {
    val opt: Option[MultiQueueData[T]] = rootPtr.sync.readOpt
    opt.map(inner => {
      val subStreams: Future[List[Stream[T]]] = Future.sequence(inner.queues.map(_.stream()))
      val future: Future[Stream[T]] = subStreams.map(subStreams => {
        val reduceOption: Option[Stream[T]] = subStreams.reduceOption(_ ++ _)
        val orElse: Stream[T] = reduceOption.getOrElse(Stream.empty[T])
        orElse
      })
      future
    }).getOrElse(Future.successful(Stream.empty))
  }

  private def this() = this(null: STMPtr[TaskQueue.MultiQueueData[T]])

  class AtomicApi(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm) extends AtomicApiBase(priority, maxRetries) {

    def sync(duration: Duration) = new SyncApi(duration)

    def sync = new SyncApi(10.seconds)

    def add(value: T)(implicit classTag: ClassTag[T]): Future[Unit.type] = atomic {
      TaskQueue.this.add(value)(_).map(_ => Unit)
    }

    def take()(implicit classTag: ClassTag[T]): Future[Option[T]] = atomic {
      TaskQueue.this.take()(_)
    }

    def size()(implicit classTag: ClassTag[T]): Future[Int] = atomic {
      TaskQueue.this.size()(_)
    }

    def take(minEmpty: Int)(implicit classTag: ClassTag[T]): Future[Option[T]] = atomic {
      TaskQueue.this.take(minEmpty)(_)
    }

    def contains(id: String)(implicit classTag: ClassTag[T]): Future[Boolean] = atomic {
      TaskQueue.this.contains(id)(_)
    }

    def grow()(implicit classTag: ClassTag[T]): Future[Unit.type] = atomic {
      TaskQueue.this.grow()(_).map(_ => Unit)
    }

    def stream(timeout: Duration = 30.seconds)(implicit classTag: ClassTag[T]): Future[Stream[T]] = {
      val opt: Option[MultiQueueData[T]] = rootPtr.atomic.sync.readOpt
      opt.map(inner => {
        val subStreams: Future[List[Stream[T]]] = Future.sequence(inner.queues.map(_.atomic().stream(timeout)))
        val future: Future[Stream[T]] = subStreams.map(subStreams => {
          val reduceOption: Option[Stream[T]] = subStreams.reduceOption(_ ++ _)
          val orElse: Stream[T] = reduceOption.getOrElse(Stream.empty[T])
          orElse
        })
        future
      }).getOrElse(Future.successful(Stream.empty))
    }

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(value: T)(implicit classTag: ClassTag[T]): Unit.type = sync {
        AtomicApi.this.add(value)
      }

      def take()(implicit classTag: ClassTag[T]): Option[T] = sync {
        AtomicApi.this.take()
      }

      def size()(implicit classTag: ClassTag[T]): Int = sync {
        AtomicApi.this.size()
      }

      def take(minEmpty: Int)(implicit classTag: ClassTag[T]): Option[T] = sync {
        AtomicApi.this.take(minEmpty)
      }

      def contains(id: String)(implicit classTag: ClassTag[T]): Boolean = sync {
        AtomicApi.this.contains(id)
      }

      def grow()(implicit classTag: ClassTag[T]): Unit.type = sync {
        AtomicApi.this.grow()
      }

      def stream(timeout: Duration = 30.seconds)(implicit classTag: ClassTag[T]): Stream[T] = sync {
        AtomicApi.this.stream(duration)
      }
    }

  }

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def add(value: T)(implicit ctx: STMTxnCtx): Unit = sync {
      TaskQueue.this.add(value)
    }

    def take()(implicit ctx: STMTxnCtx): Option[T] = sync {
      TaskQueue.this.take()
    }

    def size()(implicit ctx: STMTxnCtx): Int = sync {
      TaskQueue.this.size()
    }

    def take(minEmpty: Int)(implicit ctx: STMTxnCtx): Option[T] = sync {
      TaskQueue.this.take(minEmpty)
    }

    def contains(id: String)(implicit ctx: STMTxnCtx): Boolean = sync {
      TaskQueue.this.contains(id)
    }

    def grow()(implicit ctx: STMTxnCtx): Unit = sync {
      TaskQueue.this.grow()
    }

    def stream()(implicit ctx: STMTxnCtx): Stream[T] = sync {
      TaskQueue.this.stream()
    }
  }

}

