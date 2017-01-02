package stm.collection

import stm._
import stm.collection.IdQueue.MultiQueueData
import storage.Restm.PointerType
import storage.{Restm, TransactionConflict}

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Random


trait Identifiable {
  def id : String
}

object IdQueue {
  case class MultiQueueData[T <: Identifiable]
  (
    size: DistributedScalar,
    index: TreeSet[String],
    queues : List[SimpleLinkedList[T]] = List.empty
  ) {
    def add(value:T, self: STMPtr[IdQueue.MultiQueueData[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
      val shuffledLists = queues.map(_->Random.nextDouble()).sortBy(_._2).map(_._1)
      def add(list : Seq[SimpleLinkedList[T]] = shuffledLists): Future[Unit] = {
        if(list.isEmpty) throw new TransactionConflict("Could not lock any queue") else {
          val head = list.head
          val tail: Seq[SimpleLinkedList[T]] = list.tail
          head.lock().flatMap(locked=>{
            if(locked) {
              head.add(value)
            } else {
              add(tail)
            }
          })
        }
      }
      add()
    }
    def take(self: STMPtr[IdQueue.MultiQueueData[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
      take(self, queues.size)
    }
    def take(self: STMPtr[IdQueue.MultiQueueData[T]], minEmpty : Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
      require(queues.size >= minEmpty)
      val shuffledLists = queues.map(_->Random.nextDouble()).sortBy(_._2).map(_._1)
      def take(list : List[SimpleLinkedList[T]] = shuffledLists, _minEmpty : Int = minEmpty): Future[Option[T]] = {
        if(list.isEmpty) {
          throw new TransactionConflict(s"Could not confirm emptiness of $minEmpty")
        } else {
          list.head.lock().flatMap(locked=>{
            if(locked) {
              list.head.remove().flatMap((opt: Option[T]) =>{
                opt.map(_=>Future.successful(opt)).getOrElse({
                  if(_minEmpty <= 1) {
                    Future.successful(None)
                  } else {
                    take(list.tail,_minEmpty-1)
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
    def grow(self: STMPtr[IdQueue.MultiQueueData[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
      SimpleLinkedList.create[T].map((newList: SimpleLinkedList[T]) => copy(queues=queues ++ List(newList))).flatMap(self.write)
    }
  }
  def create[T <: Identifiable](size: Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[IdQueue[T]] = {
    createInnerData[T](size).flatMap(STMPtr.dynamic(_)).map(new IdQueue(_))
  }

  private def createInnerData[T <: Identifiable](size: Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[MultiQueueData[T]] = {
    Future.sequence((1 to size).map(_ => SimpleLinkedList.create[T])).flatMap(queues => {
      DistributedScalar.create(size).flatMap((counter: DistributedScalar) => {
        TreeSet.create[String]().map((index: TreeSet[String]) => {
          new IdQueue.MultiQueueData[T](counter, index, queues.toList)
        })
      })
    })
  }

  def createSync[T <: Identifiable](size: Int)(implicit cluster: Restm, executionContext: ExecutionContext): IdQueue[T] =
    Await.result(new STMTxn[IdQueue[T]] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[IdQueue[T]] = {
        create[T](size)
      }
    }.txnRun(cluster),60.seconds)
}

class IdQueue[T <: Identifiable](rootPtr: STMPtr[IdQueue.MultiQueueData[T]]) {
  private def this() = this(null:STMPtr[IdQueue.MultiQueueData[T]])
  def id: String = rootPtr.id.toString


  def this(ptr:PointerType) = this(new STMPtr[IdQueue.MultiQueueData[T]](ptr))

  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {
    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(value:T)(implicit classTag: ClassTag[T]): Unit.type = sync { AtomicApi.this.add(value) }
      def take()(implicit classTag: ClassTag[T]): Option[T] = sync { AtomicApi.this.take() }
      def size()(implicit classTag: ClassTag[T]): Int = sync { AtomicApi.this.size() }
      def take(minEmpty : Int)(implicit classTag: ClassTag[T]): Option[T] = sync { AtomicApi.this.take(minEmpty) }
      def contains(id : String)(implicit classTag: ClassTag[T]): Boolean = sync { AtomicApi.this.contains(id) }
      def grow()(implicit classTag: ClassTag[T]): Unit.type = sync { AtomicApi.this.grow() }
      def stream()(implicit classTag: ClassTag[T]): Stream[T] = sync { AtomicApi.this.stream() }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)
    def add(value:T)(implicit classTag: ClassTag[T]): Future[Unit.type] = atomic { IdQueue.this.add(value)(_,executionContext).map(_ => Unit) }
    def take()(implicit classTag: ClassTag[T]): Future[Option[T]] = atomic { IdQueue.this.take()(_,executionContext) }
    def size()(implicit classTag: ClassTag[T]): Future[Int] = atomic { IdQueue.this.size()(_,executionContext) }
    def take(minEmpty : Int)(implicit classTag: ClassTag[T]): Future[Option[T]] = atomic { IdQueue.this.take(minEmpty)(_,executionContext) }
    def contains(id : String)(implicit classTag: ClassTag[T]): Future[Boolean] = atomic { IdQueue.this.contains(id)(_,executionContext) }
    def grow()(implicit classTag: ClassTag[T]): Future[Unit.type] = atomic { IdQueue.this.grow()(_,executionContext).map(_ => Unit) }

    def stream()(implicit classTag: ClassTag[T]): Future[Stream[T]] = {
      val opt: Option[MultiQueueData[T]] = rootPtr.atomic.sync.readOpt
      opt.map(inner=>{
        val subStreams: Future[List[Stream[T]]] = Future.sequence(inner.queues.map(_.atomic().stream()))
        val future: Future[Stream[T]] = subStreams.map(subStreams => {
          val reduceOption: Option[Stream[T]] = subStreams.reduceOption(_ ++ _)
          val orElse: Stream[T] = reduceOption.getOrElse(Stream.empty[T])
          orElse
        })
        future
      }).getOrElse(Future.successful(Stream.empty))
    }

  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority,maxRetries)
  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def add(value:T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Unit = sync { IdQueue.this.add(value) }
    def take()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Option[T] = sync { IdQueue.this.take() }
    def size()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Int = sync { IdQueue.this.size() }
    def take(minEmpty : Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Option[T] = sync { IdQueue.this.take(minEmpty) }
    def contains(id : String)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Boolean = sync { IdQueue.this.contains(id) }
    def grow()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Unit = sync { IdQueue.this.grow() }
    def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Stream[T] = sync { IdQueue.this.stream() }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)

  def add(value:T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
    getInner().flatMap(inner => {
//      inner.index.contains(value.id).map(x=>require(!x)).flatMap(_=>{
//      })
      inner.add(value,rootPtr)
        .flatMap(x=>inner.size.add(1.0).map(_=>x))
        .flatMap(x=>inner.index.add(value.id).map(_=>x))
    })
  }

  def take(minEmpty : Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
    getInner().flatMap(inner => {
      inner.take(rootPtr, minEmpty).flatMap(x=>{
        if(x.isDefined) {
          inner.size.add(-1.0)
            .flatMap(_=>inner.index.remove(x.get.id))
            .map(_ =>x)
        } else {
          Future.successful(x)
        }
      })
    })
  }

  def take()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
    getInner().flatMap(inner => {
      inner.take(rootPtr).flatMap(x=>{
        if(x.isDefined) {
          inner.size.add(-1.0).flatMap(_=>inner.index.remove(x.get.id).map(require).map(_=>x))
        } else {
          Future.successful(x)
        }
      })
    })
  }

  def grow()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
    getInner().flatMap(inner => {
      inner.grow(rootPtr)
    })
  }

  def size()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Int] = {
    getInner().flatMap(inner => {
      inner.size.get().map(_.toInt)
    })
  }

  def contains(id:String)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Boolean] = {
    getInner().flatMap(inner => {
      inner.index.contains(id)
    })
  }

  private def getInner()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[MultiQueueData[T]] = {
    rootPtr.readOpt().flatMap(_.map(Future.successful).getOrElse(IdQueue.createInnerData[T](8)
      .flatMap((x: MultiQueueData[T]) => rootPtr.write(x).map(_=>x))))
  }


  def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Stream[T]] = {
    val opt: Option[MultiQueueData[T]] = rootPtr.sync.readOpt
    opt.map(inner=>{
      val subStreams: Future[List[Stream[T]]] = Future.sequence(inner.queues.map(_.stream()))
      val future: Future[Stream[T]] = subStreams.map(subStreams => {
        val reduceOption: Option[Stream[T]] = subStreams.reduceOption(_ ++ _)
        val orElse: Stream[T] = reduceOption.getOrElse(Stream.empty[T])
        orElse
      })
      future
    }).getOrElse(Future.successful(Stream.empty))
  }

}

