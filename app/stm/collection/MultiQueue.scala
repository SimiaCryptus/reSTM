package stm.collection

import stm._
import stm.collection.MultiQueue.MultiQueueData
import storage.Restm.PointerType
import storage.{Restm, TransactionConflict}

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Random

object MultiQueue {
  case class MultiQueueData[T]
  (
    queues : List[SimpleLinkedList[T]] = List.empty
  ) {
    def add(value:T, self: STMPtr[MultiQueue.MultiQueueData[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
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
    def take(self: STMPtr[MultiQueue.MultiQueueData[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
      take(self, queues.size)
    }
    def take(self: STMPtr[MultiQueue.MultiQueueData[T]], minEmpty : Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
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
    def grow(self: STMPtr[MultiQueue.MultiQueueData[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
      SimpleLinkedList.create[T].map((newList: SimpleLinkedList[T]) => copy(queues=queues ++ List(newList))).flatMap(self.write(_))
    }
  }
  def create[T](size: Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[MultiQueue[T]] = {
    Future.sequence((1 to size).map(i=>SimpleLinkedList.create[T])).flatMap(queues=>{
      STMPtr.dynamic(new MultiQueue.MultiQueueData[T](queues.toList)).map(new MultiQueue(_))
    })
  }

  def createSync[T](size: Int)(implicit cluster: Restm, executionContext: ExecutionContext): MultiQueue[T] =
    Await.result(new STMTxn[MultiQueue[T]] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[MultiQueue[T]] = {
        create[T](size)
      }
    }.txnRun(cluster),60.seconds)
}

class MultiQueue[T](rootPtr: STMPtr[MultiQueue.MultiQueueData[T]]) {
  private def this() = this(null:STMPtr[MultiQueue.MultiQueueData[T]])
  def id = rootPtr.id.toString


  def this(ptr:PointerType) = this(new STMPtr[MultiQueue.MultiQueueData[T]](ptr))

  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {
    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(value:T)(implicit classTag: ClassTag[T]) = sync { AtomicApi.this.add(value) }
      def take()(implicit classTag: ClassTag[T]): Option[T] = sync { AtomicApi.this.take() }
      def take(minEmpty : Int)(implicit classTag: ClassTag[T]): Option[T] = sync { AtomicApi.this.take(minEmpty) }
      def grow()(implicit classTag: ClassTag[T]) = sync { AtomicApi.this.grow() }
      def stream()(implicit classTag: ClassTag[T]): Stream[T] = sync { AtomicApi.this.stream() }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)
    def add(value:T)(implicit classTag: ClassTag[T]) = atomic { MultiQueue.this.add(value)(_,executionContext).map(_ => Unit) }
    def take()(implicit classTag: ClassTag[T]): Future[Option[T]] = atomic { MultiQueue.this.take()(_,executionContext) }
    def take(minEmpty : Int)(implicit classTag: ClassTag[T]): Future[Option[T]] = atomic { MultiQueue.this.take(minEmpty)(_,executionContext) }
    def grow()(implicit classTag: ClassTag[T]) = atomic { MultiQueue.this.grow()(_,executionContext).map(_ => Unit) }

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
    def add(value:T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { MultiQueue.this.add(value) }
    def take()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Option[T] = sync { MultiQueue.this.take() }
    def take(minEmpty : Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Option[T] = sync { MultiQueue.this.take(minEmpty) }
    def grow()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { MultiQueue.this.grow() }
    def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { MultiQueue.this.stream() }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)

  def add(value:T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
    getInner().flatMap(inner => {
      inner.add(value,rootPtr)
    })
  }

  def take(minEmpty : Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
    getInner().flatMap(inner => {
      inner.take(rootPtr, minEmpty)
    })
  }

  def take()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
    getInner().flatMap(inner => {
      inner.take(rootPtr)
    })
  }

  def grow()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
    getInner().flatMap(inner => {
      inner.grow(rootPtr)
    })
  }

  private def getInner()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[MultiQueueData[T]] = {
    rootPtr.readOpt().map(_.orElse(Option(new MultiQueue.MultiQueueData[T]()))).map(_.get)
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

