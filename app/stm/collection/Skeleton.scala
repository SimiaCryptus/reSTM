package stm.collection

import stm._
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag

object Skeleton {
  case class SkeletonData[T]
  (
    // Primary mutable internal data here
  ) {
    // In-txn operations
    def sampleOperation(self: STMPtr[Skeleton.SkeletonData[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Unit] = {
      self.write(this)
    }
  }
  def create[T]()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Skeleton[T]] =
    STMPtr.dynamic(new Skeleton.SkeletonData[T]()).map(new Skeleton(_))

  def createSync[T]()(implicit cluster: Restm, executionContext: ExecutionContext, classTag: ClassTag[T]): Skeleton[T] =
    Await.result(new STMTxn[Skeleton[T]] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Skeleton[T]] = {
        create[T]()
      }
    }.txnRun(cluster),60.seconds)

}

class Skeleton[T](rootPtr: STMPtr[Skeleton.SkeletonData[T]]) {
  def id: String = rootPtr.id.toString

  def this(ptr:PointerType) = this(new STMPtr[Skeleton.SkeletonData[T]](ptr))

  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {
    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def sampleOperation()(implicit classTag: ClassTag[T]): Unit.type = sync { AtomicApi.this.sampleOperation() }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)
    def sampleOperation()(implicit classTag: ClassTag[T]): Future[Unit.type] = atomic { Skeleton.this.sampleOperation()(_,executionContext,classTag).map(_ => Unit) }
  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority,maxRetries)
  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def sampleOperation()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Unit] = sync { Skeleton.this.sampleOperation() }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)

  def sampleOperation()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Future[Unit]] = {
    getInner().map(inner => {
      inner.sampleOperation(rootPtr)
    })
  }

  private def getInner()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.orElse(Option(new Skeleton.SkeletonData[T]()))).map(_.get)
  }
}

