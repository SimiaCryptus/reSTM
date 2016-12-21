package stm.collection

import stm._
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


object Skeleton {
  def empty[T] = new STMTxn[Skeleton[T]] {
    override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Skeleton[T]] = create[T]
  }

  def create[T](implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = STMPtr.dynamic[Option[InternalData[T]]](None).map(new Skeleton(_))

  def static[T](id: PointerType) = new Skeleton(STMPtr.static[Option[Skeleton.InternalData[T]]](id, None))


  private case class InternalData[T]
  (
    // Primary mutable intranl data here
  ) {
    // In-txn operations
  }

}

class Skeleton[T](rootPtr: STMPtr[Option[Skeleton.InternalData[T]]]) {

  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def sampleOperation() = sync { AtomicApi.this.sampleOperation() }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)

    def sampleOperation() = atomic { Skeleton.this.sampleOperation()(_,executionContext).map(_ => Unit) }
  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority,maxRetries)

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def sampleOperation()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { Skeleton.this.sampleOperation() }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def sampleOperation()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(r => r).getOrElse(new Skeleton.InternalData[T]())
    }).flatMap(newRootData => rootPtr.write(Option(newRootData)))
  }
}

