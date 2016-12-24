package stm.collection

import stm._
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag

object Skeleton {
  case class SkeletonData[T]
  (
    // Primary mutable internal data here
  ) {
    // In-txn operations
  }
}

class Skeleton[T](rootPtr: STMPtr[Skeleton.SkeletonData[T]]) {

  def this()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = this(STMPtr.dynamicSync(new Skeleton.SkeletonData[T]()))
  def this(ptr:PointerType) = this(new STMPtr[Skeleton.SkeletonData[T]](ptr))

  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {
    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def sampleOperation()(implicit classTag: ClassTag[T]) = sync { AtomicApi.this.sampleOperation() }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)
    def sampleOperation()(implicit classTag: ClassTag[T]) = atomic { Skeleton.this.sampleOperation()(_,executionContext,classTag).map(_ => Unit) }
  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority,maxRetries)
  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def sampleOperation()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = sync { Skeleton.this.sampleOperation() }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)

  def sampleOperation()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = {
    rootPtr.readOpt().map(prev => {
      prev.orElse(Option(new Skeleton.SkeletonData[T]()))
        .map(state=>state->state) // Invoke mutable internal method here
        .get
    }).flatMap(newRootData => rootPtr.write(newRootData._1).map(_=>newRootData._2))
  }
}

