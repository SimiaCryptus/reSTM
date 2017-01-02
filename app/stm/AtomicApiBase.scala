package stm

import storage.Restm

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

abstract class AtomicApiBase(priority: Duration = 0.seconds, maxRetries: Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) {
  def atomic[T](f: STMTxnCtx => Future[T]): Future[T] = new STMTxn[T] {
    override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[T] = f(ctx)
  }.txnRun(cluster, priority = priority, maxRetry = maxRetries)(executionContext)
}
