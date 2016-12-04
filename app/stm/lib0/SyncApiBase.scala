package stm.lib0

import stm.{STMTxn, STMTxnCtx}
import storage.Restm

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

abstract class SyncApiBase(duration: Duration) {
  def sync[T](f: =>Future[T]) : T = Await.result(f, duration)
}

abstract class AtomicApiBase()(implicit cluster: Restm, executionContext: ExecutionContext) {
  def atomic[T](f: STMTxnCtx=>Future[T]) : Future[T] = new STMTxn[T] {
    override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[T] = f(ctx)
  }.txnRun(cluster)(executionContext)
}
