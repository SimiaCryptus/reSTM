package stm

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.UUID
import java.util.concurrent.{Callable, Executors, TimeUnit}

import _root_.util.Util._
import com.google.common.annotations.VisibleForTesting
import com.google.common.util.concurrent.ThreadFactoryBuilder
import storage.actors.ActorLog
import storage.{Restm, TransactionConflict}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Random

trait STMTxn[+R] {
  def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[R]

  private[this] var allowCompletion = true
  private[this] def now = System.currentTimeMillis()
  private[this] val startTime = now
  private[this] def age = now - startTime

  @VisibleForTesting
  def testAbandoned(): STMTxn[R] = {
    allowCompletion = false
    this
  }

  def toString(e: Throwable): String = {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream()
    e.printStackTrace(new PrintStream(out))
    out.toString
  }

  final def txnRun(cluster: Restm, maxRetry: Int = 100, priority: Duration = 0.seconds)(implicit executionContext: ExecutionContext): Future[R] = chainEx("Transaction Exception") {
    monitorFuture("STMTxn.txnRun") {
      val opId = UUID.randomUUID().toString
      def _txnRun(retryNumber: Int, prior: Option[STMTxnCtx]): Future[R] = {
        val ctx: STMTxnCtx = new STMTxnCtx(cluster, priority + 0.milliseconds, prior)
        chainEx("Transaction Exception") { Future { txnLogic()(ctx, executionContext) }
          .flatMap(x => x)
          .flatMap(result => {
            if (allowCompletion) {
              if(age > 5.seconds.toMillis) {
                ctx.revert().map(_ => throw new TransactionConflict("Transaction took too long"))
              } else {
                ActorLog.log(s"Committing $ctx for operation $opId retry $retryNumber/$maxRetry")
                ctx.commit().map(_ => result)
              }
            } else {
              ActorLog.log(s"Prevented committing $ctx for operation $opId retry $retryNumber/$maxRetry")
              Future.successful(result)
            }
          })}
          .recoverWith({
            case e: TransactionConflict if retryNumber < maxRetry =>
              ActorLog.log(s"Revert $ctx for operation $opId retry $retryNumber/$maxRetry due to ${toString(e)}")
              //if(!e.isInstanceOf[LockedException]) e.printStackTrace()
              ctx.revert()
              val promisedFuture = Promise[Future[R]]()
              STMTxn.retryPool.schedule(new Callable[Future[R]] {
                override def call(): Future[R] = {
                  val future = _txnRun(retryNumber + 1, Option(ctx)
                    .filter(_ => false) // TODO: Seems to be a problem enabling this
                  )
                  promisedFuture.success(future)
                  future
                }
              }, Random.nextInt(1 + Random.nextInt(1+((retryNumber * retryNumber) / 100))), TimeUnit.MILLISECONDS)
              promisedFuture.future.flatMap(x=>x)
            case e: Throwable =>
              if(!e.isInstanceOf[TransactionConflict]) {
                e.printStackTrace()
              }
              if (allowCompletion) {
                ActorLog.log(s"Revert $ctx for operation $opId retry $retryNumber/$maxRetry due to ${toString(e)}")
                ctx.revert()
              } else {
                ActorLog.log(s"Prevent revert $ctx for operation $opId retry $retryNumber/$maxRetry due to ${toString(e)}")
              }
              Future.failed(new RuntimeException(s"Failed operation $opId after $retryNumber attempts, ${toString(e)}"))
          })
      }
      _txnRun(0, None)
    }
  }
}

object STMTxn {

  private[STMTxn] val retryPool = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder().setNameFormat("txn-retry-%d").build())

}
