package stm

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.UUID

import _root_.util.Util._
import com.google.common.annotations.VisibleForTesting
import storage.actors.ActorLog
import storage.{Restm, TransactionConflict}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

trait STMTxn[+R] {
  def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[R]

  private[this] var allowCompletion = true

  @VisibleForTesting
  def testAbandoned() = {
    allowCompletion = false
    this
  }

  def toString(e: Throwable) = {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream()
    e.printStackTrace(new PrintStream(out))
    out.toString
  }

  final def txnRun(cluster: Restm, maxRetry: Int = 100, priority: Duration = 0.seconds)(implicit executionContext: ExecutionContext): Future[R] = monitorFuture("STMTxn.txnRun") {
    val opId = UUID.randomUUID().toString
    def _txnRun(retryNumber: Int, prior: Option[STMTxnCtx]): Future[R] = monitorFuture("STMTxn.txnRun.attempt") {
      val ctx: STMTxnCtx = new STMTxnCtx(cluster, priority + 0.milliseconds, prior)
      chainEx("Transaction Exception") { Future { txnLogic()(ctx, executionContext) }
        .flatMap(x => x)
        .flatMap(result => {
          if (allowCompletion) {
            ActorLog.log(s"Committing $ctx for operation $opId retry $retryNumber/$maxRetry")
            ctx.commit().map(_ => result)
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
            Thread.sleep(Random.nextInt(5+2*retryNumber))
            _txnRun(retryNumber + 1, Option(ctx).filter(_=>false)) // TODO: Seems to be a problem enabling this
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
