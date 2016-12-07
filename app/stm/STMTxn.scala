package stm

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.UUID

import com.google.common.annotations.VisibleForTesting
import storage.Restm
import storage.util.ActorLog

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

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

  final def txnRun(cluster: Restm, maxRetry: Int = 100, priority: Duration = 0.seconds)(implicit executionContext: ExecutionContext): Future[R] = {
    val opId = UUID.randomUUID().toString
    def _txnRun(retryNumber: Int, prior: Option[STMTxnCtx]): Future[R] = {
      val ctx: STMTxnCtx = new STMTxnCtx(cluster, priority + 0.milliseconds, prior)
      txnLogic()(ctx, executionContext)
        .flatMap(result => {
          if (allowCompletion) {
            ActorLog.log(s"Committing $ctx for operation $opId retry $retryNumber/$maxRetry")
            ctx.commit().map(_ => result)
          } else {
            ActorLog.log(s"Prevented committing $ctx for operation $opId retry $retryNumber/$maxRetry")
            Future.successful(result)
          }
        })
        .recoverWith({
          case e: Throwable if retryNumber < maxRetry =>
            //e.printStackTrace(System.out)
            ActorLog.log(s"Revert $ctx for operation $opId retry $retryNumber/$maxRetry due to ${toString(e)}")
            ctx.revert()
            _txnRun(retryNumber + 1, Option(ctx))
          case e: Throwable =>
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
