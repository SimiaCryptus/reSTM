/*
 * Copyright (c) 2017 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package stm

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{Callable, Executors, TimeUnit}

import _root_.util.Util._
import com.google.common.annotations.VisibleForTesting
import com.google.common.util.concurrent.{AtomicDouble, ThreadFactoryBuilder}
import storage.actors.ActorLog
import storage.{Restm, TransactionConflict}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Random, Try}

trait STMTxn[+R] extends STMTxnInstrumentation {
  private[this] val startTime = now
  private[this] var allowCompletion = true

  def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[R]

  @VisibleForTesting
  def testAbandoned(): STMTxn[R] = {
    allowCompletion = false
    this
  }

  final def txnRun(cluster: Restm, maxRetry: Int = 100, priority: Duration = 0.seconds)(implicit executionContext: ExecutionContext): Future[R] = chainEx("Transaction Exception") {
    metrics.numberExecuted.incrementAndGet()
    metrics.callSites.getOrElseUpdate(caller, new AtomicInteger(0)).incrementAndGet()
    monitorFuture("STMTxn.txnRun") {
      val opId = UUID.randomUUID().toString

      def _txnRun(retryNumber: Int, prior: Option[STMTxnCtx]): Future[R] = {
        val ctx: STMTxnCtx = new STMTxnCtx(cluster, priority + 0.milliseconds, prior)
        chainEx("Transaction Exception") {
          metrics.numberAttempts.incrementAndGet()
          Future.fromTry{ Try {
            txnLogic()(ctx, executionContext)
          } }
            .flatMap(x => x)
            .flatMap(result => {
              if (allowCompletion) {
                val totalTime: Long = age
                metrics.totalTimeMs.addAndGet(totalTime)
                if (totalTime > 5.seconds.toMillis) {
                  ctx.revert().map(_ => throw new TransactionConflict("Transaction took too long"))
                } else {
                  metrics.numberSuccess.incrementAndGet()
                  ActorLog.log(s"Committing $ctx for operation $opId retry $retryNumber/$maxRetry")
                  ctx.commit().map(_ => result)
                }
              } else {
                ActorLog.log(s"Prevented committing $ctx for operation $opId retry $retryNumber/$maxRetry")
                Future.successful(result)
              }
            })
        }
          .recoverWith({
            case e: TransactionConflict if retryNumber < maxRetry =>
              metrics.numberFailed.incrementAndGet()
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
              }, Random.nextInt(1 + Random.nextInt(1 + ((retryNumber * retryNumber) / 1000))), TimeUnit.MICROSECONDS)
              promisedFuture.future.flatMap(x => x)
            case e: Throwable =>
              metrics.numberFailed.incrementAndGet()
              if (!e.isInstanceOf[TransactionConflict]) {
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

  private[this] def age = now - startTime

  private[this] def now = System.currentTimeMillis()

  def toString(e: Throwable): String = {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream()
    e.printStackTrace(new PrintStream(out))
    out.toString
  }
}

object STMTxn {

  private[STMTxn] val retryPool = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder().setNameFormat("txn-retry-%d").build())

}
