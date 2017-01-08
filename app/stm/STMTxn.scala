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
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, Executors, TimeUnit}

import _root_.util.Util._
import com.google.common.annotations.VisibleForTesting
import com.google.common.util.concurrent.ThreadFactoryBuilder
import storage.actors.ActorLog
import storage.{Restm, TransactionConflict}

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.Random

trait STMTxn[+R] extends STMTxnInstrumentation {
  private implicit def executionContext = StmPool.executionContext

  private[this] var allowCompletion = true
  val opId = UUID.randomUUID().toString

  def txnLogic()(implicit ctx: STMTxnCtx): Future[R]

  @VisibleForTesting
  def testAbandoned(): STMTxn[R] = {
    allowCompletion = false
    this
  }

  final def txnRun(cluster: Restm, maxRetry: Int = 50, priority: Duration = 0.seconds): Future[R] = chainEx("Transaction Exception") {
    metrics.numberExecuted.incrementAndGet()
    metrics.callSites.getOrElseUpdate(caller, new AtomicInteger(0)).incrementAndGet()
    monitorFuture("STMTxn.txnRun") {
      def _txnRun(retryNumber: Int = 0): Future[R] = {
        val ctx: STMTxnCtx = new STMTxnCtx(cluster, priority + 0.milliseconds, STMTxn.this)
        chainEx("Transaction Exception") {
          metrics.numberAttempts.incrementAndGet()
          val rawExecute: Future[R] = Future { txnLogic()(ctx) }.flatMap(x⇒x)
          rawExecute.flatMap(result => {
              if (allowCompletion) {
                val totalTime = ctx.age
                metrics.totalTimeMs.addAndGet(totalTime.toMicros)
                val timeout = 5.seconds
                if (totalTime > timeout) {
                  ctx.revert().map(_ => throw new TransactionConflict("Transaction timed out at " + timeout))
                } else {
                  metrics.numberSuccess.incrementAndGet()
                  ctx.commit().map(_ => {
                    ActorLog.log(s"TXN END: Committing $ctx for operation $opId in $totalTime retry $retryNumber/$maxRetry - Code $codeId")
                    result
                  })
                }
              } else {
                ActorLog.log(s"TXN END: Prevented committing $ctx for operation $opId in ${ctx.age} retry $retryNumber/$maxRetry - Code $codeId")
                Future.successful(result)
              }
            })
        }.recoverWith({
            case e: TransactionConflict if retryNumber < maxRetry =>
              metrics.numberFailed.incrementAndGet()
              ActorLog.log(s"TXN END: Revert $ctx for operation $opId retry $retryNumber/$maxRetry due to ${toString(e)} - Code $codeId")
              //if(!e.isInstanceOf[LockedException]) e.printStackTrace()
              val revert: Future[Unit] = ctx.revert()
              revert.flatMap(_⇒{
                val promisedFuture = Promise[Future[R]]()
                STMTxn.retryPool.schedule(new Callable[Future[R]] {
                  override def call(): Future[R] = {
                    val future = _txnRun(retryNumber + 1)
                    promisedFuture.success(future)
                    future
                  }
                }, (1 + Random.nextInt(1 + ((retryNumber * retryNumber) * 100))), TimeUnit.MICROSECONDS)
                promisedFuture.future.flatMap((x: Future[R]) => x)
              })
            case e: Throwable =>
              metrics.numberFailed.incrementAndGet()
              if (!e.isInstanceOf[TransactionConflict]) {
                e.printStackTrace()
              }
              if (allowCompletion) {
                ctx.revert().flatMap(_⇒{
                  ActorLog.log(s"TXN END: Revert $ctx for operation $opId retry $retryNumber/$maxRetry due to ${toString(e)} - Code $codeId")
                  Future.failed(new RuntimeException(s"Failed operation $opId after $retryNumber attempts", e))
                })
              } else {
                ActorLog.log(s"TXN END: Prevent revert $ctx for operation $opId retry $retryNumber/$maxRetry due to ${toString(e)} - Code $codeId")
                Future.failed(new RuntimeException(s"Failed operation $opId after $retryNumber attempts", e))
              }
          })
      }
      _txnRun()
    }
  }


  def toString(e: Throwable): String = {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream()
    e.printStackTrace(new PrintStream(out))
    out.toString
  }
}

object STMTxn {

  def now = System.nanoTime().nanoseconds
  private[STMTxn] lazy val retryPool = Executors.newScheduledThreadPool(4, new ThreadFactoryBuilder().setNameFormat("txn-retry-%d").build())

}
