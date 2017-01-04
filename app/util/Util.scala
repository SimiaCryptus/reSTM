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

package util

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.{AtomicDouble, ThreadFactoryBuilder}
import stm.STMTxnInstrumentation
import storage.TransactionConflict

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}


object Util {
  val codeMetricsData = new TrieMap[String, CodeMetrics]()
  val scalarData = new TrieMap[String, AtomicDouble]()
  private[util] val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("code-metrics-pool-%d").build()))

  def mod(a: Long, b: Int): Int = {
    val result = (a % b).toInt
    if (result < 0) result + b
    else result
  }

  def toDigits(number: Long, radix: Int): List[Int] = {
    if (number == 0) {
      List.empty
    } else {
      val bit = mod(number, radix)
      val remainder = Math.floorDiv(number - bit, radix)
      toDigits(remainder, radix) ++ List(bit)
    }
  }

  def clearMetrics(): Unit = {
    codeMetricsData.clear()
    STMTxnInstrumentation.metrics.clear()
  }

  def now: FiniteDuration = System.nanoTime().nanoseconds

  def delta(name: String, delta: Double): Double = scalarData.getOrElseUpdate(name, new AtomicDouble(0)).addAndGet(delta)

  def monitorBlock[T](name: String)(f: => T): T = codeMetricsData.getOrElseUpdate(name, new CodeMetrics).sync(f)

  def monitorFuture[T](name: String)(f: => Future[T])(implicit executionContext: ExecutionContext): Future[T] = codeMetricsData.getOrElseUpdate(name, new CodeMetrics).future(f)

  def chainEx[T](str: => String, verbose: Boolean = true)(f: => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val stackTrace: Array[StackTraceElement] = Thread.currentThread().getStackTrace
    f.recover({
      case e: TransactionConflict =>
        val wrappedEx = new TransactionConflict(e.msg, e, e.conflitingTxn)
        wrappedEx.setStackTrace(stackTrace)
        throw wrappedEx
      case e =>
        val wrappedEx = new RuntimeException(str, e)
        wrappedEx.setStackTrace(stackTrace)
        throw wrappedEx
    })
  }

  def getMetrics = Map(
    "code" -> codeMetricsData.toMap.mapValues(_.get()).groupBy(_._1.split("\\.").head),
    "scalars" -> scalarData.toMap.mapValues(_.get()).groupBy(_._1.split("\\.").head),
    "txns" → STMTxnInstrumentation.metrics.toMap
  )
}

import util.Util._

class CodeMetrics {

  val invokeCount = new AtomicInteger(0)
  val successCount = new AtomicInteger(0)
  val errorCount = new AtomicInteger(0)
  val totalTime = new AtomicDouble(0.0)

  def sync[T](f: => T): T = {
    val start = now
    invokeCount.incrementAndGet()
    val result: Try[T] = Try {
      f
    }
    totalTime.addAndGet((now - start).toUnit(TimeUnit.SECONDS))
    if (result.isFailure) errorCount.incrementAndGet()
    else successCount.incrementAndGet()
    result.get
  }

  def future[T](f: => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val start = now
    invokeCount.incrementAndGet()
    val result = f
    result.onComplete({
      case Success(_) =>
        successCount.incrementAndGet()
        totalTime.addAndGet((now - start).toUnit(TimeUnit.SECONDS))
      case Failure(_) =>
        errorCount.incrementAndGet()
        totalTime.addAndGet((now - start).toUnit(TimeUnit.SECONDS))
    })
    result
  }

  def get() = Map(
    "invocations" -> invokeCount.get(),
    "success" -> successCount.get(),
    "failed" -> errorCount.get(),
    "totalTime" -> totalTime.get(),
    "avgTime" -> totalTime.get() / (successCount.get() + errorCount.get())
  )
}
