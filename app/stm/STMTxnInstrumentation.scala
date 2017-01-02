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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Seq
import scala.concurrent.duration._


object STMTxnInstrumentation {
  val metrics = new TrieMap[String, STMTxnMetrics]()


  class STMTxnMetrics {
    val numberExecuted = new AtomicInteger(0)
    val numberAttempts = new AtomicInteger(0)
    val numberFailed = new AtomicInteger(0)
    val numberSuccess = new AtomicInteger(0)
    val totalTimeMs = new AtomicLong(0)
    val callSites = new TrieMap[String, AtomicInteger]()
    def getAvgTime = ((totalTimeMs.get * 1000) / numberAttempts.get).microseconds.toUnit(TimeUnit.SECONDS)
    def getSuccessRatio = numberSuccess.get.toDouble / numberSuccess.get
  }
}

trait STMTxnInstrumentation {
  lazy val metrics = STMTxnInstrumentation.metrics.getOrElseUpdate(codeId, new STMTxnInstrumentation.STMTxnMetrics())

  val creationStack = {
    def internalStack(frame: StackTraceElement):Boolean = {
      frame.getClassName.startsWith("stm")
    }
    val stackTrace: List[StackTraceElement] = Thread.currentThread().getStackTrace.toList
      .filterNot(_.getClassName.contains("SyncApiBase"))
      .filterNot(_.getClassName.startsWith("scala."))
      .filterNot(_.getMethodName.equals("apply"))
    val dropFilter = stackTrace.dropWhile(!internalStack(_))
    dropFilter.splitAt(dropFilter.lastIndexWhere(internalStack)+1)
  }

  lazy val caller: String = {
    val (stmStack, callingStack) = creationStack
    val frame = callingStack.head
    s"${frame.getClassName}.${frame.getMethodName}(${frame.getFileName}:${frame.getLineNumber})"
  }

  lazy val codeId: String = {
    val (stmStack, callingStack) = creationStack
    val frame = stmStack.last
    s"${frame.getClassName}.${frame.getMethodName}(${frame.getFileName}:${frame.getLineNumber})"
  }
}