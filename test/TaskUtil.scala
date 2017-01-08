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

import java.util.Date

import stm.task.TaskStatus.Orphan
import stm.task._
import storage._
import storage.types.JacksonValue

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

object TaskUtil {
  def awaitTask[T](sortTask: Task[T], taskTimeout: FiniteDuration = 10.minutes, diagnosticsTimeout: FiniteDuration = 60.seconds)(implicit cluster: Restm, executionContext: ExecutionContext): T = {
    def now = new Date()
    val timeout = new Date(now.getTime + taskTimeout.toMillis)
    val continueLoop = true
    var lastSummary = ""
    var lastChanged = now

    val queue = StmExecutionQueue.get()
    require(null != queue)
    while (!sortTask.future.isCompleted && continueLoop) {
      if (!timeout.after(now)) throw new RuntimeException("Time Out")
      try {
        System.out.println(s"Checking Status at ${new Date()}...")
        val priority = 50.milliseconds
        val statusTrace = sortTask.atomic(-priority).sync(diagnosticsTimeout).getStatusTrace(queue)

        def isOrphaned(node: TaskStatusTrace): Boolean = node.status.isInstanceOf[Orphan] || node.children.exists(isOrphaned)

        def statusSummary(node: TaskStatusTrace = statusTrace): Map[String, Int] = (List(node.status.toString -> 1) ++ node.children.flatMap(statusSummary(_).toList))
          .groupBy(_._1).mapValues(_.map(_._2).reduceOption(_ + _).getOrElse(0))

        val numQueued = Option(queue).map(_.workQueue.atomic(priority = -priority).sync(diagnosticsTimeout).size()).getOrElse(-1)
        val numRunning = ExecutionStatusManager.currentlyRunning()

        val summary = JacksonValue.simple(statusSummary()).pretty
        if (lastSummary == summary) {
          System.err.println(s"Stale Status at ${new Date()} - $numQueued tasks queued, $numRunning runnung - $summary")
          require(lastChanged.compareTo(new Date(now.getTime - 1000.minutes.toMillis)) > 0)
        } else if (isOrphaned(statusTrace)) {
          //println(JacksonValue.simple(statusTrace).pretty)
          System.err.println(s"Orphaned Tasks at ${new Date()} - $numQueued tasks queued, $numRunning runnung - $summary")
          //continueLoop = false
        } else if (numQueued > 0 || numRunning > 0) {
          System.out.println(s"Status OK at ${new Date()} - $numQueued tasks queued, $numRunning runnung - $summary")
        } else {
          System.err.println(s"Status Idle at ${new Date()} - $numQueued tasks queued, $numRunning runnung - $summary")
          //continueLoop = false
        }
        if (lastSummary != summary) {
          lastSummary = summary
          lastChanged = now
        }
        if (continueLoop) Try {
          Await.ready(sortTask.future, 15.seconds)
        }
      } catch {
        case e : IllegalArgumentException if e.getMessage.contains("requirement failed") ⇒ throw e
        case e : Throwable ⇒
          e.printStackTrace()
          //throw e
      }
    }
    System.out.println(s"Colleting Result at ${new Date()}")
    Await.result(sortTask.future, 5.seconds)
  }
}
