import java.util.Date

import stm.task.TaskStatus.Orphan
import stm.task._
import storage._
import storage.types.JacksonValue

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

object TaskUtil {
  def awaitTask[T](sortTask: Task[T], taskTimeout: FiniteDuration = 10.minutes, diagnosticsTimeout: FiniteDuration = 30.seconds)(implicit cluster: Restm, executionContext: ExecutionContext): T = {
    def now = new Date()

    val timeout = new Date(now.getTime + taskTimeout.toMillis)
    var continueLoop = true
    var lastSummary = ""
    while (!sortTask.future.isCompleted && continueLoop) {
      if (!timeout.after(now)) throw new RuntimeException("Time Out")

      System.out.println(s"Checking Status at ${new Date()}...")
      val statusTrace = sortTask.atomic(-0.milliseconds).sync(diagnosticsTimeout).getStatusTrace(Option(StmExecutionQueue))

      def isOrphaned(node: TaskStatusTrace): Boolean = (node.status.isInstanceOf[Orphan]) || node.children.exists(isOrphaned(_))

      def statusSummary(node: TaskStatusTrace = statusTrace): Map[String, Int] = (List(node.status.toString -> 1) ++ node.children.flatMap(statusSummary(_).toList))
        .groupBy(_._1).mapValues(_.map(_._2).reduceOption(_ + _).getOrElse(0))

      val numQueued = StmExecutionQueue.workQueue.atomic().sync(diagnosticsTimeout).size
      val numRunning = ExecutionStatusManager.currentlyRunning()

      val summary = JacksonValue.simple(statusSummary()).pretty
      if (lastSummary == summary) {
        System.err.println(s"Stale Status at ${new Date()} - $numQueued tasks queued, $numRunning runnung - ${summary}")
      } else if (isOrphaned(statusTrace)) {
        //println(JacksonValue.simple(statusTrace).pretty)
        System.err.println(s"Orphaned Tasks at ${new Date()} - $numQueued tasks queued, $numRunning runnung - ${summary}")
        //continueLoop = false
      } else if (numQueued > 0 || numRunning > 0) {
        System.out.println(s"Status OK at ${new Date()} - $numQueued tasks queued, $numRunning runnung - ${summary}")
      } else {
        System.err.println(s"Status Idle at ${new Date()} - $numQueued tasks queued, $numRunning runnung - ${summary}")
        //continueLoop = false
      }
      lastSummary = summary
      if (continueLoop) Try {
        Await.ready(sortTask.future, 15.seconds)
      }
    }
    System.out.println(s"Colleting Result at ${new Date()}")
    Await.result(sortTask.future, 5.seconds)
  }
}
