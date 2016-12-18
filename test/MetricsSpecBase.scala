import java.util.concurrent.Executors
import java.util.{Date, UUID}

import _root_.util.Metrics
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import stm.collection.{LinkedList, TreeCollection}
import stm.concurrent.TaskStatus.Queued
import stm.concurrent.{StmDaemons, StmExecutionQueue, Task, TaskStatusTrace}
import storage.Restm._
import storage.actors.ActorLog
import storage.data.JacksonValue
import storage.remote.RestmCluster
import storage.{RestmActors, _}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Random


abstract class MetricsSpecBase extends WordSpec with MustMatchers {
  implicit def cluster: Restm
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  "Metrics" should {
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr)
    "work" in {
      ActorLog.enabled = true
      StmExecutionQueue.verbose = true

      val taskTimeout = 10.minutes
      val insertTimeout = 90.seconds
      val taskSize = 2000

      val input = randomUUIDs.take(taskSize).toSet
      val collection = TreeCollection.static[String](new PointerType)
      Await.result(Future.sequence(input.map(collection.atomic.add(_))), insertTimeout)
      System.out.println("Input Prepared")

      val sortTask: Task[LinkedList[String]] = collection.atomic.sync.sort()
      System.out.println("Task Started")

      StmDaemons.start()
      StmExecutionQueue.registerDaemons(8)
      System.out.println("Execution Engine Started")
      try {
        def now = new Date()
        val timeout = new Date(now.getTime + taskTimeout.toMillis)
        while(!sortTask.future.isCompleted && timeout.after(now)) {
          System.out.println("Checking Status...")
          val statusTrace: TaskStatusTrace = sortTask.atomic(-1.seconds).sync(60.seconds).getStatusTrace(Option(StmExecutionQueue))
          def isOrphaned(node : TaskStatusTrace) : Boolean = (node.status == Queued) || node.children.contains(isOrphaned(_))
          if(isOrphaned(statusTrace)) {
            println(JacksonValue.simple(statusTrace).pretty)
            System.err.println("Orphaned Tasks")
            Thread.sleep(15000)
          } else {
            System.out.println("Status OK")
            Thread.sleep(60.seconds.toMillis)
          }
        }
        System.out.println(s"Colleting Result at ${new Date()}")
        val sortResult = Await.result(sortTask.future, 5.seconds)
        val output = sortResult.atomic.stream().toList
        output mustBe input.toList.sorted
      } finally {
        System.out.println(s"Final Data at ${new Date()}")
        println(JacksonValue.simple(Metrics.get()).pretty)
        println(JacksonValue.simple(sortTask.atomic().sync(60.seconds).getStatusTrace(Option(StmExecutionQueue))).pretty)

        System.out.println("Stopping Execution Engine")
        Await.result(StmDaemons.stop(), 30.seconds)
      }
    }
  }
}


class LocalClusterMetricsSpec extends MetricsSpecBase with BeforeAndAfterEach {
  private val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val shards = (0 until 8).map(_ => new RestmActors()(pool)).toList

  override def beforeEach() {
    shards.foreach(_.clear())
  }

  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}
