import java.util.concurrent.Executors
import java.util.{Date, UUID}

import _root_.util.Util
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import stm.collection.{LinkedList, TreeCollection}
import stm.concurrent.TaskStatus.Orphan
import stm.concurrent._
import storage.Restm._
import storage.actors.ActorLog
import storage.data.JacksonValue
import storage.remote.RestmCluster
import storage.{RestmActors, _}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try


abstract class MetricsSpecBase extends WordSpec with MustMatchers {
  implicit def cluster: Restm
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  "Metrics" should {
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr)
    "work" in {
      ActorLog.enabled = true
      StmExecutionQueue.verbose = false

      val taskTimeout = 3.minutes
      val insertTimeout = 90.seconds
      val taskSize = 100

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
        var continueLoop = true
        while(!sortTask.future.isCompleted && continueLoop) {
          if(!timeout.after(now)) throw new RuntimeException("Time Out")

          System.out.println(s"Checking Status at ${new Date()}...")
          val statusTrace = sortTask.atomic(-0.milliseconds).sync(60.seconds).getStatusTrace(Option(StmExecutionQueue))
          def isOrphaned(node : TaskStatusTrace) : Boolean = (node.status.isInstanceOf[Orphan]) || node.children.exists(isOrphaned(_))
          def statusSummary(node : TaskStatusTrace = statusTrace) : Map[String,Int] = (List(node.status.toString -> 1) ++ node.children.flatMap(statusSummary(_).toList))
            .groupBy(_._1).mapValues(_.map(_._2).reduceOption(_+_).getOrElse(0))

          val numQueued = StmExecutionQueue.workQueue.atomic().sync.size
          val numRunning = ExecutionStatusManager.currentlyRunning()

          val summary = JacksonValue.simple(statusSummary()).pretty
          if(isOrphaned(statusTrace)) {
            //println(JacksonValue.simple(statusTrace).pretty)
            System.err.println(s"Orphaned Tasks at ${new Date()} - $numQueued tasks queued, $numRunning runnung - ${summary}")
            //continueLoop = false
          } else if(numQueued > 0 || numRunning > 0) {
            System.out.println(s"Status OK at ${new Date()} - $numQueued tasks queued, $numRunning runnung - ${summary}")
          } else {
            System.err.println(s"Status Idle at ${new Date()} - $numQueued tasks queued, $numRunning runnung - ${summary}")
            //continueLoop = false
          }
          if(continueLoop) Try{Await.ready(sortTask.future, 5.seconds)}
        }
        System.out.println(s"Colleting Result at ${new Date()}")
        val sortResult = Await.result(sortTask.future, 5.seconds)
        val output = sortResult.atomic().sync.stream().toList
        output mustBe input.toList.sorted
      } finally {
        System.out.println(s"Final Data at ${new Date()}")
        println(JacksonValue.simple(Util.get()).pretty)
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