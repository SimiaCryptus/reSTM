import java.util.concurrent.Executors
import java.util.{Date, UUID}

import _root_.util.Metrics
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import stm.collection.{LinkedList, TreeCollection}
import stm.concurrent.{StmDaemons, StmExecutionQueue, Task}
import storage.Restm._
import storage.actors.ActorLog
import storage.data.JacksonValue
import storage.remote.RestmCluster
import storage.{RestmActors, _}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}


abstract class MetricsSpecBase extends WordSpec with MustMatchers {
  implicit def cluster: Restm
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  "Metrics" should {
    val collection = TreeCollection.static[String](new PointerType)
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr)
    "work" in {
      ActorLog.enabled = true
      StmExecutionQueue.verbose = true
      StmDaemons.start()
      StmExecutionQueue.registerDaemons(8)
      val input = randomUUIDs.take(1500).toSet
      input.foreach(collection.atomic.sync.add(_))
      val sortTask: Task[LinkedList[String]] = collection.atomic.sync.sort()
      def now = new Date()
      val timeout = new Date(now.getTime + 10.minutes.toMillis)
      while(!sortTask.future.isCompleted && timeout.after(now)) {
        println(JacksonValue(sortTask.atomic.sync.getStatusTrace()).pretty)
        Thread.sleep(15000)
      }
      println(JacksonValue(Metrics.get()).pretty)
      val sortResult: LinkedList[String] = Await.result(sortTask.future, 5.seconds)
      val output = sortResult.stream().toList
      output mustBe input.toList.sorted
      Await.result(StmDaemons.stop(), 30.seconds)
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
