import java.util.concurrent.Executors
import java.util.{Date, UUID}

import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import stm.lib0._
import storage.Restm._
import storage.data.JacksonValue
import storage.util._
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
      StmExecutionQueue.start(1)
      val input = randomUUIDs.take(500).toSet
      input.foreach(collection.atomic.sync.add(_))
      val sortTask: Task[LinkedList[String]] = collection.atomic.sync.sort()
      def now = new Date()
      val timeout = new Date(now.getTime + 600.seconds.toMillis)
      while(!sortTask.future.isCompleted && timeout.after(now)) {
        println(JacksonValue(sortTask.atomic.sync.getStatusTrace()).pretty)
        Thread.sleep(15000)
      }
      val sortResult: LinkedList[String] = Await.result(sortTask.future, 5.seconds)
      val output = sortResult.stream().toList
      output mustBe input.toList.sorted
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
