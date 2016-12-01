import java.util.UUID
import java.util.concurrent.Executors

import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm._
import storage.Restm._
import storage.util._
import storage.{RestmActors, _}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}


abstract class SimpleSetSpecBase extends WordSpec with MustMatchers {
  implicit def cluster: Restm

  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  "STM System" should {

    "simple binary tree" in {

      val collection = SimpleSet.static(new PointerType)

      val items: List[String] = Stream.continually(UUID.randomUUID().toString.take(6)).take(20).toList

      for (item <- items) {
        try {
          collection.atomic.sync.contains(item) mustBe false
          collection.atomic.sync.add(item)
          collection.atomic.sync.contains(item) mustBe true
        } catch {
          case e => throw new RuntimeException(s"Error processing $item",e)
        }
      }

      for (item <- items) {
        try {
          collection.atomic.sync.contains(item) mustBe true
        } catch {
          case e =>
            Thread.sleep(1000)
            throw new RuntimeException(s"Error verifying $item",e)
        }
      }
    }

  }
}

class LocalSimpleSetSpec extends SimpleSetSpecBase with BeforeAndAfterEach {
  override def beforeEach() {
    cluster.internal.asInstanceOf[RestmActors].clear()
  }

  val cluster = LocalRestmDb
}

class LocalClusterSimpleSetSpec extends SimpleSetSpecBase with BeforeAndAfterEach {
  private val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val shards = (0 until 8).map(_ => new RestmActors()(pool)).toList

  override def beforeEach() {
    shards.foreach(_.clear())
  }

  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}

class IntegrationSimpleSetSpec extends SimpleSetSpecBase with OneServerPerTest {
  val cluster = new RestmProxy(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}

class IntegrationInteralSimpleSetSpec extends SimpleSetSpecBase with OneServerPerTest {
  private val newExeCtx: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val cluster = new RestmImpl(new InternalRestmProxy(s"http://localhost:$port")(newExeCtx))(newExeCtx)
}



