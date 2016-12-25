import java.util.concurrent.Executors

import _root_.util.Util
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm.collection._
import storage.Restm._
import storage.remote.{RestmCluster, RestmHttpClient, RestmInternalRestmHttpClient}
import storage.{RestmActors, _}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Random

object ClassificationTreeTestBase {
}

abstract class ClassificationTreeTestBase extends WordSpec with MustMatchers with BeforeAndAfterEach {

  override def afterEach() {
    Util.clearMetrics()
  }

  implicit def cluster: Restm
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  "ClassificationTree" should {
    List(10,100,1000).foreach(items=> {
      s"support insert and iterate over $items items" in {
        val collection = new ClassificationTree(new PointerType)
        val input = Stream.continually(new ClassificationTreeItem(Map("value" -> Random.nextGaussian()))).take(items).toSet
        input.foreach(collection.atomic().sync.add("data", _))
        val output = collection.atomic().sync.iterateClusterMembers(max = items)._2.toSet
        output.size mustBe input.size
        output mustBe input
      }
    })
  }

}

class LocalClassificationTreeTest extends ClassificationTreeTestBase with BeforeAndAfterEach {
  override def beforeEach() {
    super.beforeEach()
    cluster.internal.asInstanceOf[RestmActors].clear()
  }

  val cluster = LocalRestmDb
}

class LocalClusterClassificationTreeTest extends ClassificationTreeTestBase with BeforeAndAfterEach {
  private val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val shards = (0 until 8).map(_ => new RestmActors()).toList

  override def beforeEach() {
    super.beforeEach()
    shards.foreach(_.clear())
  }

  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}

class ServletClassificationTreeTest extends ClassificationTreeTestBase with OneServerPerTest {
  val cluster = new RestmHttpClient(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}


class ActorServletClassificationTreeTest extends ClassificationTreeTestBase with OneServerPerTest {
  private val newExeCtx: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val cluster = new RestmImpl(new RestmInternalRestmHttpClient(s"http://localhost:$port")(newExeCtx))(newExeCtx)
}

