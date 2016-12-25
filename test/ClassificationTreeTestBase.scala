import java.util.concurrent.Executors

import _root_.util.Util
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm.STMPtr
import stm.collection.ClassificationTree.ClassificationTreeNode
import stm.collection._
import storage.Restm._
import storage._
import storage.actors.RestmActors
import storage.remote.{RestmCluster, RestmHttpClient, RestmInternalRestmHttpClient}
import storage.types.JacksonValue

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
    List(10,100).foreach(items=> {
      s"support insert and iterate over $items items" in {
        val collection = new ClassificationTree(new PointerType)
        val input = Stream.continually(new ClassificationTreeItem(Map("value" -> Random.nextGaussian()))).take(items).toSet
        input.foreach(collection.atomic().sync.add("data", _))
        val output = collection.atomic().sync.iterateTree(max = items)._2.toSet
        output.size mustBe input.size
        output mustBe input
      }
    })
    List(100).foreach(items=> {
      s"support query and describe operations for $items items" in {
        val collection = new ClassificationTree(new PointerType)
        def randomItems(center:Double=0.0,scale:Double=1.0) = Stream.continually(new ClassificationTreeItem(Map("value" -> (center + scale * Random.nextGaussian()))))
        randomItems(center = -2).take(items).foreach(x=>collection.atomic().sync.add("foo", x))
        randomItems(center = 1).take(items).foreach(x=>collection.atomic().sync.add("bar", x))
        randomItems().take(10).foreach(x => {
          val id: STMPtr[ClassificationTreeNode] = collection.atomic().sync.getClusterId(x)
          println(s"$x routed to node "+JacksonValue.simple(id))
          println(s"Clustered Members: "+JacksonValue.simple(collection.atomic().sync.iterateCluster(id)))
          println(s"Tree Path: "+JacksonValue.simple(collection.atomic().sync.getClusterPath(id.id)))
          println(s"Node Counts: "+JacksonValue.simple(collection.atomic().sync.getClusterCount(id.id)))
          println()
        })
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

