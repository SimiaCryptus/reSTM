import java.util.concurrent.Executors

import _root_.util.{LevenshteinDistance, Util}
import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm.STMPtr
import stm.collection.ClassificationTree.{ClassificationTreeNode, _}
import stm.collection._
import storage.Restm._
import storage._
import storage.actors.RestmActors
import storage.remote.{RestmCluster, RestmHttpClient, RestmInternalRestmHttpClient}
import storage.types.JacksonValue

import scala.collection.JavaConverters._
import scala.concurrent.duration._
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

        (1 to 10).toList.filterNot(_=>collection.atomic().sync.iterateTree(max = items)._2.toSet.size == input.size).size mustBe 0
        val output = collection.atomic().sync.iterateTree(max = items)._2.toSet
        output.size mustBe input.size
        output mustBe input
      }
    })
    List(100).foreach(items=> {
      s"operations on $items scalar items" in {
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
//    List(100).foreach(items=> {
//      s"operations on $items text items" in {
//        val collection = new ClassificationTree(new PointerType)
//        collection.atomic().sync.setClusterStrategy(new TextClassificationStrategy)
//        def randomItems(length:Int=4) = Stream.continually(new ClassificationTreeItem(Map("value" -> UUID.randomUUID().toString.take(length))))
//        randomItems(length = 4).take(items).foreach(x=>collection.atomic().sync.add("foo", x))
//        randomItems(length = 3).take(items).foreach(x=>collection.atomic().sync.add("bar", x))
//        randomItems(4).take(10).foreach(x => {
//          val id: STMPtr[ClassificationTreeNode] = collection.atomic().sync.getClusterId(x)
//          println(s"$x routed to node "+JacksonValue.simple(id))
//          println(s"Clustered Members: "+JacksonValue.simple(collection.atomic().sync.iterateCluster(id)))
//          println(s"Tree Path: "+JacksonValue.simple(collection.atomic().sync.getClusterPath(id.id)))
//          println(s"Node Counts: "+JacksonValue.simple(collection.atomic().sync.getClusterCount(id.id)))
//          println()
//        })
//      }
//    })
    List(50,200,1000).foreach(items=> {
      s"operations on $items item dictionary" in {
        val collection = new ClassificationTree(new PointerType)
        val dictionary = IOUtils.readLines(this.getClass().getClassLoader.getResourceAsStream("20k.txt"), "UTF8")
          .asScala.toList.toStream.map(x=>new ClassificationTreeItem(Map("value" -> x)))

        var toInsert = dictionary.take(items)
        if(items > 100) {
          collection.atomic().sync.setClusterStrategy(new TextClassificationStrategy(branchThreshold = 20))
          toInsert.take(100).foreach(x=>collection.atomic().sync(30.seconds).add("foo", x))
          toInsert = toInsert.drop(100)
        }
        collection.atomic().sync.setClusterStrategy(new TextClassificationStrategy(branchThreshold = 5))
        toInsert.foreach(x=>collection.atomic().sync(30.seconds).add("foo", x))


        dictionary.drop(100).take(items-100).foreach(x=>collection.atomic().sync(30.seconds).add("foo", x))

        val spellings = List("wit", "tome", "morph")
        spellings.map(x=>new ClassificationTreeItem(Map("value" -> x))).foreach(x => {
          val word = x.attributes("value").toString
          val closest = dictionary.map(_.attributes("value").toString).sortBy((a)=>LevenshteinDistance.getDefaultInstance.apply(a,word)).take(5).toList
          val id: STMPtr[ClassificationTreeNode] = collection.atomic().sync(30.seconds).getClusterId(x)
          println(s"$x routed to node "+JacksonValue.simple(id))
          println(s"Clustered Members: "+JacksonValue.simple(collection.atomic().sync.iterateCluster(id)))
          println(s"Tree Path: "+JacksonValue.simple(collection.atomic().sync.getClusterPath(id.id)))
          println(s"Node Counts: "+JacksonValue.simple(collection.atomic().sync.getClusterCount(id.id)))
          println(s"Closest Inserted Words: "+closest)
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

