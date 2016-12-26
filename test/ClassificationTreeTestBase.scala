import java.util.concurrent.Executors

import TaskUtil._
import _root_.util.{LevenshteinDistance, Util}
import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm.STMPtr
import stm.collection.ClassificationTree.{ClassificationTreeNode, _}
import stm.collection._
import stm.task.{StmDaemons, StmExecutionQueue}
import storage.Restm._
import storage._
import storage.actors.RestmActors
import storage.remote.{RestmCluster, RestmHttpClient, RestmInternalRestmHttpClient}
import storage.types.JacksonValue

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object ClassificationTreeTestBase {
}

abstract class ClassificationTreeTestBase extends WordSpec with MustMatchers with BeforeAndAfterEach {

  override def beforeEach() {
    super.beforeEach()
    implicit val _cluster = cluster
    StmDaemons.start()
    StmExecutionQueue.registerDaemons(8)
  }
  override def afterEach() {
    super.afterEach()
    implicit val _cluster = cluster
    Await.result(StmDaemons.stop(), 10.seconds)
    Util.clearMetrics()
  }

  def cluster: Restm
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  "ClassificationTree" should {
    implicit def _cluster = cluster
//    List(10,100).foreach(items=> {
//      s"support insert and iterate over $items items" in {
//        val collection = new ClassificationTree(new PointerType)
//        val input = Stream.continually(new ClassificationTreeItem(Map("value" -> Random.nextGaussian()))).take(items).toSet
//        input.foreach(collection.atomic().sync.add("data", _))
//
//        (1 to 10).toList.filterNot(_=>collection.atomic().sync.iterateTree(max = items)._2.toSet.size == input.size).size mustBe 0
//        val output = collection.atomic().sync.iterateTree(max = items)._2.toSet
//        output.size mustBe input.size
//        output mustBe input
//      }
//    })
//    List(100).foreach(items=> {
//      s"model and classify against $items scalar items" in {
//        val scale = 3.0
//        val verify = 5
//        val minCorrectPct = .5
//
//        val minCorrect: Int = (verify * 2 * minCorrectPct).floor.toInt
//        val collection = new ClassificationTree(new PointerType)
//        def randomItems(offset:Double=0.0,freq:Double=1.0) = {
//          Stream.continually(Random.nextGaussian()*scale)
//            .filter(x=>Math.pow(Math.sin(x*freq+offset),2)>Random.nextDouble())
//            .map(x=>new ClassificationTreeItem(Map("value" -> (x))))
//        }
//
//        Map(
//          "A" -> randomItems(freq = 5),
//          "B" -> randomItems(freq = 5, offset = 5)
//        ).map(e => {
//          val (key, value) = e
//          value.take(items).foreach(x => collection.atomic().sync.add(key, x))
//          key -> value.drop(items).take(verify).toList
//        }).map(e=>{
//          val (key, values) = e
//          values.map(value=>{
//            val id: STMPtr[ClassificationTreeNode] = collection.atomic().sync.getClusterId(value)
//            println(s"$value routed to node "+JacksonValue.simple(id))
//            println(s"Clustered Members: "+JacksonValue.simple(collection.atomic().sync.iterateCluster(id)))
//            println(s"Tree Path: "+JacksonValue.simple(collection.atomic().sync.getClusterPath(id.id)))
//            val counts = collection.atomic().sync.getClusterCount(id.id)
//            println(s"Node Counts: "+JacksonValue.simple(counts))
//            val predictions = counts.mapValues(_.toDouble / counts.values.sum)
//            val prediction = predictions.maxBy(_._2)
//            if(prediction._1 == key) {
//              println(s"Correct Prediction: "+prediction)
//              println()
//              1
//            } else {
//              println(s"False Prediction: "+prediction)
//              println()
//              0
//            }
//          }).sum
//        }).sum must be > minCorrect
//      }
//    })
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

        collection.atomic().sync.setClusterStrategy(new DefaultClassificationStrategy(branchThreshold = Int.MaxValue))
        dictionary.take(items).foreach(x=>collection.atomic().sync(30.seconds).add("foo", x))
        println(s"Top-level rule generation")
        awaitTask(collection.atomic().sync.splitTree(new DefaultClassificationStrategy(branchThreshold = 16)), taskTimeout = 30.minutes)
        println()
        println(s"Top-level rule generation")
        awaitTask(collection.atomic().sync.splitTree(new DefaultClassificationStrategy(branchThreshold = 4)), taskTimeout = 30.minutes)
        println()

        val spellings = List("wit", "tome", "morph")
        spellings.map(x=>new ClassificationTreeItem(Map("value" -> x))).foreach(x => {
          val word = x.attributes("value").toString
          val closest = dictionary.take(items).map(_.attributes("value").toString).sortBy((a)=>LevenshteinDistance.getDefaultInstance.apply(a,word)).take(5).toList
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
    cluster.internal.asInstanceOf[RestmActors].clear()
    super.beforeEach()
  }

  val cluster = LocalRestmDb
}

class LocalClusterClassificationTreeTest extends ClassificationTreeTestBase with BeforeAndAfterEach {
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
  val cluster = new RestmImpl(new RestmInternalRestmHttpClient(s"http://localhost:$port")(executionContext))(executionContext)
}

