import java.io.FileInputStream
import java.util.Date
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
import storage.remote.{RestmCluster, RestmHttpClient}
import storage.types.JacksonValue

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random

object ClassificationTreeTestBase {
}

abstract class ClassificationTreeTestBase extends WordSpec with MustMatchers with BeforeAndAfterEach {

  override def beforeEach() {
    super.beforeEach()
    implicit val _cluster = cluster
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
    List(10,100).foreach(items=> {
      s"support insert and iterate over $items items" in {
        val collection = new ClassificationTree(new PointerType)
        val input = Stream.continually(new ClassificationTreeItem(Map("value" -> Random.nextGaussian()))).take(items).toSet
        input.foreach(collection.atomic().sync.add("data", _))


        (1 to 10).toList.map(_=>collection.atomic().sync.iterateTree(max = items)._2.toSet.size) mustBe (1 to 10).toList.map(_=>input.size)
        val output = collection.atomic().sync.iterateTree(max = items)._2.toSet
        output.size mustBe input.size
        output mustBe input
      }
    })
    List(100).foreach(items=> {
      s"model and classify against $items scalar items" in {
        val scale = 3.0
        val verify = 5
        val minCorrectPct = .5

        val minCorrect: Int = (verify * 2 * minCorrectPct).floor.toInt
        val collection = new ClassificationTree(new PointerType)
        def randomItems(offset:Double=0.0,freq:Double=1.0) = {
          Stream.continually(Random.nextGaussian()*scale)
            .filter(x=>Math.pow(Math.sin(x*freq+offset),2)>Random.nextDouble())
            .map(x=>new ClassificationTreeItem(Map("value" -> (x))))
        }

        Map(
          "A" -> randomItems(freq = 5),
          "B" -> randomItems(freq = 5, offset = 5)
        ).map(e => {
          val (key, value) = e
          value.take(items).foreach(x => collection.atomic().sync.add(key, x))
          key -> value.drop(items).take(verify).toList
        }).map(e=>{
          val (key, values) = e
          values.map(value=>{
            val id: STMPtr[ClassificationTreeNode] = collection.atomic().sync.getClusterId(value)
            println(s"$value routed to node "+JacksonValue.simple(id))
            println(s"Clustered Members: "+JacksonValue.simple(collection.atomic().sync.iterateCluster(id)))
            println(s"Tree Path: "+JacksonValue.simple(collection.atomic().sync.getClusterPath(id)))
            val counts = collection.atomic().sync.getClusterCount(id)
            println(s"Node Counts: "+JacksonValue.simple(counts))
            val predictions = counts.mapValues(_.toDouble / counts.values.sum)
            val prediction = predictions.maxBy(_._2)
            if(prediction._1 == key) {
              println(s"Correct Prediction: "+prediction)
              println()
              1
            } else {
              println(s"False Prediction: "+prediction)
              println()
              0
            }
          }).sum
        }).sum must be > minCorrect
      }
    })
    List(1000).foreach(items=> {
      s"operations on $items item dictionary" in {
        StmDaemons.start()
        StmExecutionQueue.registerDaemons(8)
        val collection = new ClassificationTree(new PointerType)
        val dictionary = IOUtils.readLines(this.getClass().getClassLoader.getResourceAsStream("20k.txt"), "UTF8")
          .asScala.toList.toStream.map(x=>new ClassificationTreeItem(Map("value" -> x)))

        println(s"Populating tree at ${new Date()}")
        collection.atomic().sync.setClusterStrategy(new DefaultClassificationStrategy(branchThreshold = Int.MaxValue))
        dictionary.take(items).grouped(64).map(_.toList).foreach(collection.atomic().sync(30.seconds).addAll("foo", _))
        println(s"Top-level rule generation at ${new Date()}")
        awaitTask(collection.atomic().sync.splitTree(new DefaultClassificationStrategy(branchThreshold = 16)), taskTimeout = 30.minutes)
        println()
        println(s"Second-level rule generation at ${new Date()}")
        awaitTask(collection.atomic().sync.splitTree(new DefaultClassificationStrategy(branchThreshold = 4)), taskTimeout = 30.minutes)
        println()

        println(s"Testing model at ${new Date()}")
        val spellings = List("wit", "tome", "morph")
        spellings.map(x=>new ClassificationTreeItem(Map("value" -> x))).foreach(x => {
          val word = x.attributes("value").toString
          val closest = dictionary.take(items).map(_.attributes("value").toString).sortBy((a)=>LevenshteinDistance.getDefaultInstance.apply(a,word)).take(5).toList
          val id: STMPtr[ClassificationTreeNode] = collection.atomic().sync(30.seconds).getClusterId(x)
          println(s"$x routed to node "+JacksonValue.simple(id))
          println(s"Clustered Members: "+JacksonValue.simple(collection.atomic().sync.iterateCluster(id)))
          println(s"Tree Path: "+JacksonValue.simple(collection.atomic().sync.getClusterPath(id)))
          println(s"Node Counts: "+JacksonValue.simple(collection.atomic().sync.getClusterCount(id)))
          println(s"Closest Inserted Words: "+closest)
          println()
        })

        println(JacksonValue.simple(Util.getMetrics()).pretty)
        Util.clearMetrics()
      }
    })
    List(1000).foreach(items=> {
      s"modeling on $items items from forest cover" in {
        StmDaemons.start()
        StmExecutionQueue.registerDaemons(8)
        val collection = new ClassificationTree(new PointerType)
        val fields = List(
          List("Elevation"), //                              quantitative    meters                       Elevation in meters
          List("Aspect"), //                                 quantitative    azimuth                      Aspect in degrees azimuth
          List("Slope"), //                                  quantitative    degrees                      Slope in degrees
          List("Horizontal_Distance_To_Hydrology"), //       quantitative    meters                       Horz Dist to nearest surface water features
          List("Vertical_Distance_To_Hydrology"), //         quantitative    meters                       Vert Dist to nearest surface water features
          List("Horizontal_Distance_To_Roadways"), //        quantitative    meters                       Horz Dist to nearest roadway
          List("Hillshade_9am"), //                          quantitative    0 to 255 index               Hillshade index at 9am, summer solstice
          List("Hillshade_Noon"), //                         quantitative    0 to 255 index               Hillshade index at noon, summer soltice
          List("Hillshade_3pm"), //                          quantitative    0 to 255 index               Hillshade index at 3pm, summer solstice
          List("Horizontal_Distance_To_Fire_Points"), //    quantitative    meters                       Horz Dist to nearest wildfire ignition points
          (1 to 4).map(i => s"Wilderness_Area_$i"), // (4 binary columns)     qualitative     0 (absence) or 1 (presence)  Wilderness area designation
          (1 to 40).map(i => s"Soil_Type_$i"), // (40 binary columns)          qualitative     0 (absence) or 1 (presence)  Soil Type designation
          List("Cover_Type") // (7 types)                    integer         1 to 7                       Forest Cover Type designation
        ).flatten
        val lines = IOUtils.readLines(new FileInputStream("covtype.data.txt"), "UTF8").asScala.toList
        val dataSet = lines.map(_.trim).filterNot(_.isEmpty).map(x=>{
          val values = x.split(",").map(Integer.parseInt(_).toInt)
          val combined = fields.zip(values).toMap
          new ClassificationTreeItem(combined)
        }).filter(_.attributes.contains("Cover_Type")).map(_->Random.nextDouble()).sortBy(_._2).map(_._1)

        println(s"Populating tree at ${new Date()}")
        collection.atomic().sync.setClusterStrategy(new DefaultClassificationStrategy(branchThreshold = Int.MaxValue))

        dataSet.take(items).groupBy(_.attributes("Cover_Type")).foreach(t=>{
          val (cover_type: Any,stream: Seq[ClassificationTreeItem]) = t
          stream.grouped(64).map(_.map(item=>item.copy(attributes = item.attributes - "Cover_Type")).toList).foreach(collection.atomic().sync(30.seconds).addAll(cover_type.toString, _))
        })


        println(s"Top-level rule generation at ${new Date()}")
        awaitTask(collection.atomic().sync.splitTree(new DefaultClassificationStrategy(branchThreshold = 16)), taskTimeout = 30.minutes)
        println()
        println(s"Second-level rule generation at ${new Date()}")
        awaitTask(collection.atomic().sync.splitTree(new DefaultClassificationStrategy(branchThreshold = 4)), taskTimeout = 30.minutes)
        println()

        println(s"Testing model at ${new Date()}")
        dataSet.drop(items).take(10).foreach(item => {
          val testValue = item.copy(attributes = item.attributes - "Cover_Type")
          val coverType = item.attributes("Cover_Type")
          val id: STMPtr[ClassificationTreeNode] = collection.atomic().sync(30.seconds).getClusterId(testValue)
          println(s"$item routed to node "+JacksonValue.simple(id))
          println(s"Clustered Members: "+JacksonValue.simple(collection.atomic().sync.iterateCluster(id)))
          println(s"Tree Path: "+JacksonValue.simple(collection.atomic().sync.getClusterPath(id)))
          val counts: Map[String, Int] = collection.atomic().sync.getClusterCount(id)
          println(s"Node Counts: "+JacksonValue.simple(counts))
          println(s"Actual Type: "+coverType)
          val predictions = counts.mapValues(_.toDouble / counts.values.sum)
          val prediction = predictions.maxBy(_._2)
          if(prediction._1 == coverType.toString) {
            println(s"Correct Prediction: "+prediction._1)
            println()
            1
          } else {
            println(s"False Prediction: "+prediction._1)
            println()
            0
          }
          println()
        })

        println(JacksonValue.simple(Util.getMetrics()).pretty)
        Util.clearMetrics()
      }
    })
  }

}

class LocalClassificationTreeTest extends ClassificationTreeTestBase with BeforeAndAfterEach {
  override def beforeEach() {
    cluster.internal.asInstanceOf[RestmActors].clear()
    super.beforeEach()
  }

  val cluster = LocalRestmDb()
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


//class ActorServletClassificationTreeTest extends ClassificationTreeTestBase with OneServerPerTest {
//  val cluster = new RestmImpl(new RestmInternalRestmHttpClient(s"http://localhost:$port")(executionContext))(executionContext)
//}

