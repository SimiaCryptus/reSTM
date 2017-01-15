/*
 * Copyright (c) 2017 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Date
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import TaskUtil._
import _root_.util.{ForestCoverDataset, Util}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm.STMPtr
import stm.clustering._
import stm.clustering.strategy.{DefaultClassificationStrategy, NoBranchStrategy}
import stm.task.{ExecutionStatusManager, StmDaemons, StmExecutionQueue}
import storage.Restm._
import storage._
import storage.actors.RestmActors
import storage.remote.{RestmCluster, RestmHttpClient}
import storage.types.JacksonValue

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

object ClassificationTreeTestBase {
}

abstract class ClassificationTreeTestBase extends WordSpec with MustMatchers with BeforeAndAfterEach with BeforeAndAfterAll {


  override def beforeEach() {
    super.beforeEach()
    pool = Executors.newFixedThreadPool(8, new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build())
  }

  override def afterAll() {
  }

  override def afterEach() {
    super.afterEach()
    Await.result(StmDaemons.stop(), 30.seconds)
    Util.clearMetrics()
    pool.shutdownNow()
    pool.awaitTermination(1, TimeUnit.MINUTES)
    pool = null
    println("Shutdown complete")
    Thread.sleep(1000)
  }

  def cluster: Restm

  private var pool: ExecutorService = _

  "ClassificationTree" should {
    List(5, 10, 100, 1000).foreach(items => {
      s"support insert and iterate over $items items" in {
        try {
          implicit val executionContext = ExecutionContext.fromExecutor(pool)
          implicit val _cluster = cluster
          StmDaemons.start()
          StmExecutionQueue.get().registerDaemons(8)
          val collection = new ClassificationTree(new PointerType)
          collection.atomic().sync.setClusterStrategy(new DefaultClassificationStrategy(1))
          val input = Stream.continually(ClassificationTreeItem(Map("value" -> Random.nextGaussian()))).take(items).toSet
          input.foreach(collection.atomic().sync.add("data", _))

          def now = System.currentTimeMillis()
          val timeout = now + 30.seconds.toMillis
          def isWorkQueueEmpty = StmExecutionQueue.get().workQueue.atomic().sync.size() > 0
          def isAnythingRunning = ExecutionStatusManager.currentlyRunning() > 0

          while ((isWorkQueueEmpty || isAnythingRunning) && timeout > now) Thread.sleep(2000)
          println(JacksonValue.simple(ExecutionStatusManager.status()).pretty)
          val rootPtr: STMPtr[ClassificationTreeNode] = collection.dataPtr.atomic.sync.read.root

          def print(prefix: String, nodePtr: STMPtr[ClassificationTreeNode]): Unit = {
            val node: ClassificationTreeNode = nodePtr.atomic.sync.read
            val nodeId = node.atomic().sync.getTreeId(nodePtr, rootPtr)
            val nodeSize = node.itemBuffer.map(_.atomic().sync.size()).getOrElse(0)
            println(prefix + s"Node $nodeId size $nodeSize")
            node.pass.foreach(pass => print(prefix + " ", pass))
            node.fail.foreach(fail => print(prefix + " ", fail))
            node.exception.foreach(ex => print(prefix + " ", ex))
          }
          print("", rootPtr)
          val output = collection.atomic().sync.stream().map(_.value).toSet
          output.size mustBe input.size
          output mustBe input
        } finally {
          println(JacksonValue.simple(Util.getMetrics).pretty)
          Util.clearMetrics()
        }
      }
    })


    List(100).foreach(items=> {
      s"model and classify against $items scalar items" in {
        implicit val executionContext = ExecutionContext.fromExecutor(pool)
        implicit val _cluster = cluster

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
            println(s"Clustered Members: "+JacksonValue.simple(collection.atomic().sync.stream().toList))
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

    List(1, 100, 10000, 1000000).foreach(items => {
      s"modeling on $items items from forest cover" in {
        try {
          implicit val executionContext = ExecutionContext.fromExecutor(pool)
          implicit val _cluster = cluster

          println(s"Begin test at ${new Date()}")
          StmDaemons.start()
          StmExecutionQueue.get().registerDaemons(8)
          val collection = new ClassificationTree(new PointerType)
          collection.atomic().sync.setClusterStrategy(new NoBranchStrategy())
          val testingSet = ForestCoverDataset.dataSet.rows.take(100)
          val trainingSet = ForestCoverDataset.dataSet.rows.slice(100, items + 100)
          require(testingSet.nonEmpty)
          require(trainingSet.nonEmpty)

          print(s"Populating tree at ${new Date()}")
          val insertFutures = trainingSet.groupBy(_.label).map(t => Future {
            val (cover_type: Any, stream: Seq[ForestCoverDataset.dataSet.PageRow]) = t
            stream.map(item => item.asClassificationTreeItem).grouped(512).map(_.toList)
              .foreach((block: List[ClassificationTreeItem]) => {
                print(".")
                Console.flush()
                collection.atomic().sync(30.seconds).addAll(cover_type.toString, block)
              })
          })
          Await.result(Future.sequence(insertFutures), 5.minutes)
          println(s"completed at ${new Date()}")

          println(s"Top-level rule generation at ${new Date()}")
          awaitTask(collection.atomic().sync(5.minutes).splitTree(new DefaultClassificationStrategy(branchThreshold = 16)), taskTimeout = 30.minutes)
          println()
          println(s"Second-level rule generation at ${new Date()}")
          awaitTask(collection.atomic().sync(5.minutes).splitTree(new DefaultClassificationStrategy(branchThreshold = 4)), taskTimeout = 30.minutes)
          println()

          println(s"Testing model at ${new Date()}")
          val correct = testingSet.map(item => {
            val testValue = item.asClassificationTreeItem
            val coverType = item.label
            val id: STMPtr[ClassificationTreeNode] = collection.atomic().sync(90.seconds).getClusterId(testValue)
            println()
            println(s"item routed to node " + JacksonValue.simple(id))
            //println(s"Clustered Members: "+JacksonValue.simple(collection.atomic().sync.iterateCluster(id)))
            println(s"Tree Path: " + JacksonValue.simple(collection.atomic().sync.getClusterPath(id)))
            val counts: Map[String, Int] = collection.atomic().sync(5.minutes).getClusterCount(id)
            println(s"Node Counts: " + JacksonValue.simple(counts))
            println(s"Actual Type: " + coverType)
            val predictions = counts.mapValues(_.toDouble / counts.values.sum)
            if (predictions.isEmpty) {
              println(s"Empty Node Reached!")
              0
            } else {
              val prediction = predictions.maxBy(_._2)
              if (prediction._1 == coverType.toString) {
                println(s"Correct Prediction: " + prediction._1)
                println()
                1
              } else {
                println(s"False Prediction: " + prediction._1)
                println()
                0
              }
            }
          }).sum
          println(s"$correct correct out of 100")
        } finally {
          println(JacksonValue.simple(Util.getMetrics).pretty)
          Util.clearMetrics()
        }
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

class LocalClusterClassificationTreeTest extends ClassificationTreeTestBase with BeforeAndAfterEach with BeforeAndAfterAll {
  val shards: List[RestmActors] = (0 until 8).map(_ => new RestmActors()).toList

  override def beforeEach() {
    super.beforeEach()
    shards.foreach(_.clear())
  }

  override def afterAll() {
    restmPool.shutdown()
  }

  val restmPool: ExecutorService = Executors.newFixedThreadPool(8, new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())
  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(restmPool))
}

class ServletClassificationTreeTest extends ClassificationTreeTestBase with OneServerPerTest {
  val cluster = new RestmHttpClient(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))
}


//class ActorServletClassificationTreeTest extends ClassificationTreeTestBase with OneServerPerTest {
//  val cluster = new RestmImpl(new RestmInternalRestmHttpClient(s"http://localhost:$port")(executionContext))(executionContext)
//}

