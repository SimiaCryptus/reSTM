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

import java.util.UUID
import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.google.gson.{Gson, GsonBuilder, JsonObject}
import dispatch._
import org.scalatest.{MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm.collection.clustering.{ClassificationStrategy, ClassificationTreeItem, DefaultClassificationStrategy, NoBranchStrategy}
import stm.task.{StmDaemons, StmExecutionQueue, Task}
import storage.Restm
import storage.remote.RestmHttpClient
import storage.types.JacksonValue
import util.Util

import scala.collection.JavaConverters._
import scala.collection.immutable.{IndexedSeq, Seq}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.util.Random

object ClassificationHttpClientUtil {

  def shuffle[T](list: List[T]) = list.map(_→Random.nextDouble()).sortBy(_._2).map(_._1)

  def test(baseUrl: String, items:Int = Integer.MAX_VALUE, timeout : Duration = 90.seconds)
          (implicit executionContext: ExecutionContextExecutor, cluster: RestmHttpClient) = {

    val treeId = UUID.randomUUID().toString.split("-").head

    def insert(label: String, item: Seq[Any]): String = {
      val request = (url(baseUrl) / "cluster" / treeId).addQueryParameter("label", label)
      val requestBody = item.map(JacksonValue.simple(_).toString).mkString("\n")
      println(s"PUT ${request.url} - Writing ${item.size} items - ${requestBody.length} bytes")
      Await.result(Http(request.PUT << requestBody OK as.String), timeout)
    }

    def getStrategy(): ClassificationStrategy = {
      val request = (url(baseUrl) / "cluster" / treeId / "config")
      val json = Await.result(Http(request.GET OK as.String), timeout)
      println(s"GET ${request.url}")
      JacksonValue(json).deserialize[ClassificationStrategy]().get
    }

    def setStrategy(item: ClassificationStrategy): String = {
      val request = (url(baseUrl) / "cluster" / treeId / "config")
      val requestBody = JacksonValue(item).toString
      println(s"POST ${request.url}")
      Await.result(Http(request.POST << requestBody OK as.String), timeout)
    }

    def split(item: ClassificationStrategy, clusterId: Int = 1): JsonObject = {
      val request = (url(baseUrl) / "cluster" / treeId / clusterId / "split")
      val requestBody = JacksonValue(item).toString
      println(s"POST ${request.url}")
      val result = Await.result(Http(request.POST << requestBody OK as.String), timeout)
      val gson: Gson = new GsonBuilder().setPrettyPrinting().create()
      gson.fromJson(result, classOf[JsonObject])
    }

    def query(item: ClassificationTreeItem): JsonObject = {
      val queryString = item.attributes.filterNot(_._1 == "Cover_Type").mapValues(x ⇒ List(x.toString))
      val request = (url(baseUrl) / "cluster" / treeId / "find").setQueryParameters(queryString)
      println(s"GET ${request.url}")
      val result = Await.result(Http(request.GET OK as.String), timeout)
      val gson: Gson = new GsonBuilder().setPrettyPrinting().create()
      gson.fromJson(result, classOf[JsonObject])
    }

    def info(): String = {
      val request = (url(baseUrl) / "cluster" / treeId / "config")
      println(s"GET ${request.url}")
      Await.result(Http(request.GET OK as.String), timeout)
    }

    println(getStrategy())
    println(setStrategy(new NoBranchStrategy))
    println(info())

    val blocks: List[(String, IndexedSeq[ForestCoverDataset.dataSet.PageRow])] = Random.shuffle(ForestCoverDataset.dataSet.rows
      .take(items).grouped(10000).flatMap(_.groupBy(_.label))).toList
    val itemSum = blocks.grouped(4).map(_.toParArray.map(x ⇒ {
      val (key: String, block: IndexedSeq[ForestCoverDataset.dataSet.PageRow]) = x
      block.grouped(50).foreach(b⇒insert(key, b.map(_.asMap)))
      block.size
    }).sum).sum
    println(s"Uploaded $itemSum in ${blocks.size} blocks")

    val taskInfo = split(new DefaultClassificationStrategy(2))
    println(taskInfo)
    val taskId = taskInfo.getAsJsonPrimitive("task").getAsString
    TaskUtil.awaitTask(new Task[AnyRef](new Restm.PointerType(taskId)), 100.minutes)

    val correct = ForestCoverDataset.dataSet.rows.slice(Math.min(items, ForestCoverDataset.dataSet.size-100),items+100).map(testValue ⇒ {
      val queryResult = query(testValue.asClassificationTreeItem)
      println(queryResult)
      val counts = queryResult.getAsJsonObject("counts").entrySet().asScala
        .map(e => e.getKey → e.getValue.getAsInt).toList
        .sortBy(_._2).reverse
      val leafId = queryResult.getAsJsonObject("path").getAsJsonPrimitive("treeId").getAsInt
      val countStr = counts.map(x=>x._1.toString+"→"+x._2.toString).mkString(",")
      val predictedClass = if(counts.isEmpty) "null" else counts.maxBy(_._2)._1
      val actualClass = testValue.label
      if (actualClass == predictedClass) {
        println(s"Correct: $actualClass correctly predicted in leaf $leafId with label counts ($countStr)")
        1
      } else {
        println(s"Incorrect: $actualClass was not the most common in leaf $leafId with label counts ($countStr)")
        0
      }
    }).sum
    println(s"Correct responses: $correct")
  }

}

class ClassificationServletSpec extends WordSpec with MustMatchers with OneServerPerTest {

      "Single Node Servlet" should {
        "provide demo cluster tree" in {
          val baseUrl = s"http://localhost:$port"
          implicit val cluster: RestmHttpClient = new RestmHttpClient(baseUrl)(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
            new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))
          implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
            new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))
          try {
            //ActorLog.enabled = true
            val timeout : Duration = 90.seconds
            Await.result(Http((url(baseUrl) / "sys" / "init").GET OK as.String), timeout)
            StmExecutionQueue.get().verbose = true
            Thread.sleep(1000) // Allow platform to start
            ClassificationHttpClientUtil.test(baseUrl, items = 10000, timeout = timeout)
            Await.result(Http((url(baseUrl) / "sys" / "shutdown").GET OK as.String), timeout)
            Await.result(StmDaemons.join(), 5.minutes)
            Thread.sleep(1000) // Allow rest of processes to complete
          } finally {
            println(JacksonValue.simple(Util.getMetrics).pretty)
            Util.clearMetrics()
          }
        }
      }

}
