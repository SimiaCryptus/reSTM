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

package util

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
import stm.clustering.ClassificationTreeItem
import stm.clustering.strategy.{ClassificationStrategy, DefaultClassificationStrategy, NoBranchStrategy}
import stm.task.{StmExecutionQueue, Task}
import storage.Restm
import storage.remote.RestmHttpClient
import storage.types.JacksonValue

import scala.collection.JavaConverters._
import scala.collection.immutable.{IndexedSeq, Seq}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.util.Random

object DTTestUtil {

  def main(args: Array[String]): Unit = {
    val itemLimit = Option(args).filter(_.length > 1).map(_ (1)).map(Integer.parseInt).getOrElse(Integer.MAX_VALUE)
    implicit val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4,
      new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))
    val baseUrl = args(0)
    implicit val restm = new RestmHttpClient(baseUrl)
    StmExecutionQueue.init()
    new DTTestUtil(baseUrl, items=itemLimit).test()
  }

}
class DTTestUtil(baseUrl: String, timeout : Duration = 90.seconds, items:Int = Integer.MAX_VALUE) {


  val treeId = UUID.randomUUID().toString.split("-").head

  def insert(label: String, item: Seq[Any])(implicit executionContext: ExecutionContextExecutor): String = {
    val request = (url(baseUrl) / "cluster" / treeId).addQueryParameter("label", label)
    val requestBody = item.map(JacksonValue.simple(_).toString).mkString("\n")
    println(s"PUT ${request.url} - Writing ${item.size} items - ${requestBody.length} bytes")
    Await.result(Http(request.PUT << requestBody OK as.String), timeout)
  }

  def getStrategy()(implicit executionContext: ExecutionContextExecutor): ClassificationStrategy = {
    val request = (url(baseUrl) / "cluster" / treeId / "config")
    val json = Await.result(Http(request.GET OK as.String), timeout)
    println(s"GET ${request.url}")
    JacksonValue(json).deserialize[ClassificationStrategy]().get
  }

  def setStrategy(item: ClassificationStrategy)(implicit executionContext: ExecutionContextExecutor): String = {
    val request = (url(baseUrl) / "cluster" / treeId / "config")
    val requestBody = JacksonValue(item).toString
    println(s"POST ${request.url}")
    Await.result(Http(request.POST << requestBody OK as.String), timeout)
  }

  def split(item: ClassificationStrategy, clusterId: Int = 1)(implicit executionContext: ExecutionContextExecutor): JsonObject = {
    val request = (url(baseUrl) / "cluster" / treeId / clusterId / "split")
    val requestBody = JacksonValue(item).toString
    println(s"POST ${request.url}")
    val result = Await.result(Http(request.POST << requestBody OK as.String), timeout)
    val gson: Gson = new GsonBuilder().setPrettyPrinting().create()
    gson.fromJson(result, classOf[JsonObject])
  }

  def query(item: ClassificationTreeItem)(implicit executionContext: ExecutionContextExecutor): JsonObject = {
    val queryString = item.attributes.filterNot(_._1 == "Cover_Type").mapValues(x ⇒ List(x.toString))
    val request = (url(baseUrl) / "cluster" / treeId / "find").setQueryParameters(queryString)
    println(s"GET ${request.url}")
    val result = Await.result(Http(request.GET OK as.String), timeout)
    val gson: Gson = new GsonBuilder().setPrettyPrinting().create()
    gson.fromJson(result, classOf[JsonObject])
  }

  def info()(implicit executionContext: ExecutionContextExecutor): String = {
    val request = (url(baseUrl) / "cluster" / treeId / "config")
    println(s"GET ${request.url}")
    Await.result(Http(request.GET OK as.String), timeout)
  }

  def test()
          (implicit executionContext: ExecutionContextExecutor , cluster: RestmHttpClient) = {
    val taskId: String = startTreeTask(items)
    TaskUtil.awaitTask(new Task[AnyRef](new Restm.PointerType(taskId)), 100.minutes)
    verifyModel()
  }


  private def verifyModel()(implicit executionContext: ExecutionContextExecutor) = {
    val correct = ForestCoverDataset.load(100).rows.map(testValue ⇒ {
      val queryResult = query(testValue.asClassificationTreeItem)
      println(queryResult)
      val counts = queryResult.getAsJsonObject("counts").entrySet().asScala
        .map(e => e.getKey → e.getValue.getAsInt).toList
        .sortBy(_._2).reverse
      val leafId = queryResult.getAsJsonObject("path").getAsJsonPrimitive("treeId").getAsString
      val countStr = counts.map(x => x._1.toString + "→" + x._2.toString).mkString(",")
      val predictedClass = if (counts.isEmpty) "null" else counts.maxBy(_._2)._1
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

  def startTreeTask(items: Int)(implicit executionContext: ExecutionContextExecutor) = {
    println(getStrategy())
    println(setStrategy(new NoBranchStrategy))
    println(info())

    val forestData = ForestCoverDataset.load(items)
    val blocks: List[(String, IndexedSeq[forestData.PageRow])] = Random.shuffle(
      Random.shuffle(forestData.rows).take(items).groupBy(_.label).toList.flatMap(block ⇒ block._2.grouped(50).map(block._1 → _))).toList
    println(s"Data label distribution: " + blocks.groupBy(_._1).mapValues(_.map(_._2.size).sum))

    val itemSum = blocks.grouped(4).map(_.toParArray.map(x ⇒ {
      val (key: String, block: IndexedSeq[forestData.PageRow]) = x
      insert(key, block.map(_.asMap))
      block.size
    }).sum).sum
    println(s"Uploaded $itemSum in ${blocks.size} blocks")

    val taskInfo = split(new DefaultClassificationStrategy())
    println(taskInfo)
    val taskId = taskInfo.getAsJsonPrimitive("task").getAsString
    println(s"Started Task: $taskId")
    taskId
  }
}
