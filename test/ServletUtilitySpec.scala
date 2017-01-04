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

import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import storage.remote.RestmHttpClient

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}


class ServletUtilitySpec extends WordSpec with MustMatchers with OneServerPerTest {
  private val baseUrl = s"http://localhost:$port"
  implicit val cluster = new RestmHttpClient(baseUrl)(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))
  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))


//  "Single Node Servlet" should {
//    "provide demo sort class" in {
//      StmDaemons.start()
//      StmExecutionQueue.get().verbose = true
//      StmExecutionQueue.get().registerDaemons(4)(storageService, executionContext)
//      Thread.sleep(1000) // Allow platform to start
//      Desktop.getDesktop.browse(new URI(s"http://localhost:$port/sys/logs/"))
//      Await.result(StmDaemons.join(), 300.minutes)
//      Thread.sleep(1000) // Allow rest of processes to complete
//    }
//  }

  //    "Single Node Servlet" should {
  //      "provide demo sort class" in {
  //        StmExecutionQueue.verbose = true
  //        StmExecutionQueue.registerDaemons(4)(storageService,executionContext)
  //        StmDaemons.start()
  //        try {
  //          Thread.sleep(1000) // Allow platform to start
  //
  //          val treeId = UUID.randomUUID().toString
  //
  //          //val result: String = Await.result(Http((url(baseUrl) / "cluster" / treeId).GET OK as.String), 30.seconds)
  //
  //          def insert(label:String, item:Any): String = {
  //            val request = (url(baseUrl) / "cluster" / treeId).addQueryParameter("label", label)
  //            Await.result(Http(request.PUT << JacksonValue(item).toString OK as.String), 30.seconds)
  //          }
  //          def query(item:Any): String = {
  //            val request = (url(baseUrl) / "cluster" / treeId / "find")
  //            Await.result(Http(request.POST << JacksonValue(item).toString OK as.String), 30.seconds)
  //          }
  //          def info(): String = {
  //            val request = (url(baseUrl) / "cluster" / treeId / "config")
  //            Await.result(Http(request.GET OK as.String), 30.seconds)
  //          }
  //
  //          println(insert("A", Map("value" -> 5)))
  //          println(query(Map("value" -> 5)))
  //          println(info())
  //
  //          Await.result(Http((url(baseUrl) / "sys" / "shutdown").GET OK as.String), 30.seconds)
  //          Await.result(StmDaemons.join(), 300.minutes)
  //          Thread.sleep(1000) // Allow rest of processes to complete
  //        } finally {
  //          Await.result(StmDaemons.stop(), 10.seconds)
  //        }
  //      }
  //    }


}
