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
import dispatch._
import org.scalatest.{MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm.task.StmDaemons
import storage.remote.RestmHttpClient
import storage.types.JacksonValue
import util.Util

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}



class ClassificationServletSpec extends WordSpec with MustMatchers with OneServerPerTest {

      "Single Node Servlet" should {
        "provide demo cluster tree" in {
          val baseUrl = s"http://localhost:$port"
          implicit val cluster: RestmHttpClient = new RestmHttpClient(baseUrl)(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4,
            new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))
          implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4,
            new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))
          try {
            //ActorLog.enabled = true
            val timeout : Duration = 90.seconds
            Await.result(Http((url(baseUrl) / "sys" / "init").GET OK as.String), timeout)
            //StmExecutionQueue.get().verbose = true
            Thread.sleep(1000) // Allow platform to start
            new DTTestUtil(baseUrl, timeout = timeout).test(items = 10000)
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
