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
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import storage.Restm
import storage.Restm._
import storage.actors.RestmActors
import storage.remote.RestmHttpClient
import storage.types.TxnTime

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

abstract class RestmSpecBase extends WordSpec with MustMatchers {
  def cluster: Restm

  "reSTM Storage Layer" should {

    "commit new data" in {
      val ptrId = new PointerType
      val txnA = Await.result(Future.successful(TxnTime.next()), 30.seconds)
      require(txnA > new TimeStamp(0l,0l,0))

      Await.result(cluster.getPtr(ptrId), 30.seconds) mustBe None
      Await.result(cluster.lock(ptrId, txnA), 30.seconds) mustBe None
      Await.result(cluster.queueValue(ptrId, txnA, new ValueType("foo")), 30.seconds)
      Await.result(cluster.commit(txnA), 30.seconds)

      val txnB = Await.result(Future.successful(TxnTime.next()), 30.seconds)
      Await.result(cluster.getPtr(ptrId, txnB), 30.seconds) mustBe Some(new ValueType("foo"))
    }

    "revert data" in {
      val ptrId = new PointerType
      val txnA = Await.result(Future.successful(TxnTime.next()), 30.seconds)
      require(txnA > new TimeStamp(0l,0l,0))

      Await.result(cluster.getPtr(ptrId), 30.seconds) mustBe None
      Await.result(cluster.lock(ptrId, txnA), 30.seconds) mustBe None
      Await.result(cluster.queueValue(ptrId, txnA, new ValueType("foo")), 30.seconds)
      Await.result(cluster.reset(txnA), 30.seconds)

      val txnB = Await.result(Future.successful(TxnTime.next()), 30.seconds)
      Await.result(cluster.getPtr(ptrId, txnB), 30.seconds) mustBe None
    }
  }
}

class LocalRestmSpec extends RestmSpecBase with BeforeAndAfterEach {

  val cluster = LocalRestmDb()

  override def beforeEach() {
    cluster.internal.asInstanceOf[RestmActors].clear()
  }
}

class IntegrationSpec extends RestmSpecBase with OneServerPerTest {

  private val pool = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))
  val cluster = new RestmHttpClient(s"http://localhost:$port")(pool)
}


