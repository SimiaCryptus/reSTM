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

import _root_.util.Util
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerSuite
import stm.{STMPtr, STMTxn, STMTxnCtx}
import storage.Restm.PointerType
import storage._
import storage.actors.RestmActors
import storage.remote.{RestmCluster, RestmHttpClient, RestmInternalRestmHttpClient}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

abstract class StmSpecBase extends WordSpec with MustMatchers with BeforeAndAfterEach {

  override def afterEach() {
    Util.clearMetrics()
  }

  implicit def cluster: Restm

  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))

  "Transactional Pointers" should {
    //    def randomUUIDs: Stream[String] = Stream.continually(UUID.randomUUID().toString.take(8))
    "support basic operations" in {
      val id: PointerType = new PointerType
      Await.result(new STMTxn[String] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[String] = {
          val ptr: STMPtr[String]#SyncApi = new STMPtr[String](id).sync
          ptr.init("foo")
          ptr.read mustBe "foo"
          Future.successful("OK")
        }
      }.txnRun(cluster), 30.seconds) mustBe "OK"
      Await.result(new STMTxn[String] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[String] = {
          new STMPtr[String](id).read.map(x => x)
        }
      }.txnRun(cluster), 30.seconds) mustBe "foo"
      Await.result(new STMTxn[String] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[String] = {
          val ptr: STMPtr[String]#SyncApi = new STMPtr[String](id).sync
          ptr.read mustBe "foo"
          ptr.write("bar")
          Future.successful("OK")
        }
      }.txnRun(cluster), 30.seconds) mustBe "OK"
      Await.result(new STMTxn[String] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[String] = {
          new STMPtr[String](id).read.map(x => x)
        }
      }.txnRun(cluster), 30.seconds) mustBe "bar"
    }
    "support in-txn overwrites" in {
      val id: PointerType = new PointerType
      Await.result(new STMTxn[String] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[String] = {
          val ptr: STMPtr[String]#SyncApi = new STMPtr[String](id).sync
          ptr.init("foo")
          ptr.read mustBe "foo"
          ptr.write("bar")
          ptr.read mustBe "bar"
          Future.successful("OK")
        }
      }.txnRun(cluster), 30.seconds) mustBe "OK"
      Await.result(new STMTxn[String] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[String] = {
          new STMPtr[String](id).read.map(x => x)
        }
      }.txnRun(cluster), 30.seconds) mustBe "bar"
      Await.result(new STMTxn[String] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[String] = {
          val ptr: STMPtr[String]#SyncApi = new STMPtr[String](id).sync
          ptr.read mustBe "bar"
          ptr.write("jack")
          ptr.read mustBe "jack"
          ptr.write("jill")
          ptr.read mustBe "jill"
          Future.successful("OK")
        }
      }.txnRun(cluster), 30.seconds) mustBe "OK"
      Await.result(new STMTxn[String] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[String] = {
          new STMPtr[String](id).read.map(x => x)
        }
      }.txnRun(cluster), 30.seconds) mustBe "jill"
    }
  }

}

class LocalStmSpec extends StmSpecBase with BeforeAndAfterEach {
  val cluster = LocalRestmDb()

  override def beforeEach() {
    super.beforeEach()
    cluster.internal.asInstanceOf[RestmActors].clear()
  }
}

class LocalClusterStmSpec extends StmSpecBase with BeforeAndAfterEach {
  //  private val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
  //    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))
  val shards: List[RestmActors] = (0 until 8).map(_ => new RestmActors()).toList
  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))

  override def beforeEach() {
    super.beforeEach()
    shards.foreach(_.clear())
  }
}

class ServletStmSpec extends StmSpecBase with OneServerPerSuite {
  val cluster = new RestmHttpClient(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))
}

class ActorServletStmSpec extends StmSpecBase with OneServerPerSuite {
  val cluster = new RestmImpl(new RestmInternalRestmHttpClient(s"http://localhost:$port")(newExeCtx))(newExeCtx)
  private val newExeCtx: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build()))
}

object StmSpecBase {
}

