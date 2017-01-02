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
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerSuite
import stm.collection.{LinkedList, TreeCollection}
import stm.task.Task.TaskResult
import stm.task.{DaemonConfig, StmDaemons, StmExecutionQueue, Task}
import stm.{STMPtr, STMTxn, STMTxnCtx}
import storage.Restm._
import storage._
import storage.actors.RestmActors
import storage.remote.{RestmCluster, RestmHttpClient, RestmInternalRestmHttpClient}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

abstract class StmExecutionSpecBase extends WordSpec with MustMatchers {
  implicit def cluster: Restm

  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))

  "TreeCollection" should {
    val collection = new TreeCollection[String](new PointerType)

    def randomStr = UUID.randomUUID().toString.take(8)

    def randomUUIDs = Stream.continually(randomStr)

    "support sorting" in {
      StmDaemons.start()
      try {
        StmExecutionQueue.get().registerDaemons()
        val input = randomUUIDs.take(20).toSet
        input.foreach(collection.atomic().sync.add(_))
        val sortTask = collection.atomic().sort().flatMap(_.future)
        val sortResult: LinkedList[String] = Await.result(sortTask, 30.seconds)
        val output = sortResult.atomic().sync.stream().toList
        output mustBe input.toList.sorted
      } finally {
        Await.result(StmDaemons.stop(), 30.seconds)
      }
    }
  }

  "StmExecutionQueue" should {
    "support queued and chained operations" in {
      StmDaemons.start()
      try {
        StmExecutionQueue.get().registerDaemons()
        val hasRun = new STMPtr[java.lang.Integer](new PointerType)
        hasRun.atomic.sync.init(0)
        Await.result(StmExecutionQueue.get().atomic.sync.add((cluster, executionContext) => {
          hasRun.atomic(cluster, executionContext).sync.write(1)
          Task.TaskSuccess("foo")
        }).atomic().sync.map(StmExecutionQueue.get(), (value, cluster, executionContext) => {
          require(value == "foo")
          hasRun.atomic(cluster, executionContext).sync.write(2)
          Task.TaskSuccess("bar")
        }).future, 10.seconds)
        hasRun.atomic.sync.readOpt mustBe Some(2)
      } finally {
        Await.result(StmDaemons.stop(), 30.seconds)
      }
    }
    "support futures" in {
      StmDaemons.start()
      try {
        StmExecutionQueue.get().registerDaemons()
        val hasRun = new STMPtr[java.lang.Integer](new PointerType)
        hasRun.atomic.sync.init(0)
        val task: Task[String] = StmExecutionQueue.get().atomic.sync.add((cluster, executionContext) => {
          hasRun.atomic(cluster, executionContext).sync.write(1)
          Task.TaskSuccess("foo")
        })
        Await.result(task.future, 30.seconds) mustBe "foo"
        hasRun.atomic.sync.readOpt mustBe Some(1)
      } finally {
        Await.result(StmDaemons.stop(), 30.seconds)
      }
    }
    "support continued operations" in {
      StmDaemons.start()
      try {
        StmExecutionQueue.get().registerDaemons()
        val counter = new STMPtr[java.lang.Integer](new PointerType)
        counter.atomic.sync.init(0)
        val count = 20
        val task = StmExecutionQueue.get().atomic.sync.add(StmExecutionSpecBase.recursiveTask(counter, count) _)
        Await.result(task.future, 10.seconds)
        counter.atomic.sync.readOpt mustBe Some(count)
      } finally {
        Await.result(StmDaemons.stop(), 30.seconds)
      }
    }
  }

  "StmDaemons" should {
    "support named daemons" in {
      StmDaemons.start()
      val counter = try {
        val counter = new STMPtr[java.lang.Integer](new PointerType)
        counter.atomic.sync.init(0)
        StmDaemons.config.atomic().sync.add(DaemonConfig("SimpleTest/StmDaemons", (cluster: Restm, executionContext: ExecutionContext) => {
          while (!Thread.interrupted()) {
            new STMTxn[Integer] {
              override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Integer] = {
                counter.read().flatMap(prev => counter.write(prev + 1).map(_ => prev + 1))
              }
            }.txnRun(cluster)(executionContext)
            Thread.sleep(100)
          }
        }))
        Thread.sleep(1500)
        val ticks: Integer = counter.atomic.sync.readOpt.get
        println(ticks)
        require(ticks > 1)
        counter
      } finally {
        Await.result(StmDaemons.stop(), 30.seconds)
      }
      val ticks2: Integer = counter.atomic.sync.readOpt.get
      Thread.sleep(500)
      val ticks3: Integer = counter.atomic.sync.readOpt.get
      require(ticks2 == ticks3)
    }
  }

}

class LocalStmExecutionSpec extends StmExecutionSpecBase with BeforeAndAfterEach {
  val cluster = LocalRestmDb()

  override def beforeEach() {
    cluster.internal.asInstanceOf[RestmActors].clear()
  }
}

class LocalClusterStmExecutionSpec extends StmExecutionSpecBase with BeforeAndAfterEach {
  //  private val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
  //    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))
  val shards: List[RestmActors] = (0 until 8).map(_ => new RestmActors()).toList
  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))

  override def beforeEach() {
    shards.foreach(_.clear())
  }
}

class ServletStmExecutionSpec extends StmExecutionSpecBase with OneServerPerSuite {
  val cluster = new RestmHttpClient(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))
}

class ActorServletStmExecutionSpec extends StmExecutionSpecBase with OneServerPerSuite {
  val cluster = new RestmImpl(new RestmInternalRestmHttpClient(s"http://localhost:$port")(newExeCtx))(newExeCtx)
  private val newExeCtx: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build()))
}

object StmExecutionSpecBase {
  def recursiveTask(counter: STMPtr[java.lang.Integer], n: Int)(cluster: Restm, executionContext: ExecutionContext): TaskResult[String] = {
    Await.result(new STMTxn[Int] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Int] = {
        counter.read().map(_ + 1).flatMap(x => counter.write(x).map(_ => x))
      }
    }.txnRun(cluster)(executionContext), 100.milliseconds)
    if (n > 1) {
      val function: (Restm, ExecutionContext) => TaskResult[String] = recursiveTask(counter, n - 1)
      Task.TaskContinue(newFunction = function, queue = StmExecutionQueue.get())
    } else {
      Task.TaskSuccess("foo")
    }
  }
}

