import java.util.UUID
import java.util.concurrent.Executors

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

object StmExecutionSpecBase {
  def recursiveTask(counter: STMPtr[java.lang.Integer], n:Int)(cluster: Restm, executionContext: ExecutionContext) : TaskResult[String] = {
    val x = Await.result(new STMTxn[Int] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Int] = {
        counter.read().map(_ + 1).flatMap(x => counter.write(x).map(_ => x))
      }
    }.txnRun(cluster)(executionContext), 100.milliseconds)
    if (n>1) {
      val function: (Restm, ExecutionContext) => TaskResult[String] = recursiveTask(counter,n-1) _
      new Task.TaskContinue(newFunction = function, queue = StmExecutionQueue)
    } else {
      new Task.TaskSuccess("foo")
    }
  }
}

abstract class StmExecutionSpecBase extends WordSpec with MustMatchers {
  implicit def cluster: Restm
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

  "TreeCollection" should {
    val collection = new TreeCollection[String](new PointerType)
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr)
    "support sorting" in {
      StmDaemons.start()
      try {
        StmExecutionQueue.registerDaemons(1)
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
        StmExecutionQueue.registerDaemons(1)
        val hasRun = new STMPtr[java.lang.Integer](new PointerType)
        hasRun.atomic.sync.init(0)
        Await.result(StmExecutionQueue.atomic.sync.add((cluster, executionContext) => {
          hasRun.atomic(cluster, executionContext).sync.write(1)
          new Task.TaskSuccess("foo")
        }).atomic().sync.map(StmExecutionQueue, (value, cluster, executionContext) => {
          require(value=="foo")
          hasRun.atomic(cluster, executionContext).sync.write(2)
          new Task.TaskSuccess("bar")
        }).future, 10.seconds)
        hasRun.atomic.sync.readOpt mustBe Some(2)
      } finally {
        Await.result(StmDaemons.stop(), 30.seconds)
      }
    }
    "support futures" in {
      StmDaemons.start()
      try {
        StmExecutionQueue.registerDaemons(1)
        val hasRun = new STMPtr[java.lang.Integer](new PointerType)
        hasRun.atomic.sync.init(0)
        val task: Task[String] = StmExecutionQueue.atomic.sync.add((cluster, executionContext) => {
          hasRun.atomic(cluster, executionContext).sync.write(1)
          new Task.TaskSuccess("foo")
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
        StmExecutionQueue.registerDaemons(1)
        val counter = new STMPtr[java.lang.Integer](new PointerType)
        counter.atomic.sync.init(0)
        val count = 20
        val task = StmExecutionQueue.atomic.sync.add(StmExecutionSpecBase.recursiveTask(counter,count) _)
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
        StmDaemons.config.atomic().sync.add(DaemonConfig("SimpleTest/StmDaemons", (cluster: Restm, executionContext:ExecutionContext) => {
          while(!Thread.interrupted()) {
            new STMTxn[Integer] {
              override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Integer] = {
                counter.read().flatMap(prev=>counter.write(prev+1).map(_=>prev+1))
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
  override def beforeEach() {
    cluster.internal.asInstanceOf[RestmActors].clear()
  }

  val cluster = LocalRestmDb()
}

class LocalClusterStmExecutionSpec extends StmExecutionSpecBase with BeforeAndAfterEach {
  private val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))
  val shards = (0 until 8).map(_ => new RestmActors()).toList

  override def beforeEach() {
    shards.foreach(_.clear())
  }

  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8)))
}

class ServletStmExecutionSpec extends StmExecutionSpecBase with OneServerPerSuite {
  val cluster = new RestmHttpClient(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8)))
}



class ActorServletStmExecutionSpec extends StmExecutionSpecBase with OneServerPerSuite {
  private val newExeCtx: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))
  val cluster = new RestmImpl(new RestmInternalRestmHttpClient(s"http://localhost:$port")(newExeCtx))(newExeCtx)
}

