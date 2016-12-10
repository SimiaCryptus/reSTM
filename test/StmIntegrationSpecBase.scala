import java.util.concurrent.Executors
import java.util.{Date, UUID}

import _root_.util.OperationMetrics
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm.lib0.StmDaemons.DaemonConfig
import stm.lib0.Task.TaskResult
import stm.lib0._
import stm.{STMPtr, STMTxn, STMTxnCtx}
import storage.Restm._
import storage.data.{JacksonValue, KryoValue}
import storage.util._
import storage.{RestmActors, _}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

object StmIntegrationSpecBase {
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


abstract class MetricsSpecBase extends WordSpec with MustMatchers {
  implicit def cluster: Restm
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  "Metrics" should {
    val collection = TreeCollection.static[String](new PointerType)
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr)
    "work" in {
      StmExecutionQueue.start(1)
      val input = randomUUIDs.take(50).toSet
      input.foreach(collection.atomic.sync.add(_))
      val sortTask: Task[LinkedList[String]] = collection.atomic.sync.sort()
      def now = new Date()
      val timeout = new Date(now.getTime + 30.seconds.toMillis)
      while(!sortTask.future.isCompleted && timeout.after(now)) {
        println(JacksonValue(sortTask.atomic.sync.getStatusTrace()).pretty)
        Thread.sleep(15000)
      }
      val sortResult: LinkedList[String] = Await.result(sortTask.future, 5.seconds)
      val output = sortResult.stream().toList
      output mustBe input.toList.sorted
    }
  }
}

class LocalClusterMetricsSpec extends MetricsSpecBase with BeforeAndAfterEach {
  private val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val shards = (0 until 8).map(_ => new RestmActors()(pool)).toList

  override def beforeEach() {
    shards.foreach(_.clear())
  }

  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}


abstract class StmIntegrationSpecBase extends WordSpec with MustMatchers {
  implicit def cluster: Restm
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  "Transactional Pointers" should {

    "basic writes" in {

      val ptr = STMPtr.static[String](new PointerType)

      Await.result(new STMTxn[Option[String]] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt()
        }
      }.txnRun(cluster)(executionContext), 30.seconds) mustBe None

      Await.result(new STMTxn[Unit] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.write("true")
        }
      }.txnRun(cluster)(executionContext), 30.seconds)

      Await.result(new STMTxn[String] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.read()
        }
      }.txnRun(cluster)(executionContext), 30.seconds) mustBe "true"

    }

  }

  "Transactional History" should {
    "recover orphaned trasactions" in {

      val collection = TreeSet.static[String](new PointerType)

      // Bootstrap collection to reduce contention at root nodes via serial inserts
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
      }

      // Insert collection and expire transactions (never commit nor rollback)
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) Try {
        Await.result(new STMTxn[Unit] {
          override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = Future {
            collection.sync.contains(item) mustBe false
            collection.sync.add(item)
            collection.sync.contains(item) mustBe true
          }
        }.testAbandoned().txnRun(cluster)(executionContext), 30.seconds)
      }
      Thread.sleep(5000)

      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
      }

      OperationMetrics.metrics.map(e => e._1 + ": " + e._2.toString).foreach(System.out.println)
      OperationMetrics.metrics.clear()
    }

    "recover orphaned pointers" in {

      val collection = TreeSet.static[String](new PointerType)

      // Bootstrap collection to reduce contention at root nodes via serial inserts
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
      }

      // Insert collection and expire transactions (never commit nor rollback)
      RestmImpl.failChainedCalls = true
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) Try {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
      }
      RestmImpl.failChainedCalls = false
      Thread.sleep(5000)

      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
      }

      OperationMetrics.metrics.map(e => e._1 + ": " + e._2.toString).foreach(System.out.println)
      OperationMetrics.metrics.clear()
    }

  }

  "TreeSet" should {
    def randomUUIDs: Stream[String] = Stream.continually(UUID.randomUUID().toString.take(8))
    "support basic operations" in {
      val collection = TreeSet.static[String](new PointerType)
      for (item <- randomUUIDs.take(5)) {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
      }
    }
    "support concurrent operations" in {
      val collection = TreeSet.static[String](new PointerType)
      // Bootstrap collection synchronously to control contention
      for (item <- randomUUIDs.take(5)) {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
      }
      // Run concurrent add/delete tests
      val futures = for (item <- randomUUIDs.take(10)) yield Future {
        try {
          for (i <- 0 until 10) {
            collection.atomic.sync.contains(item) mustBe false
            collection.atomic.sync.add(item)
            collection.atomic.sync.contains(item) mustBe true
            collection.atomic.sync.remove(item)
            collection.atomic.sync.contains(item) mustBe false
          }
        } catch {
          case e => throw new RuntimeException(s"Error in item $item", e)
        }
      }
      Await.result(Future.sequence(futures), 1.minutes)
    }
  }

  "TreeCollection" should {
    val collection = TreeCollection.static[String](new PointerType)
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr)
    "support basic operations" in {
      val input = randomUUIDs.take(50).toSet
      input.foreach(collection.atomic.sync.add(_))
      val output = Stream.continually(collection.atomic.sync.get()).takeWhile(_.isDefined).map(_.get).toSet
      output mustBe input
    }
    "support sorting" in {
      StmExecutionQueue.start(1)
      val input = randomUUIDs.take(20).toSet
      input.foreach(collection.atomic.sync.add(_))
      val sortTask: Task[LinkedList[String]] = collection.atomic.sync.sort()
      val sortResult: LinkedList[String] = Await.result(sortTask.future, 30.seconds)
      val output = sortResult.stream().toList
      output mustBe input.toList.sorted
    }
  }

  "TreeMap" should {
    val collection = TreeMap.static[String,String](new PointerType)
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs: Stream[(String,String)] = Stream.continually((randomStr, randomStr))
    "support basic operations" in {
      for (item <- randomUUIDs.take(5)) {
        collection.atomic.sync.get(item._1) mustBe None
        collection.atomic.sync.contains(item._1) mustBe false
        collection.atomic.sync.add(item._1, item._2)
        collection.atomic.sync.get(item._1) mustBe Option(item._2)
        collection.atomic.sync.contains(item._1) mustBe true
      }
    }
    "support concurrent inserts" in {
      // Bootstrap collection synchronously to control contention
      for (item <- randomUUIDs.take(5)) {
        collection.atomic.sync.get(item._1) mustBe None
        collection.atomic.sync.contains(item._1) mustBe false
        collection.atomic.sync.add(item._1, item._2)
        collection.atomic.sync.get(item._1) mustBe Option(item._2)
        collection.atomic.sync.contains(item._1) mustBe true
      }
      // Run concurrent add/delete tests
      val futures = for (item <- randomUUIDs.take(20)) yield Future {
        try {
          println(item)
          collection.atomic.sync.get(item._1) mustBe None
          collection.atomic.sync.contains(item._1) mustBe false
          collection.atomic.sync.add(item._1, item._2)
          collection.atomic.sync.get(item._1) mustBe Option(item._2)
          collection.atomic.sync.contains(item._1) mustBe true
        } catch {
          case e => throw new RuntimeException(s"Error in item $item", e)
        }
      }
      Await.result(Future.sequence(futures), 1.minutes)
    }
    "support concurrent operations" in {
      // Bootstrap collection synchronously to control contention
      for (item <- randomUUIDs.take(5)) {
        collection.atomic.sync.get(item._1) mustBe None
        collection.atomic.sync.contains(item._1) mustBe false
        collection.atomic.sync.add(item._1, item._2)
        collection.atomic.sync.get(item._1) mustBe Option(item._2)
        collection.atomic.sync.contains(item._1) mustBe true
      }
      // Run concurrent add/delete tests
      val futures = for (item <- randomUUIDs.take(10)) yield Future {
        try {
          println(item)
          for (i <- 0 until 10) {
            collection.atomic.sync.get(item._1) mustBe None
            collection.atomic.sync.contains(item._1) mustBe false
            collection.atomic.sync.add(item._1, item._2)

            collection.atomic.sync.get(item._1) mustBe Option(item._2)
            collection.atomic.sync.contains(item._1) mustBe true

            collection.atomic.sync.remove(item._1)
            collection.atomic.sync.get(item._1) mustBe None
            collection.atomic.sync.contains(item._1) mustBe false
          }
        } catch {
          case e => throw new RuntimeException(s"Error in item $item", e)
        }
      }
      Await.result(Future.sequence(futures), 1.minutes)
    }
  }

  "LinkedList" should {
    def randomUUIDs: Stream[String] = Stream.continually(UUID.randomUUID().toString.take(8))
    "support basic operations" in {
      val collection = LinkedList.static[String](new PointerType)
      val input: List[String] = randomUUIDs.take(50).toList
      input.foreach(collection.atomic.sync.add(_))
      val output = Stream.continually(collection.atomic.sync.remove).takeWhile(_.isDefined).map(_.get).toList
      input mustBe output
    }
    "support stream iteration" in {
      val collection = LinkedList.static[String](new PointerType)
      val input: List[String] = randomUUIDs.take(50).toList
      input.foreach(collection.atomic.sync.add(_))
      val output = collection.stream().toList
      input mustBe output
    }
  }

  "StmExecutionQueue" should {
    "support queued and chained operations" in {
      StmExecutionQueue.start(1)
      val hasRun = STMPtr.static[java.lang.Integer](new PointerType)
      hasRun.atomic.sync.init(0)
      StmExecutionQueue.atomic.sync.add((cluster, executionContext) => {
        hasRun.atomic(cluster, executionContext).sync.write(1)
        new Task.TaskSuccess("foo")
      }).atomic.map(StmExecutionQueue, (value, cluster, executionContext) => {
        require(value=="foo")
        hasRun.atomic(cluster, executionContext).sync.write(2)
        new Task.TaskSuccess("bar")
      })
      Thread.sleep(1000)
      hasRun.atomic.sync.readOpt mustBe Some(2)
    }
    "support futures" in {
      StmExecutionQueue.start(1)
      val hasRun = STMPtr.static[java.lang.Integer](new PointerType)
      hasRun.atomic.sync.init(0)
      val task: Task[String] = StmExecutionQueue.atomic.sync.add((cluster, executionContext) => {
        hasRun.atomic(cluster, executionContext).sync.write(1)
        new Task.TaskSuccess("foo")
      })
      Await.result(task.future, 30.seconds) mustBe "foo"
      hasRun.atomic.sync.readOpt mustBe Some(1)
    }
    "support continued operations" in {
      StmExecutionQueue.start(1)
      val counter = STMPtr.static[java.lang.Integer](new PointerType)
      counter.atomic.sync.init(0)
      val count = 20
      val task = StmExecutionQueue.atomic.sync.add(StmIntegrationSpecBase.recursiveTask(counter,count) _)
      Await.result(task.future, 10.seconds)
      counter.atomic.sync.readOpt mustBe Some(count)
    }
  }

  "StmDaemons" should {
    "support named daemons" in {
      val monitor = StmDaemons.init()
      val hasRun = STMPtr.static[java.lang.Integer](new PointerType)
      hasRun.atomic.sync.init(0)
      StmDaemons.config.atomic.sync.add(new DaemonConfig("SimpleTest/StmDaemons", KryoValue((cluster: Restm, executionContext:ExecutionContext) => {
        while(!Thread.interrupted()) {
          new STMTxn[Integer] {
            override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Integer] = {
              hasRun.read().flatMap(prev=>hasRun.write(prev+1).map(_=>prev+1))
            }
          }.txnRun(cluster)(executionContext)
          Thread.sleep(100)
        }
      })))
      Thread.sleep(1500)
      val ticks: Integer = hasRun.atomic.sync.readOpt.get
      println(ticks)
      require(ticks > 1)
      monitor.interrupt()
      StmDaemons.threads.values.foreach(_.interrupt())
      Thread.sleep(500)
      val ticks2: Integer = hasRun.atomic.sync.readOpt.get
      Thread.sleep(500)
      val ticks3: Integer = hasRun.atomic.sync.readOpt.get
      require(ticks2 == ticks3)
    }
  }

}

class LocalStmIntegrationSpec extends StmIntegrationSpecBase with BeforeAndAfterEach {
  override def beforeEach() {
    cluster.internal.asInstanceOf[RestmActors].clear()
  }

  val cluster = LocalRestmDb
}

class LocalClusterStmIntegrationSpec extends StmIntegrationSpecBase with BeforeAndAfterEach {
  private val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val shards = (0 until 8).map(_ => new RestmActors()(pool)).toList

  override def beforeEach() {
    shards.foreach(_.clear())
  }

  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}

class ServletStmIntegrationSpec extends StmIntegrationSpecBase with OneServerPerTest {
  val cluster = new RestmProxy(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}

class ActorServletStmIntegrationSpec extends StmIntegrationSpecBase with OneServerPerTest {
  private val newExeCtx: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val cluster = new RestmImpl(new InternalRestmProxy(s"http://localhost:$port")(newExeCtx))(newExeCtx)
}

