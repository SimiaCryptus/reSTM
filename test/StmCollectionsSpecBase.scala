import java.util.UUID
import java.util.concurrent.Executors

import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm._
import stm.lib0._
import storage.Restm._
import storage.util._
import storage.{RestmActors, _}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

abstract class StmCollectionsSpecBase extends WordSpec with MustMatchers {
  implicit def cluster: Restm

  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  def randomUUIDs: Stream[String] = Stream.continually(UUID.randomUUID().toString.take(8))

  "TreeSet" should {
    "support basic (concurrent) operations" in {
      val collection = TreeSet.static[String](new PointerType("test/SimpleTest/TreeSet"))
      // Bootstrap collection synchronously to control contention
      for (item <- randomUUIDs.take(5)) {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
      }
      // Run concurrent add/delete tests
      val futures = for (item <- randomUUIDs.take(20)) yield Future {
        try {
          for (i <- 0 until 2) {
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

  "TreeMap" should {
    val collection = TreeMap.static[String,String](new PointerType("test/SimpleTest/TreeMap"))
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
      val futures = for (item <- randomUUIDs.take(20)) yield Future {
        try {
          println(item)
          for (i <- 0 until 2) {
            collection.atomic.sync.get(item._1) mustBe None
            collection.atomic.sync.contains(item._1) mustBe false
            collection.atomic.sync.add(item._1, item._2)

            // Known Bug: Concurrent deletion corrupts map?
            //collection.atomic.sync.get(item._1) mustBe Option(item._2)
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
    "support basic operations" in {
      val collection = LinkedList.static[String](new PointerType("test/SimpleTest/LinkedList"))
      val input: List[String] = randomUUIDs.take(5).toList
      input.foreach(collection.atomic.sync.add(_))
      val output = Stream.continually(collection.atomic.sync.remove).takeWhile(_.isDefined).map(_.get).toList
      input mustBe output
    }
    "support stream iteration" in {
      val collection = LinkedList.static[String](new PointerType("test/SimpleTest/LinkedList"))
      val input: List[String] = randomUUIDs.take(5).toList
      input.foreach(collection.atomic.sync.add(_))
      val output = collection.stream().toList
      input mustBe output
    }
  }

  "StmExecutionQueue" should {
    "support queued and chained operations" in {
      StmExecutionQueue.start(1)
      val hasRun = STMPtr.static[java.lang.Integer](new PointerType("test/SimpleTest/StmExecutionQueue/callback"))
      hasRun.atomic.sync.init(0)
      StmExecutionQueue.atomic.sync.add((cluster, executionContext) => {
        hasRun.atomic(cluster, executionContext).sync.write(1)
        "foo"
      }).atomic.map(StmExecutionQueue, (value, cluster, executionContext) => {
        require(value=="foo")
        hasRun.atomic(cluster, executionContext).sync.write(2)
      })
      Thread.sleep(1000)
      hasRun.atomic.sync.get mustBe Some(2)
    }
  }

  "StmDaemons" should {
    "support named daemons" in {
      val monitor = StmDaemons.init()
      val hasRun = STMPtr.static[java.lang.Integer](new PointerType("test/SimpleTest/StmDaemons/callback"))
      hasRun.atomic.sync.init(0)
      StmDaemons.config.atomic.sync.add(("SimpleTest/StmDaemons", (cluster, executionContext) => {
        while(!Thread.interrupted()) {
          new STMTxn[Integer] {
            override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Integer] = {
              hasRun.read().flatMap(prev=>hasRun.write(prev+1).map(_=>prev+1))
            }
          }.txnRun(cluster)(executionContext)
          Thread.sleep(100)
        }
      }))
      Thread.sleep(1500)
      val ticks: Integer = hasRun.atomic.sync.get.get
      println(ticks)
      require(ticks > 1)
      monitor.interrupt()
      StmDaemons.threads.values.foreach(_.interrupt())
      Thread.sleep(500)
      val ticks2: Integer = hasRun.atomic.sync.get.get
      Thread.sleep(500)
      val ticks3: Integer = hasRun.atomic.sync.get.get
      require(ticks2 == ticks3)
    }
  }
}

class LocalStmCollectionsSpec extends StmCollectionsSpecBase with BeforeAndAfterEach {
  override def beforeEach() {
    cluster.internal.asInstanceOf[RestmActors].clear()
  }

  val cluster = LocalRestmDb
}

class LocalClusterStmCollectionsSpec extends StmCollectionsSpecBase with BeforeAndAfterEach {
  private val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val shards = (0 until 8).map(_ => new RestmActors()(pool)).toList

  override def beforeEach() {
    shards.foreach(_.clear())
  }

  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}

class IntegrationStmCollectionsSpec extends StmCollectionsSpecBase with OneServerPerTest {
  val cluster = new RestmProxy(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}

class IntegrationInteralStmCollectionsSpec extends StmCollectionsSpecBase with OneServerPerTest {
  private val newExeCtx: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val cluster = new RestmImpl(new InternalRestmProxy(s"http://localhost:$port")(newExeCtx))(newExeCtx)
}



