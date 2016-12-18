import java.util.concurrent.Executors
import java.util.{Date, UUID}

import _root_.util.Metrics
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm.collection.{LinkedList, TreeCollection, TreeMap, TreeSet}
import stm.concurrent.Task.TaskResult
import stm.concurrent.{StmExecutionQueue, Task}
import stm.{STMPtr, STMTxn, STMTxnCtx}
import storage.Restm._
import storage.data.JacksonValue
import storage.remote.{RestmCluster, RestmHttpClient, RestmInternalRestmHttpClient}
import storage.{RestmActors, _}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

object StmCollectionSpecBase {
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

abstract class StmCollectionSpecBase extends WordSpec with MustMatchers with BeforeAndAfterEach {

  override def afterEach() {
    Metrics.clear()
  }

  implicit def cluster: Restm
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  "TreeSet" should {
    def randomUUIDs: Stream[String] = Stream.continually(UUID.randomUUID().toString.take(8))
    "support basic operations" in {
      val collection = TreeSet.static[String](new PointerType)
      for (item <- randomUUIDs.take(5)) {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
      }
      println(JacksonValue.simple(Metrics.get()).pretty)
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
      println(JacksonValue.simple(Metrics.get()).pretty)
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
      input.foreach(collection.atomic().sync.add(_))
      val output = Stream.continually(collection.atomic().sync.remove()).takeWhile(_.isDefined).map(_.get).toList
      input mustBe output
    }
    "support concurrency" in {
      val threadCount = 100
      val syncTimeout = 60.seconds
      val maxRetries = 1000
      val strictness = 0.2
      val size = 5000
      val totalTimeout = 5.minutes

      val input = randomUUIDs.take(size).toSet
      val output = new mutable.HashSet[String]()
      val inputBuffer = new mutable.HashSet[String]()
      inputBuffer ++= input
      val collection = LinkedList.static[String](new PointerType)
      val threads = (0 to threadCount).map(_ => new Thread(new Runnable {
        override def run(): Unit = {
          input.filter(x=>inputBuffer.synchronized(inputBuffer.remove(x))).foreach(input => {
            collection.atomic(maxRetries = maxRetries).sync(syncTimeout).add(input, strictness)
            collection.atomic(maxRetries = maxRetries).sync(syncTimeout).remove(strictness).map(x=>output.synchronized(output += x))
          })
        }
      }))
      threads.foreach(_.start())
      def now = new Date()
      val timeout = new Date(now.getTime + totalTimeout.toMillis)
      while(threads.exists(_.isAlive))  {
        if(!timeout.after(now)) throw new RuntimeException("Time Out")
        Thread.sleep(100)
      }
      println(JacksonValue.simple(Metrics.get()).pretty)
      input.size mustBe output.size
      input mustBe output
    }
    "support stream iteration" in {
      val collection = LinkedList.static[String](new PointerType)
      val input: List[String] = randomUUIDs.take(10).toList
      input.foreach(collection.atomic().sync.add(_))
      val output = collection.atomic().sync.stream().toList
      input mustBe output
    }
  }

}

class LocalStmCollectionSpec extends StmCollectionSpecBase with BeforeAndAfterEach {
  override def beforeEach() {
    super.beforeEach()
    cluster.internal.asInstanceOf[RestmActors].clear()
  }

  val cluster = LocalRestmDb
}

class LocalClusterStmCollectionSpec extends StmCollectionSpecBase with BeforeAndAfterEach {
  private val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val shards = (0 until 8).map(_ => new RestmActors()(pool)).toList

  override def beforeEach() {
    super.beforeEach()
    shards.foreach(_.clear())
  }

  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}

class ServletStmCollectionSpec extends StmCollectionSpecBase with OneServerPerTest {
  val cluster = new RestmHttpClient(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}



class ActorServletStmCollectionSpec extends StmCollectionSpecBase with OneServerPerTest {
  private val newExeCtx: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val cluster = new RestmImpl(new RestmInternalRestmHttpClient(s"http://localhost:$port")(newExeCtx))(newExeCtx)
}

