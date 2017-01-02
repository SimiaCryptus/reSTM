import java.util.concurrent.{Executors, TimeUnit}
import java.util.{Date, UUID}

import _root_.util.Util
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerSuite
import stm.collection._
import stm.task.Task.TaskResult
import stm.task.{StmExecutionQueue, Task}
import stm.{STMPtr, STMTxn, STMTxnCtx}
import storage.Restm._
import storage._
import storage.actors.RestmActors
import storage.remote.{RestmCluster, RestmHttpClient, RestmInternalRestmHttpClient}
import storage.types.JacksonValue

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Random

abstract class StmCollectionSpecBase extends WordSpec with BeforeAndAfterEach with MustMatchers {

  override def afterEach() {
    Util.clearMetrics()
  }

  implicit def cluster: Restm

  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))


  s"BatchedTreeCollection via ${getClass.getSimpleName}" must {
    def randomStr = UUID.randomUUID().toString.take(8)

    def randomUUIDs = Stream.continually(randomStr)

    List(1, 10, 100).foreach(items => {
      s"synchronous add and get with $items items" in {
        val collection = new BatchedTreeCollection[String](new PointerType)
        val input = randomUUIDs.take(items).sorted.toList
        input.map(List(_)).foreach(collection.atomic().sync.add(_))
        val output = Stream.continually(collection.atomic().sync.get()).takeWhile(_.isDefined).flatMap(_.get).sorted.toList
        output mustBe input
        output.size mustBe input.size
        output.toSet mustBe input.toSet
        println(JacksonValue.simple(Util.getMetrics).pretty)
      }
    })
    List(10, 100, 1000).foreach(items => {
      s"insert and toList with $items items" in {
        val collection = new BatchedTreeCollection[String](new PointerType)
        val input = randomUUIDs.take(items).toSet
        input.map(List(_)).foreach(collection.atomic().sync.add(_))
        val output = collection.atomic().stream().toSet
        output.size mustBe input.size
        output mustBe input
        println(JacksonValue.simple(Util.getMetrics).pretty)
      }
    })
  }


  s"DistributedScalar via ${getClass.getSimpleName}" must {
    List(1, 10, 100).foreach(items => {
      s"accumulates $items values" in {
        def randomStream = Stream.continually(Random.nextDouble())

        val collection = DistributedScalar.createSync()
        val input = randomStream.take(items).sorted.toList
        input.foreach(collection.atomic().sync.add(_))
        val outputSum = collection.atomic().sync.get()
        val inputSum = input.sum
        outputSum mustBe inputSum +- 0.000001
        println(JacksonValue.simple(Util.getMetrics).pretty)
      }
    })
  }

  s"TreeSet via ${getClass.getSimpleName}" must {
    def randomUUIDs: Stream[String] = Stream.continually(UUID.randomUUID().toString.take(12))

    List(1, 10, 100).foreach(items => s"synchronous insert and verify with $items items" in {
      val collection = new TreeSet[String](new PointerType)
      val input = randomUUIDs.take(items)
      for (item <- input) {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
        //        println(s"Inserted $item")
      }
      for (item <- input) {
        collection.atomic.sync.contains(item) mustBe true
        collection.atomic.sync.remove(item)
        collection.atomic.sync.contains(item) mustBe false
        //        println(s"Deleted $item")
      }
      println(JacksonValue.simple(Util.getMetrics).pretty)
    })
    List(1, 5, 10).foreach(threads =>
      s"concurrent add, verify, and remove with $threads threads" in {
        //require(false)
        val collection = new TreeSet[String](new PointerType)
        // Bootstrap collection synchronously to control contention
        val sync: collection.AtomicApi#SyncApi = collection.atomic.sync(30.seconds)
        for (item <- randomUUIDs.take(10)) {
          sync.contains(item) mustBe false
          sync.add(item)
          sync.contains(item) mustBe true
        }
        val threadPool = Executors.newFixedThreadPool(threads)
        // Run concurrent add/delete tests
        val executionContext2 = ExecutionContext.fromExecutor(threadPool)
        val futures = for (item <- randomUUIDs.take(200).distinct) yield Future {
          try {
            sync.contains(item) mustBe false
            for (_ <- 0 until 10) {
              sync.add(item)
              sync.contains(item) mustBe true
              sync.remove(item) mustBe true
              sync.contains(item) mustBe false
            }
          } catch {
            case e: Throwable => throw new RuntimeException(s"Error in item $item", e)
          }
        }(executionContext2)
        Await.result(Future.sequence(futures), 1.minutes)
        println(JacksonValue.simple(Util.getMetrics).pretty)
        threadPool.shutdown()
        threadPool.awaitTermination(1, TimeUnit.MINUTES)
      }
    )
  }


  s"TreeCollection via ${getClass.getSimpleName}" must {
    def randomStr = UUID.randomUUID().toString.take(8)

    def randomUUIDs = Stream.continually(randomStr)

    List(10, 100, 1000).foreach(items => {
      s"synchronous add and get with $items items" in {
        val collection = new TreeCollection[String](new PointerType)
        val input = randomUUIDs.take(items).toSet
        input.foreach(collection.atomic().sync.add(_))
        val output = Stream.continually(collection.atomic().sync.get()).takeWhile(_.isDefined).map(_.get).toSet
        output mustBe input
        println(JacksonValue.simple(Util.getMetrics).pretty)
      }
    })
    List(10, 100, 1000).foreach(items => {
      s"insert and toList with $items items" in {
        val collection = new TreeCollection[String](new PointerType)
        val input = randomUUIDs.take(items).toSet
        input.foreach(collection.atomic().sync.add(_))
        collection.atomic().sync.toList().toSet mustBe input
        println(JacksonValue.simple(Util.getMetrics).pretty)
      }
    })
  }


  //  s"TreeMap via ${getClass.getSimpleName}" must {
  //    val collection = new TreeMap[String,String](new PointerType)
  //    def randomStr = UUID.randomUUID().toString.take(8)
  //    def randomUUIDs: Stream[(String,String)] = Stream.continually((randomStr, randomStr))
  //    List(10,100,200).foreach(items=> {
  //      s"synchronous add and verify with $items items" in {
  //        for (item <- randomUUIDs.take(items)) {
  //          collection.atomic.sync.get(item._1) mustBe None
  //          collection.atomic.sync.contains(item._1) mustBe false
  //          collection.atomic.sync.add(item._1, item._2)
  //          collection.atomic.sync.get(item._1) mustBe Option(item._2)
  //          collection.atomic.sync.contains(item._1) mustBe true
  //        }
  //        println(JacksonValue.simple(Util.getMetrics()).pretty)
  //      }
  //    })
  //    List(10,100,200).foreach(items=> {
  //      s"concurrent add and verify with $items items" in {
  //        // Bootstrap collection synchronously to control contention
  //        for (item <- randomUUIDs.take(5)) {
  //          collection.atomic.sync.get(item._1) mustBe None
  //          collection.atomic.sync.contains(item._1) mustBe false
  //          collection.atomic.sync.add(item._1, item._2)
  //          collection.atomic.sync.get(item._1) mustBe Option(item._2)
  //          collection.atomic.sync.contains(item._1) mustBe true
  //        }
  //        // Run concurrent add/delete tests
  //        val futures = for (item <- randomUUIDs.take(items)) yield Future {
  //          try {
  //            println(item)
  //            collection.atomic.sync.get(item._1) mustBe None
  //            collection.atomic.sync.contains(item._1) mustBe false
  //            collection.atomic.sync.add(item._1, item._2)
  //            collection.atomic.sync.get(item._1) mustBe Option(item._2)
  //            collection.atomic.sync.contains(item._1) mustBe true
  //          } catch {
  //            case e => throw new RuntimeException(s"Error in item $item", e)
  //          }
  //        }
  //        Await.result(Future.sequence(futures), 1.minutes)
  //        println(JacksonValue.simple(Util.getMetrics()).pretty)
  //      }
  //    })
  //    List(10,100,200).foreach(items=> {
  //      s"concurrent add, verify, and remove with $items items" in {
  //        // Bootstrap collection synchronously to control contention
  //        for (item <- randomUUIDs.take(5)) {
  //          collection.atomic.sync.get(item._1) mustBe None
  //          collection.atomic.sync.contains(item._1) mustBe false
  //          collection.atomic.sync.add(item._1, item._2)
  //          collection.atomic.sync.get(item._1) mustBe Option(item._2)
  //          collection.atomic.sync.contains(item._1) mustBe true
  //        }
  //        val threads: Int = 10
  //        // Run concurrent add/delete tests
  //        val futures = for (item <- randomUUIDs.take(threads)) yield Future {
  //          try {
  //            println(item)
  //            for (i <- 0 until items/threads) {
  //              collection.atomic.sync.get(item._1) mustBe None
  //              collection.atomic.sync.contains(item._1) mustBe false
  //              collection.atomic.sync.add(item._1, item._2)
  //
  //              collection.atomic.sync.get(item._1) mustBe Option(item._2)
  //              collection.atomic.sync.contains(item._1) mustBe true
  //
  //              collection.atomic.sync.remove(item._1)
  //              collection.atomic.sync.get(item._1) mustBe None
  //              collection.atomic.sync.contains(item._1) mustBe false
  //            }
  //          } catch {
  //            case e => throw new RuntimeException(s"Error in item $item", e)
  //          }
  //        }
  //        Await.result(Future.sequence(futures), 1.minutes)
  //        println(JacksonValue.simple(Util.getMetrics()).pretty)
  //      }
  //    })
  //  }

  s"SimpleLinkedList via ${getClass.getSimpleName}" must {
    def randomUUIDs: Stream[String] = Stream.continually(UUID.randomUUID().toString.take(8))

    List(10, 100, 1000).foreach(items => {
      s"synchronous add and remove with $items items" in {
        val collection = SimpleLinkedList.static[String](new PointerType)
        val input: List[String] = randomUUIDs.take(items).toList
        input.foreach(collection.atomic().sync.add(_))
        val output = Stream.continually(collection.atomic().sync.remove()).takeWhile(_.isDefined).map(_.get).toList
        output mustBe input
        println(JacksonValue.simple(Util.getMetrics).pretty)
      }
    })
    List(10, 100, 1000).foreach(items => {
      s"concurrent add and remove with $items items" in {
        val threadCount = 20
        val syncTimeout = 60.seconds
        val maxRetries = 1000
        val totalTimeout = 5.minutes

        val input = randomUUIDs.take(items).toSet
        val output = new mutable.HashSet[String]()
        val inputBuffer = new mutable.HashSet[String]()
        inputBuffer ++= input
        val collection = SimpleLinkedList.static[String](new PointerType)
        val threads = (0 to threadCount).map(_ => new Thread(new Runnable {
          override def run(): Unit = {
            input.filter(x => inputBuffer.synchronized(inputBuffer.remove(x))).foreach(input => {
              collection.atomic(maxRetries = maxRetries).sync(syncTimeout).add(input)
              collection.atomic(maxRetries = maxRetries).sync(syncTimeout).remove().map(x => output.synchronized(output += x))
            })
          }
        }))
        threads.foreach(_.start())

        def now = new Date()

        val timeout = new Date(now.getTime + totalTimeout.toMillis)
        while (threads.exists(_.isAlive)) {
          if (!timeout.after(now)) throw new RuntimeException("Time Out")
          Thread.sleep(100)
        }
        println(JacksonValue.simple(Util.getMetrics).pretty)
        input.size mustBe output.size
        input mustBe output
        println(JacksonValue.simple(Util.getMetrics).pretty)
      }
    })
    List(10, 100, 1000).foreach(items => {
      s"stream iteration with $items items" in {
        val collection = SimpleLinkedList.static[String](new PointerType)
        val input: List[String] = randomUUIDs.take(items).toList
        input.foreach(collection.atomic().sync.add(_))
        val output = collection.atomic().sync.stream().toList
        input mustBe output
        println(JacksonValue.simple(Util.getMetrics).pretty)
      }
    })
  }

  s"IdQueue via ${getClass.getSimpleName}" must {
    def randomUUIDs: Stream[String] = Stream.continually(UUID.randomUUID().toString.take(8))

    List(10, 100, 1000).foreach(items => {
      s"synchronous add and remove with $items items" in {
        val collection = IdQueue.createSync[TestValue](8)
        val input: List[TestValue] = randomUUIDs.take(items).toList.map(new TestValue(_))
        input.foreach(collection.atomic().sync.add(_))
        collection.atomic().sync.size() mustBe items
        collection.atomic().sync.stream().map((item: TestValue) => {
          collection.atomic().sync.contains(item.id) mustBe true
          1
        }).sum mustBe items
        val output = Stream.continually(collection.atomic().sync.take()).takeWhile(_.isDefined).map(_.get).toList
        output.toSet mustBe input.toSet
        collection.atomic().sync.size() mustBe 0
        input.count((item: TestValue) => {
          val contains = collection.atomic().sync.contains(item.id)
          if (contains) println(s"Found ghost: ${item.id}")
          contains
        }) mustBe 0
        println(JacksonValue.simple(Util.getMetrics).pretty)
      }
    })
    List(10, 100, 1000).foreach(items => {
      s"concurrent add and remove with $items items" in {
        val syncTimeout = 60.seconds

        val input = randomUUIDs.take(items).toList.map(new TestValue(_))
        val pool = Executors.newFixedThreadPool(20)
        val exeCtx = ExecutionContext.fromExecutor(pool)
        val collection = IdQueue.createSync[TestValue](8)
        val shuffled = input.map(_ -> Random.nextDouble()).toList.sortBy(_._2).map(_._1)
        input.size mustBe shuffled.size
        val future: Future[List[TestValue]] = Future.sequence(
          input.map(input => Future {
            collection.atomic().sync(syncTimeout).add(input)
            collection.atomic().sync(syncTimeout).take(2)
          }(exeCtx))).map(_.flatten)
        val output = new mutable.HashSet[TestValue]()
        output ++= Await.result(future, 1.minutes)

        output ++= Stream.continually({
          collection.atomic().sync(syncTimeout).take()
        }).takeWhile(_.isDefined).toList.map(_.get)
        output.size mustBe input.size
        output.toSet mustBe input.toSet
        println(JacksonValue.simple(Util.getMetrics).pretty)

      }
    })
    List(10, 100, 1000).foreach(items => {
      s"stream iteration with $items items" in {
        val collection = IdQueue.createSync[TestValue](8)
        val input: List[TestValue] = randomUUIDs.take(items).toList.map(new TestValue(_))
        input.foreach(collection.atomic().sync.add(_))
        val output = collection.atomic().sync.stream().toList
        input.toSet mustBe output.toSet
        println(JacksonValue.simple(Util.getMetrics).pretty)
      }
    })
  }
}

class LocalStmCollectionSpec extends StmCollectionSpecBase with BeforeAndAfterEach {
  val cluster = LocalRestmDb()

  override def beforeEach() {
    super.beforeEach()
    cluster.internal.asInstanceOf[RestmActors].clear()
  }
}

class LocalClusterStmCollectionSpec extends StmCollectionSpecBase with BeforeAndAfterEach {
  val shards: List[RestmActors] = (0 until 8).map(_ => new RestmActors()).toList
  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))

  override def beforeEach() {
    super.beforeEach()
    shards.foreach(_.clear())
  }
}

class ServletStmCollectionSpec extends StmCollectionSpecBase with OneServerPerSuite {
  val cluster = new RestmHttpClient(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))
}

class ActorServletStmCollectionSpec extends StmCollectionSpecBase with OneServerPerSuite {
  val cluster = new RestmImpl(new RestmInternalRestmHttpClient(s"http://localhost:$port")(newExeCtx))(newExeCtx)
  private val newExeCtx: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build()))
}

class TestValue(val id: String = null) extends Identifiable {
  override def toString: String = id

  override def equals(other: Any): Boolean = other match {
    case that: TestValue =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[TestValue]

  override def hashCode(): Int = {
    val state = Seq(id)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object StmCollectionSpecBase {
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
