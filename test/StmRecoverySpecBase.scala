import java.util.UUID
import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerSuite
import stm.collection.TreeSet
import stm.task.Task.TaskResult
import stm.task.{StmExecutionQueue, Task}
import stm.{STMPtr, STMTxn, STMTxnCtx}
import storage.Restm._
import storage._
import storage.actors.RestmActors
import storage.remote.{RestmCluster, RestmHttpClient, RestmInternalRestmHttpClient}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

abstract class StmRecoverySpecBase extends WordSpec with MustMatchers {
  implicit def cluster: Restm

  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))

  "Transactional Pointers" should {
    "basic writes" in {
      val ptr = new STMPtr[String](new PointerType)
      Await.result(new STMTxn[Option[String]] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[String]] = {
          ptr.readOpt()
        }
      }.txnRun(cluster)(executionContext), 30.seconds) mustBe None
      Await.result(new STMTxn[Unit] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
          ptr.write("true")
        }
      }.txnRun(cluster)(executionContext), 30.seconds)
      Await.result(new STMTxn[String] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[String] = {
          ptr.read()
        }
      }.txnRun(cluster)(executionContext), 30.seconds) mustBe "true"
    }
  }

  "Transactional History" should {
    "recover orphaned trasactions" in {

      val collection = new TreeSet[String](new PointerType)

      // Bootstrap collection to reduce contention at root nodes via serial inserts
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
      }

      // Insert collection and expire transactions (never commit nor rollback)
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(1).toList) Try {
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

    }

    "recover orphaned pointers" in {

      val collection = new TreeSet[String](new PointerType)

      // Bootstrap collection to reduce contention at root nodes via serial inserts
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
      }

      // Insert collection and expire transactions (never commit nor rollback)
      RestmImpl.failChainedCalls = true
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(1).toList) Try {
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

    }

  }

}

class LocalStmRecoverySpec extends StmRecoverySpecBase with BeforeAndAfterEach {
  val cluster = LocalRestmDb()

  override def beforeEach() {
    cluster.internal.asInstanceOf[RestmActors].clear()
  }
}

class LocalClusterStmRecoverySpec extends StmRecoverySpecBase with BeforeAndAfterEach {
  //  private val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
  //    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))
  val shards: List[RestmActors] = (0 until 8).map(_ => new RestmActors()).toList
  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))

  override def beforeEach() {
    shards.foreach(_.clear())
  }
}

class ServletStmRecoverySpec extends StmRecoverySpecBase with OneServerPerSuite {
  val cluster = new RestmHttpClient(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))
}

class ActorServletStmRecoverySpec extends StmRecoverySpecBase with OneServerPerSuite {
  val cluster = new RestmImpl(new RestmInternalRestmHttpClient(s"http://localhost:$port")(newExeCtx))(newExeCtx)
  private val newExeCtx: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build()))
}

object StmRecoverySpecBase {
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

