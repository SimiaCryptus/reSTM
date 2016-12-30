import java.util.UUID
import java.util.concurrent.Executors

import _root_.util.Util
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerSuite
import stm.{STMPtr, STMTxn, STMTxnCtx}
import storage.Restm.PointerType
import storage._
import storage.actors.RestmActors
import storage.remote.{RestmCluster, RestmHttpClient, RestmInternalRestmHttpClient}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

object StmSpecBase {
}

abstract class StmSpecBase extends WordSpec with MustMatchers with BeforeAndAfterEach {

  override def afterEach() {
    Util.clearMetrics()
  }

  implicit def cluster: Restm
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

  "Transactional Pointers" should {
    def randomUUIDs: Stream[String] = Stream.continually(UUID.randomUUID().toString.take(8))
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
          new STMPtr[String](id).read.map(x=>x)
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
          new STMPtr[String](id).read.map(x=>x)
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
          new STMPtr[String](id).read.map(x=>x)
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
          new STMPtr[String](id).read.map(x=>x)
        }
      }.txnRun(cluster), 30.seconds) mustBe "jill"
    }
  }

}

class LocalStmSpec extends StmSpecBase with BeforeAndAfterEach {
  override def beforeEach() {
    super.beforeEach()
    cluster.internal.asInstanceOf[RestmActors].clear()
  }

  val cluster = LocalRestmDb()
}

class LocalClusterStmSpec extends StmSpecBase with BeforeAndAfterEach {
  private val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))
  val shards = (0 until 8).map(_ => new RestmActors()).toList

  override def beforeEach() {
    super.beforeEach()
    shards.foreach(_.clear())
  }

  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8)))
}

class ServletStmSpec extends StmSpecBase with OneServerPerSuite {
  val cluster = new RestmHttpClient(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8)))
}


class ActorServletStmSpec extends StmSpecBase with OneServerPerSuite {
  private val newExeCtx: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))
  val cluster = new RestmImpl(new RestmInternalRestmHttpClient(s"http://localhost:$port")(newExeCtx))(newExeCtx)
}

