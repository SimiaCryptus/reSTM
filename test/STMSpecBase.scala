import java.util.UUID
import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import _root_.util.OperationMetrics
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm.{BinaryTreeNode, STMPtr, STMTxn, STMTxnCtx}
import storage.Restm._
import storage.util._
import storage.{RestmActors, _}

import scala.collection.parallel.immutable.ParSeq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Success


abstract class STMSpecBase extends WordSpec with MustMatchers {
  def cluster: Restm

  val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  "STM System" should {

    "basic writes" in {

      val ptr = STMPtr.static[String](new PointerType)

      Await.result(new STMTxn[Option[String]] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt()
        }
      }.txnRun(cluster)(executionContext), 30.seconds) mustBe None

      Await.result(new STMTxn[Unit] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.write("true")
        }
      }.txnRun(cluster)(executionContext), 30.seconds)

      Await.result(new STMTxn[String] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.read()
        }
      }.txnRun(cluster)(executionContext), 30.seconds) mustBe "true"

    }


    "simple binary tree" in {

      val ptr = STMPtr.static[BinaryTreeNode](new PointerType)

      Await.result(new STMTxn[Option[_]] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt()
        }
      }.txnRun(cluster)(executionContext), 30.seconds) mustBe None

      val items: List[String] = Stream.continually(UUID.randomUUID().toString.take(6)).take(20).toList

      for (item <- items) {
        insertAndVerify(ptr, item)
      }

      for (item <- items) {
        try {
          Await.result(new STMTxn[Boolean] {
            override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
              ptr.readOpt().map(_.map(_.contains(item)).getOrElse(false))
            }
          }.txnRun(cluster)(executionContext), 30.seconds) mustBe true
        } catch {
          case e =>
            Thread.sleep(1000)
            throw new RuntimeException(s"Error verifying $item",e)
        }
      }
    }

    "concurrent binary tree" in {

      val ptr = STMPtr.static[BinaryTreeNode](new PointerType)
      Await.result(new STMTxn[Option[_]] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt()
        }
      }.txnRun(cluster)(executionContext), 30.seconds) mustBe None

      // Bootstrap collection to reduce contention at root nodes via serial inserts
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        insertAndVerify(ptr, item)
      }

      implicit val exectx = executionContext
      val uuids = Stream.continually(UUID.randomUUID().toString.take(6)).take(100).par
      val checkNonexist: ParSeq[Future[String]] = verifyMissing(ptr, uuids)
      //checkNonexist.foreach(Await.result(_, 1.minute))
      val inserted: ParSeq[Future[String]] = insert(ptr, checkNonexist)
      Await.result(Future.sequence(inserted.toList), 10.minute)
      val verify: ParSeq[Future[String]] = verifyContains(ptr, inserted)
      Await.result(Future.sequence(verify.toList), 1.minute)

      OperationMetrics.metrics.map(e => e._1 + ": " + e._2.toString).foreach(System.out.println)
      OperationMetrics.metrics.clear()
    }

    "recover orphaned trasactions" in {

      val ptr = STMPtr.static[BinaryTreeNode](new PointerType)
      Await.result(new STMTxn[Option[_]] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt()
        }
      }.txnRun(cluster)(executionContext), 30.seconds) mustBe None

      // Bootstrap collection to reduce contention at root nodes via serial inserts
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        insertAndVerify(ptr, item)
      }

      // Insert collection and expire transactions (never commit nor rollback)
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        Await.result(new STMTxn[Unit] {
          override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
            ptr.readOpt().map(_.map(_ += item).getOrElse(new BinaryTreeNode(item))).flatMap(ptr.write)
          }
        }.testAbandoned().txnRun(cluster)(executionContext).recover({
          case e => //e.printStackTrace(System.out)
        })(executionContext), 30.seconds)
      }
      Thread.sleep(5000)

      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        insertAndVerify(ptr, item)
      }

      OperationMetrics.metrics.map(e => e._1 + ": " + e._2.toString).foreach(System.out.println)
      OperationMetrics.metrics.clear()
    }

    "recover orphaned pointers" in {

      val ptr = STMPtr.static[BinaryTreeNode](new PointerType)
      Await.result(new STMTxn[Option[_]] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt()
        }
      }.txnRun(cluster)(executionContext), 30.seconds) mustBe None

      // Bootstrap collection to reduce contention at root nodes via serial inserts
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        insertAndVerify(ptr, item)
      }

      // Insert collection and expire transactions (never commit nor rollback)
      RestmImpl.failChainedCalls = true
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(5).toList) {
        Await.result(new STMTxn[Unit] {
          override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
            ptr.readOpt().map(_.map(_ += item).getOrElse(new BinaryTreeNode(item))).flatMap(ptr.write)
          }
        }.txnRun(cluster)(executionContext).recover({
          case e => //e.printStackTrace(System.out)
        })(executionContext), 30.seconds)
      }
      RestmImpl.failChainedCalls = false
      Thread.sleep(5000)

      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        insertAndVerify(ptr, item)
      }

      OperationMetrics.metrics.map(e => e._1 + ": " + e._2.toString).foreach(System.out.println)
      OperationMetrics.metrics.clear()
    }
  }

  def insertAndVerify(ptr: STMPtr[BinaryTreeNode], item: String): Unit = try {
    Await.result(new STMTxn[Boolean] {
      override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
        ptr.readOpt().map(_.map(_.contains(item)).getOrElse(false))
      }
    }.txnRun(cluster)(executionContext), 30.seconds) mustBe false
    Await.result(new STMTxn[Unit] {
      override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
        ptr.readOpt().map(_.map(_ += item).getOrElse(new BinaryTreeNode(item))).flatMap(ptr.write)
      }
    }.txnRun(cluster)(executionContext), 30.seconds)
    Await.result(new STMTxn[Boolean] {
      override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
        ptr.readOpt().map(_.map(_.contains(item)).getOrElse(false))
      }
    }.txnRun(cluster)(executionContext), 30.seconds) mustBe true
  } catch {
    case e => throw new RuntimeException(s"Error processing $item",e)
  }

  def verifyMissing(ptr: STMPtr[BinaryTreeNode], uuids: ParSeq[String]): ParSeq[Future[String]] = {
    uuids.map(item => {
      new STMTxn[Boolean] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt().map(_.map(_.contains(item)).getOrElse(false))
        }
      }.txnRun(cluster)(executionContext).map(result => {
        result mustBe false;
        result
      })(executionContext).map(_ => item)(executionContext)
    })
  }

  def insert(ptr: STMPtr[BinaryTreeNode], checkNonexist: ParSeq[Future[String]]): ParSeq[Future[String]] = {
    checkNonexist.map(_.flatMap(item => {
      new STMTxn[Unit] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt().map(_.map(_ += item).getOrElse(new BinaryTreeNode(item))).flatMap(ptr.write)
        }
      }.txnRun(cluster)(executionContext).map(_ => item)(executionContext)
    })(executionContext))
  }

  def verifyContains(ptr: STMPtr[BinaryTreeNode], insert: ParSeq[Future[String]]): ParSeq[Future[String]] = {
    insert.map(_.flatMap(item => {
      val txn: STMTxn[Boolean] = new STMTxn[Boolean] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          val future: Future[Boolean] = ptr.readOpt().map(_.map(_.contains(item)).getOrElse(false))
          future.onComplete({
            case Success(false) => System.err.println(s"Does not contain $item")
            case _ =>
          })
          future
        }
      }
      txn.txnRun(cluster, priority = 1.seconds)(executionContext).map(result => {
        result mustBe true;
        item
      })(executionContext)
    })(executionContext))
  }
}

class LocalSTMSpec extends STMSpecBase with BeforeAndAfterEach {
  override def beforeEach() {
    cluster.internal.asInstanceOf[RestmActors].clear()
  }

  val cluster = LocalRestmDb
}

class LocalClusterSTMSpec extends STMSpecBase with BeforeAndAfterEach {
  private val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val shards = (0 until 8).map(_ => new RestmActors()(pool)).toList

  override def beforeEach() {
    shards.foreach(_.clear())
  }

  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}

class IntegrationSTMSpec extends STMSpecBase with OneServerPerTest {
  val cluster = new RestmProxy(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}

class IntegrationInteralSTMSpec extends STMSpecBase with OneServerPerTest {
  private val newExeCtx: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val cluster = new RestmImpl(new InternalRestmProxy(s"http://localhost:$port")(newExeCtx))(newExeCtx)
}



