import java.util.UUID

import _root_.util.AOP
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm.{BinaryTreeNode, STMPtr, STMTxn, STMTxnCtx}
import storage.Restm._
import storage.util._
import storage.{RestmActors, _}

import scala.collection.parallel.immutable.ParSeq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Success


abstract class STMSpecBase extends WordSpec with MustMatchers {
  def cluster : Restm
  import ExecutionContext.Implicits.global

  "STM System" should {

    "basic writes" in {

      val ptr = STMPtr.static[String](new PointerType)

      Await.result(new STMTxn[Option[String]] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt()
        }
      }.txnRun(cluster), 30.seconds) mustBe None

      Await.result(new STMTxn[Unit] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.write("true")
        }
      }.txnRun(cluster), 30.seconds)

      Await.result(new STMTxn[String] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.read()
        }
      }.txnRun(cluster), 30.seconds) mustBe "true"

    }


    "simple binary tree" in {

      val ptr = STMPtr.static[BinaryTreeNode](new PointerType)

      Await.result(new STMTxn[Option[_]] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt()
        }
      }.txnRun(cluster), 30.seconds) mustBe None

      val items: List[String] = Stream.continually(UUID.randomUUID().toString.take(6)).take(20).toList

      for (item <- items) {
        insertAndVerify(ptr, item)
      }

      for (item <- items) {
        Await.result(new STMTxn[Boolean] {
          override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
            ptr.readOpt().map(_.map(_.contains(item)).getOrElse(false))
          }
        }.txnRun(cluster), 30.seconds) mustBe true
      }
    }

    "concurrent binary tree" in {

      val ptr = STMPtr.static[BinaryTreeNode](new PointerType)
      Await.result(new STMTxn[Option[_]] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt()
        }
      }.txnRun(cluster), 30.seconds) mustBe None

      // Bootstrap collection to reduce contention at root nodes via serial inserts
      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        insertAndVerify(ptr, item)
      }

      val uuids = Stream.continually(UUID.randomUUID().toString.take(6)).take(100).par
      val checkNonexist: ParSeq[Future[String]] = verifyMissing(ptr, uuids)
      //checkNonexist.foreach(Await.result(_, 1.minute))
      val inserted: ParSeq[Future[String]] = insert(ptr, checkNonexist)
      Await.result(Future.sequence(inserted.toList), 10.minute)
      val verify: ParSeq[Future[String]] = verifyContains(ptr, inserted)
      Await.result(Future.sequence(verify.toList), 1.minute)

      AOP.metrics.map(e=>e._1 + ": " + e._2.toString).foreach(System.out.println)
      AOP.metrics.clear()
    }

    "recover orphaned trasactions" in {

      val ptr = STMPtr.static[BinaryTreeNode](new PointerType)
      Await.result(new STMTxn[Option[_]] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt()
        }
      }.txnRun(cluster), 30.seconds) mustBe None

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
        }.testAbandoned().txnRun(cluster).recover({
          case e => //e.printStackTrace(System.out)
        }), 30.seconds)
      }
      Thread.sleep(5000)

      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        insertAndVerify(ptr, item)
      }

      AOP.metrics.map(e=>e._1 + ": " + e._2.toString).foreach(System.out.println)
      AOP.metrics.clear()
    }

    "recover orphaned pointers" in {

      val ptr = STMPtr.static[BinaryTreeNode](new PointerType)
      Await.result(new STMTxn[Option[_]] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt()
        }
      }.txnRun(cluster), 30.seconds) mustBe None

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
        }.txnRun(cluster).recover({
          case e => e.printStackTrace(System.out)
        }), 30.seconds)
      }
      RestmImpl.failChainedCalls = false
      Thread.sleep(5000)

      for (item <- Stream.continually(UUID.randomUUID().toString.take(6)).take(10).toList) {
        insertAndVerify(ptr, item)
      }

      AOP.metrics.map(e=>e._1 + ": " + e._2.toString).foreach(System.out.println)
      AOP.metrics.clear()
    }
  }

  def insertAndVerify(ptr : STMPtr[BinaryTreeNode], item: String): Unit = {
    Await.result(new STMTxn[Boolean] {
      override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
        ptr.readOpt().map(_.map(_.contains(item)).getOrElse(false))
      }
    }.txnRun(cluster), 30.seconds) mustBe false
    Await.result(new STMTxn[Unit] {
      override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
        ptr.readOpt().map(_.map(_ += item).getOrElse(new BinaryTreeNode(item))).flatMap(ptr.write)
      }
    }.txnRun(cluster), 30.seconds)
    Await.result(new STMTxn[Boolean] {
      override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
        ptr.readOpt().map(_.map(_.contains(item)).getOrElse(false))
      }
    }.txnRun(cluster), 30.seconds) mustBe true
  }

  def verifyMissing(ptr : STMPtr[BinaryTreeNode], uuids: ParSeq[String]): ParSeq[Future[String]] = {
    uuids.map(item => {
      new STMTxn[Boolean] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt().map(_.map(_.contains(item)).getOrElse(false))
        }
      }.txnRun(cluster).map(result => {
        result mustBe false;
        result
      }).map(_ => item)
    })
  }

  def insert(ptr : STMPtr[BinaryTreeNode], checkNonexist: ParSeq[Future[String]]): ParSeq[Future[String]] = {
    checkNonexist.map(_.flatMap(item => {
      new STMTxn[Unit] {
        override protected def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          ptr.readOpt().map(_.map(_ += item).getOrElse(new BinaryTreeNode(item))).flatMap(ptr.write)
        }
      }.txnRun(cluster).map(_ => item)
    }))
  }

  def verifyContains(ptr : STMPtr[BinaryTreeNode], insert: ParSeq[Future[String]]): ParSeq[Future[String]] = {
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
      txn.txnRun(cluster, priority = 1000).map(result => {
        result mustBe true;
        item
      })
    }))
  }
}

class LocalSTMSpec extends STMSpecBase with BeforeAndAfterEach {
  override def beforeEach() {
    cluster.clear()
  }
  val cluster = LocalRestmDb
}

class LocalClusterSTMSpec extends STMSpecBase with BeforeAndAfterEach {
  val shards = (0 until 8).map(_=>new RestmActors(){}).toList
  override def beforeEach() {
    shards.foreach(_.clear())
  }
  val cluster = new RestmCluster(shards)
}

class IntegrationSTMSpec extends STMSpecBase with OneServerPerTest {
  val cluster = new RestmProxy(s"http://localhost:$port")
}

class IntegrationInteralSTMSpec extends STMSpecBase with OneServerPerTest {
  val cluster = new InternalRestmProxy(s"http://localhost:$port")
}



