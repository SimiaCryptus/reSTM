import org.scalatest.{MustMatchers, WordSpec}
import storage.Restm
import storage.Restm._

import scala.concurrent.Await
import scala.concurrent.duration._

abstract class ClusterSpecBase extends WordSpec with MustMatchers {
  def cluster : Restm

  "Application" should {

    val ptrId = new PointerType
    "commit new data" in {
      val txnA = Await.result(cluster.newTxn(), 30.seconds)
      require(txnA > new TimeStamp(0))

      Await.result(cluster.getPtr(ptrId), 30.seconds) mustBe None
      Await.result(cluster.lock(ptrId, txnA), 30.seconds) mustBe None
      Await.result(cluster.queue(ptrId, txnA, new ValueType("foo")), 30.seconds)
      Await.result(cluster.commit(txnA), 30.seconds)

      val txnB = Await.result(cluster.newTxn(), 30.seconds)
      Await.result(cluster.getPtr(ptrId, txnB), 30.seconds) mustBe Some(new ValueType("foo"))
    }

    "revert data" in {
      val txnA = Await.result(cluster.newTxn(), 30.seconds)
      require(txnA > new TimeStamp(0))

      Await.result(cluster.getPtr(ptrId), 30.seconds) mustBe None
      Await.result(cluster.lock(ptrId, txnA), 30.seconds) mustBe None
      Await.result(cluster.queue(ptrId, txnA, new ValueType("foo")), 30.seconds)
      Await.result(cluster.reset(txnA), 30.seconds)

      val txnB = Await.result(cluster.newTxn(), 30.seconds)
      Await.result(cluster.getPtr(ptrId, txnB), 30.seconds) mustBe None
    }
  }
}
