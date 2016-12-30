import java.util.concurrent.Executors

import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import storage.Restm
import storage.Restm._
import storage.actors.RestmActors
import storage.remote.RestmHttpClient

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

abstract class RestmSpecBase extends WordSpec with MustMatchers {
  def cluster: Restm

  "reSTM Storage Layer" should {

    "commit new data" in {
      val ptrId = new PointerType
      val txnA = Await.result(cluster.newTxn(), 30.seconds)
      require(txnA > new TimeStamp(0))

      Await.result(cluster.getPtr(ptrId), 30.seconds) mustBe None
      Await.result(cluster.lock(ptrId, txnA), 30.seconds) mustBe None
      Await.result(cluster.queueValue(ptrId, txnA, new ValueType("foo")), 30.seconds)
      Await.result(cluster.commit(txnA), 30.seconds)

      val txnB = Await.result(cluster.newTxn(), 30.seconds)
      Await.result(cluster.getPtr(ptrId, txnB), 30.seconds) mustBe Some(new ValueType("foo"))
    }

    "revert data" in {
      val ptrId = new PointerType
      val txnA = Await.result(cluster.newTxn(), 30.seconds)
      require(txnA > new TimeStamp(0))

      Await.result(cluster.getPtr(ptrId), 30.seconds) mustBe None
      Await.result(cluster.lock(ptrId, txnA), 30.seconds) mustBe None
      Await.result(cluster.queueValue(ptrId, txnA, new ValueType("foo")), 30.seconds)
      Await.result(cluster.reset(txnA), 30.seconds)

      val txnB = Await.result(cluster.newTxn(), 30.seconds)
      Await.result(cluster.getPtr(ptrId, txnB), 30.seconds) mustBe None
    }
  }
}
class LocalRestmSpec extends RestmSpecBase with BeforeAndAfterEach {

  override def beforeEach() {
    cluster.internal.asInstanceOf[RestmActors].clear()
  }

  val cluster = LocalRestmDb()
}
class IntegrationSpec extends RestmSpecBase with OneServerPerTest {

  private val pool = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))
  val cluster = new RestmHttpClient(s"http://localhost:$port")(pool)
}


