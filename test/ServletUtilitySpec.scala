import java.util.concurrent.Executors

import org.scalatest.{MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import storage.util._

import scala.concurrent.ExecutionContext


class ServletUtilitySpec extends WordSpec with MustMatchers with OneServerPerTest {
  implicit val cluster = new RestmProxy(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())


  "Single Node Servlet" should {
//    "provide demo sort class" in {
//      StmExecutionQueue.verbose = true
//      Desktop.getDesktop.browse(new URI(s"http://localhost:$port/sys/init"))
//      Desktop.getDesktop.browse(new URI(s"http://localhost:$port/demo/sort?n=10"))
//      //Thread.sleep(600.seconds.toMillis)
//      //Desktop.getDesktop.browse(new URI(s"http://localhost:$port/sys/shutdown"))
//      Await.result(StmDaemons.join(), 30.minutes)
//      Thread.sleep(1000) // Allow rest of processes to complete
//    }
  }


}
