import java.awt.Desktop
import java.net.URI
import java.util.concurrent.Executors

import org.scalatest.{MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm.concurrent.{StmDaemons, StmExecutionQueue}
import storage.remote.RestmHttpClient

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}


class ServletUtilitySpec extends WordSpec with MustMatchers with OneServerPerTest {
  implicit val cluster = new RestmHttpClient(s"http://localhost:$port")(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())


  "Single Node Servlet" should {
    "provide demo sort class" in {
      StmExecutionQueue.verbose = true
      //StmDaemons.start()
      Desktop.getDesktop.browse(new URI(s"http://localhost:$port/sys/init"))
      Thread.sleep(1000) // Allow platform to start

//      Desktop.getDesktop.browse(new URI(s"http://localhost:$port/demo/sort?n=10"))
      Desktop.getDesktop.browse(new URI(s"http://localhost:$port/sys/logs/"))

      //Thread.sleep(600.seconds.toMillis)
      //Desktop.getDesktop.browse(new URI(s"http://localhost:$port/sys/shutdown"))
      Await.result(StmDaemons.join(), 30.minutes)
      Thread.sleep(1000) // Allow rest of processes to complete
    }
  }


}
