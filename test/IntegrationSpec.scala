import java.util.concurrent.Executors

import org.scalatestplus.play._
import storage.util.RestmProxy

import scala.concurrent.ExecutionContext

class IntegrationSpec extends ClusterSpecBase with OneServerPerTest {

  private val pool = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val cluster = new RestmProxy(s"http://localhost:$port")(pool)
}


