import org.scalatestplus.play._
import storage.util.RestmProxy

class IntegrationSpec extends ClusterSpecBase with OneServerPerTest {

  val cluster = new RestmProxy(s"http://localhost:$port")
}


