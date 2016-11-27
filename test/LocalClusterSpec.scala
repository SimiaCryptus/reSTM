import org.scalatest.BeforeAndAfterEach
import storage.util.LocalRestmDb

class LocalClusterSpec extends ClusterSpecBase with BeforeAndAfterEach {

  override def beforeEach() {
    cluster.clear()
  }

  val cluster = LocalRestmDb
}
