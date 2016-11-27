import org.scalatest.BeforeAndAfterEach
import storage.RestmActors
import storage.util.LocalRestmDb

class LocalClusterSpec extends ClusterSpecBase with BeforeAndAfterEach {

  override def beforeEach() {
    cluster.internal.asInstanceOf[RestmActors].clear()
  }

  val cluster = LocalRestmDb
}
