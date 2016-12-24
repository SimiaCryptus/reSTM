import java.util.UUID
import java.util.concurrent.Executors

import org.scalatest.{MustMatchers, WordSpec}
import stm.collection.TreeSet
import storage.Restm._
import storage.remote.RestmInternalStaticListReplicator
import storage.{RestmActors, _}

import scala.concurrent.ExecutionContext

class STMReplicationSpec extends WordSpec with MustMatchers {
  implicit val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  "RestmInternalStaticListReplicator" should {
    "persist and restore data" in {
      val nodes = (0 to 1).map(_=>new RestmActors()).toList
      val ids = randomUUIDs.take(5).toList
      addItems(nodes, ids)
      deleteItems(nodes, ids)
    }
    "tolerate node failure" in {
      val nodes = (0 to 1).map(_=>new RestmActors()).toList
      val ids = randomUUIDs.take(5).toList
      addItems(nodes, ids)
      nodes(0).clear()
      deleteItems(nodes, ids)
    }
  }

  def randomUUIDs: Stream[String] = Stream.continually(UUID.randomUUID().toString.take(8))
  val collection = new TreeSet[String](new PointerType("test/ColdStorageIntegrationSpec/" + UUID.randomUUID().toString))

  def addItems(nodes : Seq[RestmInternal], items : Seq[String] = randomUUIDs.take(5).toList)(implicit executor: ExecutionContext) = {
    implicit val cluster = new RestmImpl(new RestmInternalStaticListReplicator(nodes))
    for (item <- items) {
      try {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
      } catch {
        case e : Throwable => throw new RuntimeException(s"Error adding $item", e)
      }
    }
    items
  }

  def deleteItems(nodes : Seq[RestmInternal], items:Seq[String])(implicit executor: ExecutionContext) = {
    implicit val cluster = new RestmImpl(new RestmInternalStaticListReplicator(nodes))
    for (item <- items) {
      try {
        collection.atomic.sync.contains(item) mustBe true
        collection.atomic.sync.remove(item)
        collection.atomic.sync.contains(item) mustBe false
      } catch {
        case e : Throwable => throw new RuntimeException(s"Error deleting $item", e)
      }
    }
    items
  }
}
