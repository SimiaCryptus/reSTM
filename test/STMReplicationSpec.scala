/*
 * Copyright (c) 2017 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.UUID
import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{MustMatchers, WordSpec}
import stm.collection.TreeSet
import storage.Restm._
import storage._
import storage.actors.RestmActors
import storage.remote.RestmInternalStaticListReplicator

import scala.concurrent.ExecutionContext

class STMReplicationSpec extends WordSpec with MustMatchers {
  implicit val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))

  "RestmInternalStaticListReplicator" should {
    "persist and restore data" in {
      val nodes = (0 to 1).map(_ => new RestmActors()).toList
      val ids = randomUUIDs.take(5).toList
      addItems(nodes, ids)
      deleteItems(nodes, ids)
    }
    "tolerate node failure" in {
      val nodes = (0 to 1).map(_ => new RestmActors()).toList
      val ids = randomUUIDs.take(5).toList
      addItems(nodes, ids)
      nodes.head.clear()
      deleteItems(nodes, ids)
    }
  }
  val collection = new TreeSet[String](new PointerType("test/ColdStorageIntegrationSpec/" + UUID.randomUUID().toString))

  def randomUUIDs: Stream[String] = Stream.continually(UUID.randomUUID().toString.take(8))

  def addItems(nodes: Seq[RestmInternal], items: Seq[String] = randomUUIDs.take(5).toList)(implicit executor: ExecutionContext): Seq[String] = {
    implicit val cluster = new RestmImpl {
      override val internal: RestmInternal = new RestmInternalStaticListReplicator(nodes)
    }
    for (item <- items) {
      try {
        collection.atomic.sync.contains(item) mustBe false
        collection.atomic.sync.add(item)
        collection.atomic.sync.contains(item) mustBe true
      } catch {
        case e: Throwable => throw new RuntimeException(s"Error adding $item", e)
      }
    }
    items
  }

  def deleteItems(nodes: Seq[RestmInternal], items: Seq[String])(implicit executor: ExecutionContext): Seq[String] = {
    implicit val cluster = new RestmImpl {
      override val internal: RestmInternal = new RestmInternalStaticListReplicator(nodes)
    }
    for (item <- items) {
      try {
        collection.atomic.sync.contains(item) mustBe true
        collection.atomic.sync.remove(item)
        collection.atomic.sync.contains(item) mustBe false
      } catch {
        case e: Throwable => throw new RuntimeException(s"Error deleting $item", e)
      }
    }
    items
  }
}
