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

import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{MustMatchers, WordSpec}
import stm.collection.TreeSet
import storage.Restm._
import storage._
import storage.actors.RestmActors
import storage.cold.{BdbColdStorage, ColdStorage, DynamoColdStorage, HeapColdStorage}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class ColdStorageIntegrationSpec extends WordSpec with MustMatchers {
  implicit val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))

  "HeapColdStorage" should {
    "persist and restore data" in {
      implicit val coldStorage: ColdStorage = new HeapColdStorage
      val ids = randomUUIDs.take(5).toList
      addItems(ids)
      deleteItems(ids)
      addItems(ids)
    }
  }

  "BdbColdStorage" should {
    "persist and restore data" in {
      implicit val coldStorage: ColdStorage = new BdbColdStorage(path = "testDb", dbname = UUID.randomUUID().toString)
      val ids = randomUUIDs.take(5).toList
      addItems(ids)
      deleteItems(ids)
      addItems(ids)
    }
  }

  "DynamoLocalColdStorage" should {
    "persist and restore data" in {
      val credentials: AWSCredentials = new BasicAWSCredentials("ABC", "XYZ")
      implicit val coldStorage: ColdStorage = new DynamoColdStorage(
        tableName = "testDynamoLocalColdStorage",
        endpoint = "http://localhost:8000",
        awsCredentialsProvider = new AWSStaticCredentialsProvider(credentials))
      val ids = randomUUIDs.take(5).toList
      addItems(ids)
      Thread.sleep(5000) // DynamoDB has loose R/W consistency
      deleteItems(ids)
      Thread.sleep(5000) // DynamoDB has loose R/W consistency
      addItems(ids)
    }
  }
  val collection = new TreeSet[String](new PointerType)

  def randomUUIDs: Stream[String] = Stream.continually(UUID.randomUUID().toString.take(8))

  def addItems(items: List[String] = randomUUIDs.take(5).toList)(implicit coldStorage: ColdStorage, executor: ExecutionContext): List[String] = {
    val internal: RestmActors = new RestmActors(coldStorage)
    implicit val cluster = new RestmImpl(internal)
    for (item <- items) {
      collection.atomic.sync.contains(item) mustBe false
      collection.atomic.sync.add(item)
      collection.atomic.sync.contains(item) mustBe true
    }
    Await.result(internal.flushColdStorage(), 1.minutes)
    items
  }

  def deleteItems(items: List[String])(implicit coldStorage: ColdStorage, executor: ExecutionContext): List[String] = {
    val internal: RestmActors = new RestmActors(coldStorage)
    implicit val cluster = new RestmImpl(internal)
    for (item <- items) {
      collection.atomic.sync.contains(item) mustBe true
      collection.atomic.sync.remove(item) mustBe true
      collection.atomic.sync.contains(item) mustBe false
    }
    Await.result(internal.flushColdStorage(), 1.minutes)
    items
  }
}
