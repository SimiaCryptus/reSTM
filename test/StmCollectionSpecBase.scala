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
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import _root_.util.Util
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpec}
import stm.collection._
import stm.collection.clustering.{ClassificationTreeItem, LabeledItem, Page, PageTree}
import stm.task.TaskQueue
import storage.Restm._
import storage._
import storage.actors.{ActorLog, RestmActors}
import storage.remote.RestmCluster
import storage.types.JacksonValue

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

abstract class StmCollectionSpecBase extends WordSpec with BeforeAndAfterEach with MustMatchers {

  override def afterEach() {
    Util.clearMetrics()
  }
  override def beforeEach() {
    ActorLog.enabled = true
  }

  implicit def cluster: Restm

  val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))

  val itemCounts = List(1,100,1000)
  val threadCounts = List(1,8)


  s"PageTree via ${getClass.getSimpleName}" must {
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr).map(new LabeledItem(_, ClassificationTreeItem.empty))
    threadCounts.foreach(threads => {
      itemCounts.foreach(items => {
        s"support add and get with $items items and $threads threads" in {
          ActorLog.reset("BatchedTreeCollection")
          val bootstrapSize = 50
          try {
            val collection = new PageTree(new PointerType)
            val bootstrap = {
              implicit val _e = executionContext
              randomUUIDs.take(bootstrapSize).map(List(_)).flatMap(x ⇒ {
                collection.atomic().sync.add(Page(x))
                x
              }).toSet
            }
            val input = randomUUIDs.take(items).toSet.filterNot(bootstrap.contains)
            def insert() = {
              withPool(numThreads = threads) { executionContext ⇒ {
                implicit val _e = executionContext
                Await.result(Future.sequence(
                  input.map(List(_)).map(x ⇒ {
                    collection.atomic().add(Page(x))
                  })
                ), 30.seconds)
              }}
            }
            def verify(input: Set[LabeledItem], output: Set[LabeledItem]) = {
              output.filterNot(bootstrap.contains).size mustBe input.size
              output.filterNot(bootstrap.contains) mustBe input
            }
            val result = {
              implicit val _e = executionContext
              insert()
              verify(input, Stream.continually(collection.atomic().sync.get()).takeWhile(_.isDefined).flatMap(_.get.rows.map(_.asLabeledItem).toList).toSet)
              insert()
              verify(input, collection.atomic().stream().toSet)
            }
          } finally {
            println(JacksonValue.simple(Util.getMetrics).pretty)
          }
        }
      })
    })
  }

  s"DistributedScalar via ${getClass.getSimpleName}" must {
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr)
    threadCounts.foreach(threads => {
      itemCounts.foreach(items => {
        s"support add and get with $items items and $threads threads" in {
          ActorLog.reset("DistributedScalar")
          try {
            val collection = ScalarArray.createSync()(cluster)
            withPool(numThreads = threads) { executionContext ⇒ {
              implicit val _e = executionContext
              Await.result(Future.sequence(
                (1 to items).map(_ ⇒ {
                  collection.atomic().add(1.0)
                })
              ), 30.seconds)
            }}
            val result = {
              implicit val _e = executionContext
              collection.atomic().sync.get().toInt mustBe items
            }
          } finally {
            println(JacksonValue.simple(Util.getMetrics).pretty)
          }
        }
      })
    })
  }

  s"TreeSet via ${getClass.getSimpleName}" must {
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr)
    threadCounts.foreach(threads => {
      itemCounts.foreach(items => {
        s"support add and get with $items items and $threads threads" in {
          ActorLog.reset("TreeSet")
          val bootstrapSize = 10
          try {
            val collection = new TreeSet[String](new PointerType)
            val bootstrap: Set[String] = {
              implicit val _e = executionContext
              randomUUIDs.take(bootstrapSize).map(x ⇒ {
                collection.atomic.sync.add(x)
                x
              }).toSet
            }
            val input = randomUUIDs.take(items).toSet.filterNot(bootstrap.contains)
            withPool(numThreads = threads) { executionContext ⇒ {
              implicit val _e = executionContext
              Await.result(Future.sequence(
                input.map((item: String) ⇒ Future {
                  collection.atomic.sync.contains(item) mustBe false
                  collection.atomic.sync.add(item)
                  collection.atomic.sync.contains(item) mustBe true
                  collection.atomic.sync.remove(item)
                  collection.atomic.sync.contains(item) mustBe false
                })
              ), 30.seconds)
            }}
          } finally {
            println(JacksonValue.simple(Util.getMetrics).pretty)
          }
        }
      })
    })
  }

  s"TreeCollection via ${getClass.getSimpleName}" must {
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr)
    threadCounts.foreach(threads => {
      itemCounts.foreach(items => {
        s"support add and get with $items items and $threads threads" in {
          ActorLog.reset("TreeCollection")
          val bootstrapSize = 10
          try {
            val collection = new TreeCollection[String](new PointerType)
            val bootstrap: Set[String] = {
              implicit val _e = executionContext
              randomUUIDs.take(bootstrapSize).map(x ⇒ {
                collection.atomic().sync.add(x)
                x
              }).toSet
            }
            val input = randomUUIDs.take(items).toSet.filterNot(bootstrap.contains)
            withPool(numThreads = threads) { executionContext ⇒ {
              implicit val _e = executionContext
              Await.result(Future.sequence(
                input.map((item: String) ⇒ Future {
                  collection.atomic().sync.add(item)
                })
              ), 30.seconds)
            }}
            def verify(input: Set[String], output: Set[String]) = {
              output.filterNot(bootstrap.contains).size mustBe input.size
              output.filterNot(bootstrap.contains) mustBe input
            }
            val result = {
              implicit val _e = executionContext
              val continually: Stream[Option[String]] = Stream.continually(collection.atomic().sync.get())
              verify(input, continually.takeWhile(_.isDefined).map(_.get).toSet)
              //verify(input, collection.atomic().sync.toList().toSet)
            }
          } finally {
            println(JacksonValue.simple(Util.getMetrics).pretty)
          }
        }
      })
    })
  }

  s"LinkedList via ${getClass.getSimpleName}" must {
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr)
    threadCounts.foreach(threads => {
      itemCounts.foreach(items => {
        s"support add and get with $items items and $threads threads" in {
          ActorLog.reset("LinkedList")
          val bootstrapSize = 10
          try {
            val collection = LinkedList.static[String](new PointerType)
            val bootstrap: Set[String] = {
              implicit val _e = executionContext
              randomUUIDs.take(bootstrapSize).map(x ⇒ {
                collection.atomic().sync.add(x)
                x
              }).toSet
            }
            val input = randomUUIDs.take(items).distinct.toList.filterNot(bootstrap.contains)
            withPool(numThreads = threads) { executionContext ⇒ {
              implicit val _e = executionContext
              Await.result(Future.sequence(
                input.map((item: String) ⇒ Future {
                  collection.atomic().sync.add(item)
                })
              ), 30.seconds)
            }}
            def verify(input: List[String], output: List[String]) = {
              output.filterNot(bootstrap.contains).size mustBe input.size
              output.filterNot(bootstrap.contains) mustBe input
            }
            val result = {
              implicit val _e = executionContext
              verify(input, collection.atomic().sync.stream().toList)
            }
          } finally {
            println(JacksonValue.simple(Util.getMetrics).pretty)
          }
        }
      })
    })
  }


  s"TreeMap via ${getClass.getSimpleName}" must {
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr)
    threadCounts.foreach(threads => {
      itemCounts.foreach(items => {
        s"support add and get with $items items and $threads threads" in {
          ActorLog.reset("TreeMap")
          val bootstrapSize = 10
          try {
            val collection = new TreeMap[String,String](new PointerType)
            val bootstrap: Set[String] = {
              implicit val _e = executionContext
              randomUUIDs.take(bootstrapSize).map(x ⇒ {
                collection.atomic.sync.add(x, x.reverse)
                x
              }).toSet
            }
            val input = randomUUIDs.take(items).toSet.filterNot(bootstrap.contains)
            withPool(numThreads = threads) { executionContext ⇒ {
              implicit val _e = executionContext
              Await.result(Future.sequence(
                input.map((item: String) ⇒ Future {
                  collection.atomic.sync.contains(item) mustBe false
                  collection.atomic.sync.add(item, item.reverse)
                  collection.atomic.sync.contains(item) mustBe true
                  collection.atomic.sync.get(item) mustBe Option(item.reverse)
                  collection.atomic.sync.remove(item)
                  collection.atomic.sync.contains(item) mustBe false
                })
              ), 30.seconds)
            }}
          } finally {
            println(JacksonValue.simple(Util.getMetrics).pretty)
          }
        }
      })
    })
  }

  s"TaskQueue via ${getClass.getSimpleName}" must {
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr).map(TestObj)
    threadCounts.foreach(threads => {
      itemCounts.foreach(items => {
        s"support add and get with $items items and $threads threads" in {
          ActorLog.reset("TaskQueue")
          val bootstrapSize = 10
          try {
            val collection = TaskQueue.createSync[TestObj](8)
            val bootstrap: Set[TestObj] = {
              implicit val _e = executionContext
              randomUUIDs.take(bootstrapSize).map(x ⇒ {
                collection.atomic().sync.add(x)
                x
              }).toSet
            }
            val input = randomUUIDs.take(items).distinct.toList.filterNot(bootstrap.contains)
            withPool(numThreads = threads) { executionContext ⇒ {
              implicit val _e = executionContext
              Await.result(Future.sequence(
                input.map(item ⇒ Future {
                  collection.atomic().sync.contains(item.id) mustBe false
                  collection.atomic().sync.add(item)
                  collection.atomic().sync.contains(item.id) mustBe true
                })
              ), 30.seconds)
            }}
            def verify(input: List[TestObj], output: List[TestObj]) = {
              output.filterNot(bootstrap.contains).size mustBe input.size
              output.filterNot(bootstrap.contains).toSet mustBe input.toSet
            }
            val result = {
              implicit val _e = executionContext
              verify(input, collection.atomic().sync.stream().toList)
            }
          } finally {
            println(JacksonValue.simple(Util.getMetrics).pretty)
          }
        }
      })
    })
  }

  def withPool[T](numThreads : Int = 5)(fn: ExecutionContext⇒T) : T = {
    withPool(Executors.newFixedThreadPool(numThreads))(fn)
  }

  def withPool[T](poolDef : ⇒ExecutorService)(fn: ExecutionContext⇒T) : T = {
    val pool = poolDef
    val executionContext: ExecutionContext = ExecutionContext.fromExecutor(pool)
    try {
      fn(executionContext)
    } finally {
      pool.shutdownNow()
      pool.awaitTermination(10, TimeUnit.SECONDS)
    }
  }

}


class LocalStmCollectionSpec extends StmCollectionSpecBase with BeforeAndAfterEach {
  val cluster = LocalRestmDb()

  override def beforeEach() {
    super.beforeEach()
    cluster.internal.asInstanceOf[RestmActors].clear()
  }
}

class LocalClusterStmCollectionSpec extends StmCollectionSpecBase with BeforeAndAfterEach {
  val shards: List[RestmActors] = (0 until 8).map(_ => new RestmActors()).toList
  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))

  override def beforeEach() {
    super.beforeEach()
    shards.foreach(_.clear())
  }
}
