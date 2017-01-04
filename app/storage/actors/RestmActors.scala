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

package storage.actors

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import _root_.util.Util
import _root_.util.Util._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import storage.Restm._
import storage.RestmInternal
import storage.cold.{ColdStorage, HeapColdStorage}

import scala.collection.concurrent.TrieMap
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Success

object RestmActors {
  var IDLE_PTR_TIME = 600
}

class RestmActors(coldStorage: ColdStorage = new HeapColdStorage) extends RestmInternal {
  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(
    // Executors.newCachedThreadPool(
    Executors.newFixedThreadPool(8,
      new ThreadFactoryBuilder().setNameFormat("storage-actor-pool-%d").build()))
  val freezeThread: Thread = {
    val thread: Thread = new Thread(new Runnable {
      override def run(): Unit = {
        while (!Thread.interrupted()) {
          try {
            for (item <- Stream.continually(freezeQueue.poll()).takeWhile(null != _)) monitorBlock("RestmActors.dequeueStorage") {
              item match {
                case id: PointerType =>
                  val task = getPtrActor(id, None).map(actor => {
                    actor.collectGarbage()
                    val recordsToUpload = actor.history.filter(_.coldStorageTs.isEmpty).toList
                    if (recordsToUpload.nonEmpty) monitorBlock("Restm.coldStorage.store") {
                      coldStorage.store(id, recordsToUpload.map(record => record.time -> record.value).toMap)
                      recordsToUpload.foreach(_.coldStorageTs = Option(System.currentTimeMillis()))
                      ActorLog.log(s"$actor Persisted")

                      expireQueue.schedule(new Runnable {
                        var prevTxns: Set[TimeStamp] = actor.history.filter(_!=null).map(_.time).toArray.toSet
                        var lastRead: TimeStamp = actor.lastRead.get
                        override def run(): Unit = {
                          actor.withActor {
                            def hasNewCommit = actor.history.map(_.time).exists(!prevTxns.contains(_))
                            def isWriteLocked = actor.writeLock.isDefined
                            def isReadActive = actor.lastRead.exists(_ > lastRead)
                            if (!hasNewCommit) {
                              if(isReadActive) {
                                lastRead = actor.lastRead.get
                                expireQueue.schedule(this, RestmActors.IDLE_PTR_TIME, TimeUnit.SECONDS)
                              } else if (!isWriteLocked) {
                                ActorLog.log(s"Removed $id from active memory")
                                ptrs2.remove(id)
                                ptrs1.remove(id)
                                actor.close()
                                Util.delta("RestmActors.activeMemActors", -1.0)
                              }
                            }
                          }
                        }
                      }, RestmActors.IDLE_PTR_TIME, TimeUnit.SECONDS)
                    }
                  })
                  Await.result(task, 10.second)
                case p: Promise[_] => p.asInstanceOf[Promise[Unit]].success(Unit)
              }
            }
          } catch {
            case e: Throwable => e.printStackTrace()
          }
          Thread.sleep(10)
        }
      }
    })
    thread.setDaemon(true)
    thread.setName("RestmActors.freezeThread")
    thread.start()
    thread
  }
  protected val freezeQueue = new java.util.concurrent.LinkedBlockingDeque[AnyRef]()
  protected val txns: TrieMap[TimeStamp, TxnActor] = new scala.collection.concurrent.TrieMap[TimeStamp, TxnActor]()
  protected val ptrs1: TrieMap[PointerType, Future[MemActor]] = new scala.collection.concurrent.TrieMap[PointerType, Future[MemActor]]()
  protected val ptrs2: TrieMap[PointerType, Future[MemActor]] = new scala.collection.concurrent.TrieMap[PointerType, Future[MemActor]]()
  protected var expireQueue: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  def flushColdStorage(): Future[Unit] = {
    val promise = Promise[Unit]()
    queueStorage(promise)
    promise.future
  }

  def clear(): Unit = {
    freezeQueue.clear()
    expireQueue.shutdownNow()
    expireQueue.awaitTermination(10, TimeUnit.SECONDS)
    expireQueue = Executors.newScheduledThreadPool(1)
    ptrs2.clear()
    ptrs1.clear()
    txns.clear()
    coldStorage.clear()
  }

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] = {
    getPtrActor(id, Option(time)).flatMap(_.writeBlob(time, value))
  }

  def _initValue(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean] = {
    getPtrActor(id, Option(time)).flatMap(_.init(time, value)).andThen({ case Success(true) => queueStorage(id) })
  }

  override def _txnState(time: TimeStamp): Future[String] = {
    getTxnActor(time).getState
  }

  def _resetValue(id: PointerType, time: TimeStamp): Future[Unit] = {
    getPtrActor(id, Option(time)).flatMap(_.writeReset(time))
  }

  def _lockValue(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] = {
    getPtrActor(id, Option(time)).flatMap(_.writeLock(time))
  }

  def _commitValue(id: PointerType, time: TimeStamp): Future[Unit] = {
    getPtrActor(id, Option(time)).flatMap(_.writeCommit(time)).andThen({ case Success(_) => queueStorage(id) })

  }

  protected def getPtrActor(id: PointerType, time: Option[TimeStamp]): Future[MemActor] = monitorBlock("Restm.getPtr") {
    ptrs1.getOrElseUpdate(id,
      ptrs2.synchronized {
        ptrs2.getOrElseUpdate(id,
          Future {
            monitorBlock("Restm.newPtr") {
              val obj = new MemActor(id, time)
              try {
                val restored: Map[TimeStamp, ValueType] = coldStorage.read(id)
                obj.history ++= restored.map(e => {
                  val record = new HistoryRecord(e._1, e._2)
                  record.coldStorageTs = Option(System.currentTimeMillis())
                  record
                }).toList.sortBy(_.time)
                if (restored.isEmpty) {
                  ActorLog.log(s"$obj Initialized new value")
                } else {
                  ActorLog.log(s"$obj Initialized with ${restored.size} items restored")
                }
                Util.delta("RestmActors.activeMemActors", 1.0)
              } catch {
                case e: Throwable =>
                  ActorLog.log(s"$obj Error restoring: $e")
                  throw e
              }
              obj
            }
          }
        )
      }
    )
  }

  def queueStorage(id: AnyRef): Boolean = monitorBlock("RestmActors.queueStorage") {
    freezeQueue.add(id)
  }

  def _getValue(id: PointerType): Future[Option[ValueType]] = {
    getPtrActor(id, None).flatMap(_.getCurrentValue.map(_.map(_._2)))
  }

  def _getValue(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] = {
    getPtrActor(id, Option(time)).flatMap(_.getValue(time, ifModifiedSince))
  }

  def _addLock(id: PointerType, time: TimeStamp): Future[String] = {
    getTxnActor(time).addLock(id)
  }

  def _resetTxn(time: TimeStamp): Future[Set[PointerType]] = {
    getTxnActor(time).setState("RESET")
  }

  def _commitTxn(time: TimeStamp): Future[Set[PointerType]] = {
    getTxnActor(time).setState("COMMIT")
  }

  protected def getTxnActor(id: TimeStamp): TxnActor = monitorBlock("Restm.getTxn") {
    txns.getOrElseUpdate(id, monitorBlock("Restm.newTxn") {
      new TxnActor(id.toString)
    })
  }

  override def delete(id: PointerType, time: TimeStamp): Future[Unit] = getPtrActor(id, Option(time)).flatMap(_.delete(time))
}
