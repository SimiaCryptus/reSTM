package storage

import java.util.concurrent.{Executors, TimeUnit}

import _root_.util.Util
import _root_.util.Util._
import storage.Restm._
import storage.actors._

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.Success

class RestmActors(coldStorage : ColdStorage = new HeapColdStorage) extends RestmInternal {
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

  protected var expireQueue = Executors.newScheduledThreadPool(1)
  protected val freezeQueue = new java.util.concurrent.LinkedBlockingDeque[AnyRef]()
  private val freezeThread: Thread = {
    val thread: Thread = new Thread(new Runnable {
      override def run(): Unit = {
        while(!Thread.interrupted()) {
          for(item <- Stream.continually(freezeQueue.poll()).takeWhile(null != _)) monitorBlock("RestmActors.dequeueStorage") {
            item match {
              case id : PointerType =>
                val task = getPtrActor(id).map(actor=>{
                  val prevTxns: Set[TimeStamp] = actor.history.map(_.time).toArray.toSet
                  val recordsToUpload = actor.history.filter(_.coldStorageTs.isEmpty).toList
                  // TODO: Seems to be a problem enabling this
                  if(!recordsToUpload.isEmpty && false) monitorBlock("Restm.coldStorage.store") {
                    coldStorage.store(id, recordsToUpload.map(record=>record.time->record.value).toMap)
                    recordsToUpload.foreach(_.coldStorageTs = Option(System.currentTimeMillis()))
                    ActorLog.log(s"$actor Persisted")
                    expireQueue.schedule(new Runnable {
                      override def run(): Unit = {
                        actor.withActor {
                          if(!actor.history.map(_.time).exists(!prevTxns.contains(_))) {
                            ActorLog.log(s"Removed $id from active memory")
                            ptrs2.remove(id)
                            ptrs1.remove(id)
                            actor.close()
                            Util.delta("RestmActors.activeMemActors", -1.0)
                          }
                        }
                      }
                    }, 5, TimeUnit.MINUTES)
                }})
                Await.result(task, 10.second)
              case p : Promise[Unit] => p.success(Unit)
            }
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

  def flushColdStorage() : Future[Unit] = {
    val promise = Promise[Unit]()
    queueStorage(promise)
    promise.future
  }

  protected val txns: TrieMap[TimeStamp, TxnActor] = new scala.collection.concurrent.TrieMap[TimeStamp, TxnActor]()

  protected def getTxnActor(id: TimeStamp): TxnActor = monitorBlock("Restm.getTxn") {
    txns.getOrElseUpdate(id, monitorBlock("Restm.newTxn"){ new TxnActor(id.toString) })
  }

  protected val ptrs1: TrieMap[PointerType, Future[MemActor]] = new scala.collection.concurrent.TrieMap[PointerType, Future[MemActor]]()
  protected val ptrs2: TrieMap[PointerType, Future[MemActor]] = new scala.collection.concurrent.TrieMap[PointerType, Future[MemActor]]()

  protected def getPtrActor(id: PointerType): Future[MemActor] = monitorBlock("Restm.getPtr") { ptrs1.getOrElseUpdate(id,
    ptrs2.synchronized {
      ptrs2.getOrElseUpdate(id,
        Future { monitorBlock("Restm.newPtr") {
          val obj = new MemActor(id)
          try {
            val restored: Map[TimeStamp, ValueType] = coldStorage.read(id)
            obj.history ++= restored.map(e=>{
              val record = new HistoryRecord(e._1, e._2)
              record.coldStorageTs = Option(System.currentTimeMillis())
              record
            })
            if(restored.isEmpty) {
              ActorLog.log(s"$obj Initialized new value")
            } else {
              ActorLog.log(s"$obj Initialized with ${restored.size} items restored")
            }
            Util.delta("RestmActors.activeMemActors", 1.0)
          } catch {
            case e : Throwable =>
              ActorLog.log(s"$obj Error restoring: $e")
              throw e
          }
          obj
        } }
      )
    }
  )}

  def clear() = {
    expireQueue.shutdownNow()
    expireQueue = Executors.newScheduledThreadPool(1)
    ptrs2.clear()
    ptrs1.clear()
    txns.clear()
    coldStorage.clear()
  }

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] = {
    getPtrActor(id).flatMap(_.writeBlob(time, value))
  }

  def _initValue(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean] = {
    getPtrActor(id).flatMap(_.init(time, value)).andThen({case Success(true) => queueStorage(id)})
  }

  def queueStorage(id: AnyRef): Boolean = monitorBlock("RestmActors.queueStorage") {
    freezeQueue.add(id)
  }

  override def _txnState(time: TimeStamp): Future[String] = {
    getTxnActor(time).getState
  }

  def _resetValue(id: PointerType, time: TimeStamp): Future[Unit] = {
    getPtrActor(id).flatMap(_.writeReset(time))
  }

  def _lockValue(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] = {
    getPtrActor(id).flatMap(_.writeLock(time))
  }

  def _commitValue(id: PointerType, time: TimeStamp): Future[Unit] = {
    getPtrActor(id).flatMap(_.writeCommit(time)).andThen({case Success(_) => queueStorage(id)})

  }

  def _getValue(id: PointerType): Future[Option[ValueType]] = {
    getPtrActor(id).flatMap(_.getCurrentValue.map(_.map(_._2)))
  }

  def _getValue(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] = {
    getPtrActor(id).flatMap(_.getValue(time, ifModifiedSince))
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

  override def delete(id: PointerType, time: TimeStamp): Future[Unit] = getPtrActor(id).flatMap(_.delete(time))
}
