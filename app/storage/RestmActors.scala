package storage

import storage.Restm._
import storage.actors._
import storage.util.ActorLog

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Success

class RestmActors(coldStorage : ColdStorage = new HeapColdStorage)(implicit executionContext: ExecutionContext) extends RestmInternal {

  protected val freezeQueue = new java.util.concurrent.LinkedBlockingDeque[AnyRef]()
  private val thread: Thread = {
    val thread: Thread = new Thread(new Runnable {
      override def run(): Unit = {
        while(!Thread.interrupted()) {
          for(item <- Stream.continually(freezeQueue.poll()).takeWhile(null != _)) item match {
            case id : PointerType =>
              val actor = getPtrActor(id)
              val recordsToUpload = actor.history.filter(_.coldStorageTs.isEmpty).toList
              if(!recordsToUpload.isEmpty) {
                coldStorage.store(id, recordsToUpload.map(record=>record.time->record.value).toMap)
                recordsToUpload.foreach(_.coldStorageTs = Option(System.currentTimeMillis()))
                ActorLog.log(s"$actor Persisted")
              }
            case p : Promise[Unit] => p.success(Unit)
          }
          Thread.sleep(10)
        }
      }
    })
    thread.setDaemon(true)
    thread.setName("RestmActors/ColdStorage")
    thread.start()
    thread
  }

  def flushColdStorage() : Future[Unit] = {
    val promise = Promise[Unit]()
    freezeQueue.add(promise)
    promise.future
  }

  protected val txns: TrieMap[TimeStamp, TxnActor] = new scala.collection.concurrent.TrieMap[TimeStamp, TxnActor]()

  protected def getTxnActor(id: TimeStamp): TxnActor = txns.getOrElseUpdate(id, new TxnActor(id.toString))

  protected val ptrs: TrieMap[PointerType, MemActor] = new scala.collection.concurrent.TrieMap[PointerType, MemActor]()

  protected def getPtrActor(id: PointerType): MemActor = ptrs.getOrElseUpdate(id, {
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
    } catch {
      case e : Throwable =>
        ActorLog.log(s"$obj Error restoring: $e")
        throw e
    }
    obj
  })

  def clear() = {
    ptrs.clear()
    txns.clear()
    coldStorage.clear()
  }

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] = {
    getPtrActor(id).writeBlob(time, value)
  }

  def _initValue(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean] = {
    getPtrActor(id).init(time, value).andThen({case Success(true) => freezeQueue.add(id)})
  }

  override def _txnState(time: TimeStamp): Future[String] = {
    getTxnActor(time).getState
  }

  def _resetValue(id: PointerType, time: TimeStamp): Future[Unit] = {
    getPtrActor(id).writeReset(time)
  }

  def _lockValue(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] = {
    getPtrActor(id).writeLock(time)
  }

  def _commitValue(id: PointerType, time: TimeStamp): Future[Unit] = {
    getPtrActor(id).writeCommit(time).andThen({case Success(_) => freezeQueue.add(id)})

  }

  def _getValue(id: PointerType): Future[Option[ValueType]] = {
    getPtrActor(id).getCurrentValue.map(_.map(_._2))
  }

  def _getValue(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] = {
    getPtrActor(id).getValue(time, ifModifiedSince)
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

  override def delete(id: PointerType, time: TimeStamp): Future[Unit] = getPtrActor(id).delete(time)
}
