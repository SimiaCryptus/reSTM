package storage

import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import storage.Restm._
import storage.actors._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class RestmActors(implicit executionContext: ExecutionContext) extends RestmInternal {
  implicit val self = this

  protected val txns: TrieMap[TimeStamp, TxnActor] = new scala.collection.concurrent.TrieMap[TimeStamp, TxnActor]()

  protected def getTxnActor(id: TimeStamp): TxnActor = txns.getOrElseUpdate(id, new TxnActor(id.toString))

  protected val ptrs: TrieMap[PointerType, MemActor] = new scala.collection.concurrent.TrieMap[PointerType, MemActor]()

  protected def getPtrActor(id: PointerType): MemActor = ptrs.getOrElseUpdate(id, new MemActor(id))

  def clear() = {
    ptrs.clear()
    txns.clear()
  }

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] = {
    getPtrActor(id).writeBlob(time, value)
  }

  def _initValue(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean] = {
    getPtrActor(id).init(time, value)
  }

  override def _txnState(time: TimeStamp): Future[String] = {
    getTxnActor(time).getState()
  }

  def _resetValue(id: PointerType, time: TimeStamp): Future[Unit] = {
    getPtrActor(id).writeReset(time)
  }

  def _lockValue(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] = {
    getPtrActor(id).writeLock(time)
  }

  def _commitValue(id: PointerType, time: TimeStamp): Future[Unit] = {
    getPtrActor(id).writeCommit(time)
  }

  def _getValue(id: PointerType): Future[Option[ValueType]] = {
    getPtrActor(id).getCurrentValue().map(_.map(_._2))
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
}
