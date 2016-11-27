package storage

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import storage.Restm._
import storage.actors._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

trait RestmActors extends RestmInternal {
  implicit val self = this
  private[this] implicit val executionContext = ExecutionContext.fromExecutor(new ThreadPoolExecutor(16,16,10,TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable]()))

  protected val txns: TrieMap[TimeStamp, TxnActor] = new scala.collection.concurrent.TrieMap[TimeStamp, TxnActor]()
  protected def getTxnActor(id: TimeStamp): TxnActor = txns.getOrElseUpdate(id, new TxnActor(id.toString))

  protected val ptrs: TrieMap[PointerType, PtrActor] = new scala.collection.concurrent.TrieMap[PointerType, PtrActor]()
  protected def getPtrActor(id: PointerType): PtrActor = ptrs.getOrElseUpdate(id, new PtrActor(id))

  def clear() = {
    ptrs.clear()
    txns.clear()
  }

  override def queue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] = {
    getPtrActor(id).writeBlob(time, value)
  }

  def _init(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean] = {
    getPtrActor(id).init(time, value)
  }

  override def _txnState(time: TimeStamp): Future[String] = {
    getTxnActor(time).getState()
  }

  def _resetPtr(id: PointerType, time: TimeStamp): Future[Unit] = {
    getPtrActor(id).writeReset(time)
  }

  def _lock(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] = {
    getPtrActor(id).writeLock(time)
  }

  def _commitPtr(id: PointerType, time: TimeStamp): Future[Unit] = {
    getPtrActor(id).writeCommit(time)
  }

  def _getPtr(id: PointerType): Future[Option[ValueType]] = {
    getPtrActor(id).getCurrentValue().map(_.map(_._2))
  }

  def _getPtr(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] = {
    getPtrActor(id).getValue(time, ifModifiedSince)
  }

  def _addLock(id: PointerType, time: TimeStamp): Future[String] = {
    getTxnActor(time).addLock(id)
  }

  def _reset(time: TimeStamp): Future[Set[PointerType]] = {
    getTxnActor(time).setState("RESET")
  }

  def _commit(time: TimeStamp): Future[Set[PointerType]] = {
    getTxnActor(time).setState("COMMIT")
  }
}
