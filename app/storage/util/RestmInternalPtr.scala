package storage.util

import storage.Restm._
import storage.RestmInternal

import scala.concurrent.Future

trait RestmInternalPtr extends RestmInternal {
  def inner : RestmInternal

  override def _resetPtr(id: PointerType, time: TimeStamp): Future[Unit] =
    inner._resetPtr(id, time)

  override def _lock(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] =
    inner._lock(id, time)

  override def _commitPtr(id: PointerType, time: TimeStamp): Future[Unit] =
    inner._commitPtr(id, time)

  override def _getPtr(id: PointerType): Future[Option[ValueType]] =
    inner._getPtr(id)

  override def _init(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean] =
    inner._init(time, value, id)

  override def _getPtr(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] =
    inner._getPtr(id, time, ifModifiedSince)

  override def _addLock(id: PointerType, time: TimeStamp): Future[String] =
    inner._addLock(id, time)

  override def _reset(time: TimeStamp): Future[Set[PointerType]] =
    inner._reset(time)

  override def _commit(time: TimeStamp): Future[Set[PointerType]] =
    inner._commit(time)

  override def _txnState(time: TimeStamp): Future[String] =
    inner._txnState(time)

  override def queue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] =
    inner.queue(id, time, value)

}
