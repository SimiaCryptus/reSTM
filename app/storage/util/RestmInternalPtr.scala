package storage.util

import storage.Restm._
import storage.RestmInternal

import scala.concurrent.Future

trait RestmInternalPtr extends RestmInternal {
  def inner: RestmInternal

  override def _resetValue(id: PointerType, time: TimeStamp): Future[Unit] =
    inner._resetValue(id, time)

  override def _lockValue(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] =
    inner._lockValue(id, time)

  override def _commitValue(id: PointerType, time: TimeStamp): Future[Unit] =
    inner._commitValue(id, time)

  override def _getValue(id: PointerType): Future[Option[ValueType]] =
    inner._getValue(id)

  override def _initValue(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean] =
    inner._initValue(time, value, id)

  override def _getValue(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] =
    inner._getValue(id, time, ifModifiedSince)

  override def _addLock(id: PointerType, time: TimeStamp): Future[String] =
    inner._addLock(id, time)

  override def _resetTxn(time: TimeStamp): Future[Set[PointerType]] =
    inner._resetTxn(time)

  override def _commitTxn(time: TimeStamp): Future[Set[PointerType]] =
    inner._commitTxn(time)

  override def _txnState(time: TimeStamp): Future[String] =
    inner._txnState(time)

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] =
    inner.queueValue(id, time, value)

}
