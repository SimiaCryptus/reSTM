package storage

import storage.Restm._

import scala.concurrent.Future

trait RestmInternal {
  def _resetValue(id: PointerType, time: TimeStamp): Future[Unit]

  def _lockValue(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]]

  def _commitValue(id: PointerType, time: TimeStamp): Future[Unit]

  def _getValue(id: PointerType): Future[Option[ValueType]]

  def _getValue(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]]

  def _initValue(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean]

  def _addLock(id: PointerType, time: TimeStamp): Future[String]

  def _resetTxn(time: TimeStamp): Future[Set[PointerType]]

  def _commitTxn(time: TimeStamp): Future[Set[PointerType]]

  def _txnState(time: TimeStamp): Future[String]

  def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit]
}
