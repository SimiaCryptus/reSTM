package storage

import storage.Restm._

import scala.concurrent.Future

trait RestmInternal {
  def _resetPtr(id: PointerType, time: TimeStamp): Future[Unit]

  def _lock(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]]

  def _commitPtr(id: PointerType, time: TimeStamp): Future[Unit]

  def _getPtr(id: PointerType): Future[Option[ValueType]]

  def _getPtr(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]]

  def _init(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean]

  def _addLock(id: PointerType, time: TimeStamp): Future[String]

  def _reset(time: TimeStamp): Future[Set[PointerType]]

  def _commit(time: TimeStamp): Future[Set[PointerType]]

  def _txnState(time: TimeStamp) : Future[String]

  def queue(id: PointerType, time: TimeStamp, value: ValueType) : Future[Unit]
}
