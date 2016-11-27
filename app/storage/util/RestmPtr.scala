package storage.util

import storage.Restm._
import storage.{LockedException, Restm}

import scala.concurrent.Future

trait RestmPtr extends Restm {

  def inner : Restm

  override def newPtr(version: TimeStamp, value: ValueType): Future[PointerType] =
    inner.newPtr(version,value)

  @throws[LockedException]
  override def getPtr(id: PointerType): Future[Option[ValueType]] =
    inner.getPtr(id)

  @throws[LockedException]
  override def getPtr(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] =
    inner.getPtr(id, time, ifModifiedSince)

  override def newTxn(priority: Int): Future[TimeStamp] =
    inner.newTxn(priority)

  override def lock(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] =
    inner.lock(id, time)

  override def commit(time: TimeStamp): Future[Unit] =
    inner.commit(time)

  override def reset(time: TimeStamp): Future[Unit] =
    inner.reset(time)

  override def queue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] =
    inner.queue(id, time, value)
}
