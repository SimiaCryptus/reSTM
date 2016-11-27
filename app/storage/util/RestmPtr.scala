package storage.util

import storage.Restm._
import storage.{LockedException, Restm}

import scala.concurrent.Future

trait RestmPtr extends Restm {

  def inner: Restm

  override def newPtr(time: TimeStamp, value: ValueType): Future[PointerType] =
    inner.newPtr(time, value)

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

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] =
    inner.queueValue(id, time, value)
}
