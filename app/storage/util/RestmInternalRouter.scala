package storage.util

import storage.Restm._
import storage.RestmInternal

import scala.concurrent.Future

trait RestmInternalRouter extends RestmInternal {
  def inner(shardObj:AnyRef) : RestmInternal

  override def _resetPtr(id: PointerType, time: TimeStamp): Future[Unit] =
    inner(id)._resetPtr(id, time)

  override def _lock(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] =
    inner(id)._lock(id, time)

  override def _commitPtr(id: PointerType, time: TimeStamp): Future[Unit] =
    inner(id)._commitPtr(id, time)

  override def _getPtr(id: PointerType): Future[Option[ValueType]] =
    inner(id)._getPtr(id)

  override def _init(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean] =
    inner(id)._init(time, value, id)

  override def _getPtr(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] =
    inner(id)._getPtr(id, time, ifModifiedSince)

  override def _addLock(id: PointerType, time: TimeStamp): Future[String] =
    inner(time)._addLock(id, time)

  override def _reset(time: TimeStamp): Future[Set[PointerType]] =
    inner(time)._reset(time)

  override def _commit(time: TimeStamp): Future[Set[PointerType]] =
    inner(time)._commit(time)

  override def _txnState(time: TimeStamp): Future[String] =
    inner(time)._txnState(time)

  override def queue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] =
    inner(id).queue(id, time, value)

}

trait RestmInternalHashRouter extends RestmInternalRouter {
  def shards: List[RestmInternal]

  def inner(shardObj: AnyRef): RestmInternal = {
    var mod = shardObj.hashCode() % shards.size
    while (mod < 0) mod += shards.size
    shards(mod)
  }
}
