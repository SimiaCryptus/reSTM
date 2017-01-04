/*
 * Copyright (c) 2017 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package storage.remote

import storage.Restm._
import storage.RestmInternal

import scala.concurrent.Future

trait RestmInternalRouter extends RestmInternal {
  def inner(shardObj: AnyRef): RestmInternal

  override def _resetValue(id: PointerType, time: TimeStamp): Future[Unit] =
    inner(id)._resetValue(id, time)

  override def _lockValue(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] =
    inner(id)._lockValue(id, time)

  override def _commitValue(id: PointerType, time: TimeStamp): Future[Unit] =
    inner(id)._commitValue(id, time)

  override def _getValue(id: PointerType): Future[Option[ValueType]] =
    inner(id)._getValue(id)

  override def _initValue(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean] =
    inner(id)._initValue(time, value, id)

  override def _getValue(id: PointerType, time: TimeStamp): Future[Option[ValueType]] =
    inner(id)._getValue(id, time)

  override def _addLock(id: PointerType, time: TimeStamp): Future[String] =
    inner(time)._addLock(id, time)

  override def _resetTxn(time: TimeStamp): Future[Set[PointerType]] =
    inner(time)._resetTxn(time)

  override def _commitTxn(time: TimeStamp): Future[Set[PointerType]] =
    inner(time)._commitTxn(time)

  override def _txnState(time: TimeStamp): Future[String] =
    inner(time)._txnState(time)

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] =
    inner(id).queueValue(id, time, value)

  override def delete(id: PointerType, time: TimeStamp): Future[Unit] =
    inner(id).delete(id, time)

}

trait RestmInternalStaticListRouter extends RestmInternalRouter {
  def shards: List[RestmInternal]

  def inner(shardObj: AnyRef): RestmInternal = {
    var mod = shardObj.hashCode() % shards.size
    while (mod < 0) mod += shards.size
    shards(mod)
  }
}
