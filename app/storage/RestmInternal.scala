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

  def delete(id: PointerType, time: TimeStamp): Future[Unit]
}
