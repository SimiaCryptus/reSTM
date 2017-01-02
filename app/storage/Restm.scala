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
import storage.types._

import scala.concurrent.Future
import scala.concurrent.duration._

object Restm {
  type TimeStamp = TxnTime
  type ValueType = JacksonValue
  type PointerType = StringPtr

  def value(value: AnyRef): ValueType = JacksonValue(value)
}

class TransactionConflict(val msg: String, val cause: Throwable, val conflitingTxn: TimeStamp)
  extends RuntimeException(msg, cause) {
  def this(conflitingTxn: TimeStamp, cause: Throwable = null) = this({
    require(null != conflitingTxn);
    "Already locked by " + conflitingTxn
  }, cause, conflitingTxn)

  def this(msg: String) = this(msg, null, null)

  def this(msg: String, cause: Throwable) = this(msg, cause, {
    Option(cause).filter(_.isInstanceOf[TransactionConflict]).map(_.asInstanceOf[TransactionConflict].conflitingTxn).orNull
  })
}

trait Restm {
  def newPtr(time: TimeStamp, value: ValueType): Future[PointerType]

  @throws[TransactionConflict] def getPtr(id: PointerType): Future[Option[ValueType]]

  @throws[TransactionConflict] def getPtr(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp] = None): Future[Option[ValueType]]

  def newTxn(priority: Duration = 0.seconds): Future[TimeStamp]

  def lock(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]]

  def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit]

  def delete(id: PointerType, time: TimeStamp): Future[Unit]

  def commit(time: TimeStamp): Future[Unit]

  def reset(time: TimeStamp): Future[Unit]

}





