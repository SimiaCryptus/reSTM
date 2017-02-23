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

import scala.concurrent.{ExecutionContext, Future}

trait RestmInternalReplicator extends RestmInternal {
  def inner(): Seq[RestmInternal]

  implicit def executionContext: ExecutionContext

  override def _lockValue(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] =
    Future.sequence(inner().map(_._lockValue(id, time))).map(_.reduceOption(_.orElse(_)).flatten).flatMap(result => {
      result.map(_ => _resetValue(id, time))
        .getOrElse(Future.successful(Unit)).map(_ => result)
    })

  override def _resetValue(id: PointerType, time: TimeStamp): Future[Unit] =
    Future.sequence(inner().map(_._resetValue(id, time))).map(_ => Unit)

  override def _commitValue(id: PointerType, time: TimeStamp): Future[Unit] =
    Future.sequence(inner().map(_._commitValue(id, time))).map(_ => Unit)

  override def _getValue(id: PointerType): Future[Option[ValueType]] =
    Future.find(inner().map(_._getValue(id)))(_.isDefined).map(_.flatten)

  override def _initValue(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean] =
    Future.sequence(inner().map(_._initValue(time, value, id))).map(_.reduceOption(_ && _).getOrElse(false)).flatMap(result => {
      if (!result) {
        _resetValue(id, time).map(_ => result)
      } else {
        Future.successful(result)
      }
    })

  override def _getValue(id: PointerType, time: TimeStamp): Future[Option[ValueType]] =
    Future.find(inner().map(_._getValue(id, time)))(_.isDefined).map(_.flatten)

  override def _addLock(id: PointerType, time: TimeStamp): Future[String] =
    Future.sequence(inner().map(_._addLock(id, time).map(Option(_).filterNot(_.isEmpty).filterNot(_ == "OPEN"))))
      .map(_.reduceOption(_.orElse(_)).flatten.getOrElse("OPEN"))

  override def _resetTxn(time: TimeStamp): Future[Set[PointerType]] =
    Future.sequence(inner().map(_._resetTxn(time))).map(_.reduceOption(_ ++ _).getOrElse(Set.empty))

  override def _commitTxn(time: TimeStamp): Future[Set[PointerType]] =
    Future.sequence(inner().map(_._commitTxn(time))).map(_.reduceOption(_ ++ _).getOrElse(Set.empty))

  override def _txnState(time: TimeStamp): Future[String] =
    Future.sequence(inner().map(_._txnState(time).map(Option(_).filterNot(_.isEmpty).filterNot(_ == "OPEN"))))
      .map(_.reduceOption(_.orElse(_)).flatten.getOrElse("OPEN"))

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] =
    Future.sequence(inner().map(_.queueValue(id, time, value))).map(_ => Unit)

  override def delete(id: PointerType, time: TimeStamp): Future[Unit] =
    Future.sequence(inner().map(_.delete(id, time))).map(_ => Unit)

}

class RestmInternalStaticListReplicator(val shards: Seq[RestmInternal])(implicit val executionContext: ExecutionContext) extends RestmInternalReplicator {

  override def inner(): Seq[RestmInternal] = shards
}
