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

package stm

import storage.Restm
import storage.Restm._
import storage.actors.ActorLog
import util.Util
import util.Util.{chainEx, monitorFuture}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, _}
import scala.reflect.ClassTag

class STMTxnCtx(val cluster: Restm, val priority: Duration, val txn: STMTxn[_]) {
  private implicit def executionContext = StmPool.executionContext

  private[stm] lazy val txnId = cluster.newTxn(priority).map(id⇒{
    ActorLog.log(s"Txn $id defined at ${txn.codeId} called by ${txn.caller}, op id ${txn.opId}")
    id
  })
  private[stm] val defaultTimeout: Duration = 15.seconds
  private[this] val writeLocks = new TrieMap[PointerType, Future[Boolean]]()
  private[stm] val readCache: TrieMap[PointerType, Future[Option[_]]] = new TrieMap()
  private[stm] val initCache: TrieMap[PointerType, Option[AnyRef]] = new TrieMap()
  private[stm] val writeCache: TrieMap[PointerType, Option[AnyRef]] = new TrieMap()
  var isClosed = false

  final def log(msg: ⇒String): Future[Unit] = chainEx("Transaction Exception") {
    monitorFuture("STMTxn.txnLog") {
      txnId.flatMap(id⇒ActorLog.log(s"$id - $msg"))
    }
  }

  def newPtr[T <: AnyRef](value: T): Future[PointerType] =
    txnId.flatMap(cluster.newPtr(_, Restm.value(value)).map(ptr => {
      initCache.put(ptr, Option(value))
      ptr
    }))

  def delete(id: PointerType): Future[Unit] = Util.chainEx(s"Delete $id") {
    txnId.flatMap(txnId => Util.monitorFuture("STMTxnCtx.delete") {
      require(!isClosed)
      readOpt(id).flatMap(prior => {
        if (prior.isDefined) {
          lock(id).flatMap(_ => {
            if (!isClosed) {
              writeCache.put(id, None)
              Future.successful(Unit)
            } else {
              throw new RuntimeException(s"Post-commit write for $id")
              System.err.println(s"Post-commit delete for $id")
              cluster.delete(id, txnId)
            }
          })
        } else {
          Future.successful(Unit)
        }
      })
    })
  }

  private[stm] def readOpt[T <: AnyRef : ClassTag](id: PointerType)
                                                  : Future[Option[T]] = //Util.monitorFuture("STMTxnCtx.readOpt")
  {
    require(!isClosed)
    writeCache.get(id).orElse(initCache.get(id))
      .map(x => Future.successful(x.map(_.asInstanceOf[T]))).getOrElse(
      readCache.getOrElseUpdate(id,
        txnId.flatMap(txnId => { cluster.getPtr(id, txnId).map(_.flatMap(_.deserialize[T]())) })
      ).map(_.map(_.asInstanceOf[T])))
  }

  private[stm] def lock(id: PointerType): Future[Unit] = {
    lockOptional(id).map(success => if (!success) throw new RuntimeException(s"Lock failed: $id in txn $txnId"))
  }

  private[stm] def lockOptional(id: PointerType): Future[Boolean] = {
    writeLocks.getOrElseUpdate(id, txnId.flatMap(txnId => {
      require(!isClosed)
      cluster.lock(id, txnId)
    }).map(_.isEmpty))
  }

  override def toString: String = {
    "txn@" + txnId.value.map(_.get.toString).getOrElse("???")
  }

  private[stm] def commit(): Future[Unit] = Util.monitorFuture("STMTxnCtx.getCurrentValue") {
    //if(writeLocks.isEmpty) Future.successful(Unit) else
    isClosed = true
    txnId.flatMap(txnId => {
      val writeFutures: Iterable[Future[Unit]] = writeCache.map(write => {
        val (key: PointerType, value: Option[AnyRef]) = write
        value.map(newValue => cluster.queueValue(key, txnId, Restm.value(newValue)))
          .getOrElse(cluster.delete(key, txnId))
      })
      Future.sequence(writeFutures).flatMap(_ => cluster.commit(txnId))
    })
  }

  private[stm] def revert(): Future[Unit] = Util.monitorFuture("STMTxnCtx.getCurrentValue") {
    isClosed = true
    //if(writeLocks.isEmpty) Future.successful(Unit) else
    txnId.flatMap(cluster.reset)
  }

  private[stm] def write[T <: AnyRef : ClassTag](id: PointerType, value: T): Future[Unit] = Util.chainEx(s"write to $id") {
    txnId.flatMap(txnId => Util.monitorFuture("STMTxnCtx.write") {
      require(!isClosed)
      readOpt(id).flatMap(prior => {
        if (value != prior.orNull) {
          lock(id).flatMap(_ => {
            if (!isClosed) {
              writeCache.put(id, Option(value))
              Future.successful(Unit)
            } else {
              throw new RuntimeException(s"Post-commit write for $id")
              System.err.println(s"Post-commit write for $id")
              cluster.queueValue(id, txnId, Restm.value(value))
            }
          })
        } else {
          Future.successful(Unit)
        }
      })
    })
  }
}
