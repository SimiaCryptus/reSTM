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

package storage.actors

import storage.Restm._
import util.Util

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

class TxnActor(name: String)(implicit exeCtx: ExecutionContext) extends ActorQueue {

  private[this] val locks = new mutable.HashSet[PointerType]()
  private[this] var state = "OPEN"

  override def toString = s"txn@$objId:$name#$messageNumber"

  private def objId = Integer.toHexString(System.identityHashCode(TxnActor.this))

  def addLock(id: PointerType): Future[String] = Util.monitorFuture("TxnActor.addLock") {
    {
      withActor {
        if (state == "OPEN") locks += id
        state
      }.andThen({
        case Success(result) =>
          logMsg(s"addLock($id) $result")
        case _ =>
      })
    }
  }

  def logMsg(msg: String)(implicit exeCtx: ExecutionContext): Unit = log(s"$this $msg")

  def setState(s: String): Future[Set[PointerType]] = Util.monitorFuture("TxnActor.setState") {
    {
      withActor {
        if (state != s) {
          require(state == "OPEN", s"State is $state")
          require(s != "OPEN", "Cannot reopen")
          state = s
        }
        locks.toArray.toSet[PointerType]
      }.andThen({
        case Success(result) =>
          logMsg(s"setState($s) $result")
        case _ =>
      })
    }
  }

  def getState: Future[String] = Util.monitorFuture("TxnActor.getState") {
    {
      withActor {
        state
      }
    }
  }

}
