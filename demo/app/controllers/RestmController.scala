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

package controllers

import java.util.concurrent.Executors
import javax.inject._

import _root_.util.Util
import akka.actor.ActorSystem
import com.google.common.util.concurrent.ThreadFactoryBuilder
import play.api.mvc._
import storage.Restm._
import storage._
import storage.types.TxnTime

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object RestmController {

  val storageService = new ClusterRestmImpl()(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build())))
}

import controllers.RestmController._




@Singleton
class RestmController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {


  def newValue(time: String): Action[AnyContent] = Action.async { request =>
    Util.monitorFuture("RestmController.newValue") {
      storageService.newPtr(new TimeStamp(time), request.body.asText.map(new ValueType(_)).get).map(x => Ok(x.toString))
    }
  }

  def getValue(id: String, time: Option[String], ifModifiedSince: Option[String]): Action[AnyContent] = Action.async {
    Util.monitorFuture("RestmController.getValue") {
      val value = if (time.isDefined) {
        storageService.getPtr(new PointerType(id), new TimeStamp(time.get))
      } else {
        storageService.getPtr(new PointerType(id))
      }
      val map: Future[Result] = value.map(opt => opt.map(v => Ok(v.toString)).getOrElse(if (ifModifiedSince.isDefined) NotModified else NotFound(id)))
      map.recover({
        case e: TransactionConflict => Conflict(e.conflitingTxn.toString)
        case e: Throwable if e.toString.contains("Write locked") => Conflict("Write locked")
        case e: Throwable => throw e
      })
    }
  }

  def lockValue(id: String, time: String): Action[AnyContent] = Action.async {
    Util.monitorFuture("RestmController.lockValue") {
      storageService.lock(new PointerType(id), new TimeStamp(time)).map(x => {
        if (x.isEmpty) Ok("") else Conflict(x.toString)
      })
    }
  }

  def writeValue(id: String, time: String): Action[AnyContent] = Action.async { request =>
    Util.monitorFuture("RestmController.writeValue") {
      storageService.queueValue(new PointerType(id), new TimeStamp(time), request.body.asText.map(new ValueType(_)).get).map(_ => Ok(""))
    }
  }

  def delValue(id: String, time: String): Action[AnyContent] = Action.async { _ =>
    Util.monitorFuture("RestmController.delValue") {
      storageService.delete(new PointerType(id), new TimeStamp(time)).map(_ => Ok(""))
    }
  }

  def newTxn(priority: Int): Action[AnyContent] = Action {
    Util.monitorBlock("RestmController.newTxn") {
      Ok(TxnTime.next(priority.milliseconds).toString)
    }
  }

  def getTxn(time: String): Action[AnyContent] = Action.async {
    Util.monitorFuture("RestmController.getTxn") {
      storageService.internal._txnState(new TimeStamp(time)).map(x => Ok(x.toString))
    }
  }

  def commit(time: String): Action[AnyContent] = Action.async {
    Util.monitorFuture("RestmController.commit") {
      storageService.commit(new TimeStamp(time)).map(_ => Ok(""))
    }
  }

  def reset(time: String): Action[AnyContent] = Action.async {
    Util.monitorFuture("RestmController.reset") {
      storageService.reset(new TimeStamp(time)).map(_ => Ok(""))
    }
  }

  def _resetValue(id: String, time: String): Action[AnyContent] = Action.async {
    Util.monitorFuture("RestmController._resetValue") {
      storageService.internal._resetValue(new PointerType(id), new TimeStamp(time)).map(_ => Ok(""))
    }
  }

  def _lock(id: String, time: String): Action[AnyContent] = Action.async {
    Util.monitorFuture("RestmController._lock") {
      val future: Future[Option[TimeStamp]] = storageService.internal._lockValue(new PointerType(id), new TimeStamp(time))
      future.map(x => x.map(_.toString).map(Ok(_)).getOrElse(Ok("")))
    }
  }

  def _commitValue(id: String, time: String): Action[AnyContent] = Action.async {
    Util.monitorFuture("RestmController._commitValue") {
      storageService.internal._commitValue(new PointerType(id), new TimeStamp(time)).map(_ => Ok(""))
    }
  }

  def _init(id: String, time: String): Action[AnyContent] = Action.async { request =>
    Util.monitorFuture("RestmController._init") {
      storageService.internal._initValue(new TimeStamp(time), request.body.asText.map(new ValueType(_)).get, new PointerType(id))
        .map(ok => if (ok) Ok("") else Conflict(""))
    }
  }

  def _getValue(id: String, time: Option[String], ifModifiedSince: Option[String]): Action[AnyContent] = Action.async {
    Util.monitorFuture("RestmController._getValue") {
      if (time.isDefined) {
        storageService.internal._getValue(new PointerType(id), new TimeStamp(time.get))
          .map(x => Ok(x.map(_.toString).getOrElse(""))).recover({
          case e: TransactionConflict => Conflict(e.conflitingTxn.toString)
          case e: Throwable => throw e
        })
      } else {
        storageService.internal._getValue(new PointerType(id)).map(x => Ok(x.map(_.toString).getOrElse("")))
      }
    }
  }

  def _addLock(id: String, time: String): Action[AnyContent] = Action.async {
    Util.monitorFuture("RestmController._addLock") {
      storageService.internal._addLock(new PointerType(id), new TimeStamp(time)).map(Ok(_))
    }
  }

  def _reset(time: String): Action[AnyContent] = Action.async {
    Util.monitorFuture("RestmController._reset") {
      storageService.internal._resetTxn(new TimeStamp(time)).map(x => x.map(_.toString).reduceOption(_ + "\n" + _).map(Ok(_)).getOrElse(Ok("")))
    }
  }

  def _commit(time: String): Action[AnyContent] = Action.async {
    Util.monitorFuture("RestmController._commit") {
      storageService.internal._commitTxn(new TimeStamp(time)).map(x => x.map(_.toString).reduceOption(_ + "\n" + _).map(Ok(_)).getOrElse(Ok("")))
    }
  }
}






