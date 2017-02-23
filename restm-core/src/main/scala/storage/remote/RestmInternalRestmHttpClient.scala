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

import dispatch.{as, url, _}
import storage.Restm._
import storage.{RestmInternal, TransactionConflict}
import util.Util._
import scala.concurrent.{ExecutionContext, ExecutionException, Future}

class RestmInternalRestmHttpClient(val baseUrl: String)(implicit executionContext: ExecutionContext) extends RestmInternal {

  override def _txnState(time: TimeStamp): Future[String] = monitorFuture("RestmInternalRestmHttpClient._txnState") {
    Http((url(baseUrl) / "txn" / time.toString).GET OK as.String)
  }

  override def _resetValue(id: PointerType, time: TimeStamp): Future[Unit] = monitorFuture("RestmInternalRestmHttpClient._resetValue") {
    val req: Req = (url(baseUrl) / "_mem" / "reset" / id.toString).addQueryParameter("time", time.toString)
    Http(req.POST > { _ => Unit }).map(_ => {})
  }

  override def _lockValue(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] = monitorFuture("RestmInternalRestmHttpClient._lockValue") {
    Http((url(baseUrl) / "_mem" / "lock" / id.toString).addQueryParameter("time", time.toString).POST > { response =>
      Option(response.getResponseBody).filterNot(_.isEmpty).map(new TimeStamp(_))
    })
  }

  override def _commitValue(id: PointerType, time: TimeStamp): Future[Unit] = monitorFuture("RestmInternalRestmHttpClient._commitValue") {
    val req: Req = (url(baseUrl) / "_mem" / "commit" / id.toString).addQueryParameter("time", time.toString)
    Http(req.POST > { _ => Unit }).map(_ => {})
  }

  override def _getValue(id: PointerType): Future[Option[ValueType]] = monitorFuture("RestmInternalRestmHttpClient._getValue") {
    Http((url(baseUrl) / "_mem" / "get" / id.toString) > { response => {
      response.getStatusCode match {
        case 200 => Option(new ValueType(response.getResponseBody))
        case 409 => throw new TransactionConflict(new TimeStamp(response.getResponseBody))
      }
    }
    }).recoverWith({
      case e: ExecutionException if e.getCause != null && e.getCause != e => Future.failed(e.getCause)
    })
  }

  override def _getValue(id: PointerType, time: TimeStamp): Future[Option[ValueType]] = monitorFuture("RestmInternalRestmHttpClient._getValue") {
    val req: Req = (url(baseUrl) / "_mem" / "get" / id.toString).addQueryParameter("time", time.toString)
    Http(req > { response => {
      response.getStatusCode match {
        case 200 => Option(new ValueType(response.getResponseBody))
        case 409 => throw new TransactionConflict(new TimeStamp(response.getResponseBody))
      }
    }
    }).recoverWith({
      case e: ExecutionException if e.getCause != null && e.getCause != e => Future.failed(e.getCause)
    })
  }

  override def _addLock(id: PointerType, time: TimeStamp): Future[String] = monitorFuture("RestmInternalRestmHttpClient._addLock") {
    val req: Req = (url(baseUrl) / "_txn" / "addLock" / time.toString).addQueryParameter("id", id.toString)
    Http(req.POST > { response => response.getResponseBody })
  }

  override def _resetTxn(time: TimeStamp): Future[Set[PointerType]] = monitorFuture("RestmInternalRestmHttpClient._resetTxn") {
    Http((url(baseUrl) / "_txn" / "reset" / time.toString).POST > { response =>
      response.getResponseBody.split("\n").map(new PointerType(_)).toSet
    })
  }

  override def _commitTxn(time: TimeStamp): Future[Set[PointerType]] = monitorFuture("RestmInternalRestmHttpClient._commitTxn") {
    Http((url(baseUrl) / "_txn" / "commit" / time.toString).POST > { response =>
      response.getResponseBody.split("\n").filterNot(_.isEmpty).map(new PointerType(_)).toSet
    })
  }

  override def _initValue(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean] = monitorFuture("RestmInternalRestmHttpClient._initValue") {
    Http((url(baseUrl) / "_mem" / "init" / id.toString).addQueryParameter("time", time.toString).PUT << value.toString
      > {
      _.getStatusCode match {
        case 200 => true
        case 409 => false
      }
    })
  }

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] = monitorFuture("RestmInternalRestmHttpClient.queueValue") {
    val request: Req = (url(baseUrl) / "mem" / id.toString).addQueryParameter("time", time.toString)
    Http(request.PUT << value.toString OK as.String).map(_ => {})
  }

  override def delete(id: PointerType, time: TimeStamp): Future[Unit] = monitorFuture("RestmInternalRestmHttpClient.delete") {
    Http((url(baseUrl) / "mem" / id.toString).addQueryParameter("time", time.toString).DELETE OK as.String).map(_ => {})
  }
}
