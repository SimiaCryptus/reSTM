package storage.remote

import java.nio.charset.Charset

import dispatch.{as, url, _}
import storage.Restm._
import storage.{Restm, TransactionConflict}
import util.Util._

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


class RestmHttpClient(val baseUrl: String)(implicit executionContext: ExecutionContext) extends Restm {

  val utf8: Charset = Charset.forName("UTF-8")

  override def newTxn(priority: Duration): Future[TimeStamp] = monitorFuture("RestmHttpClient.newTxn") {
    Http((url(baseUrl) / "txn").addQueryParameter("priority", priority.toMillis.toString) OK as.String)
      .map(new TimeStamp(_))
  }

  override def lock(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] = monitorFuture("RestmHttpClient.lock") {
    Http((url(baseUrl) / "mem" / id.toString).addQueryParameter("time", time.toString).POST > { response => {
      response.getStatusCode match {
        case 200 => None
        case 409 => Option(new TimeStamp(response.getResponseBody))
      }
    }
    })
  }

  override def reset(id: TimeStamp): Future[Unit] = monitorFuture("RestmHttpClient.reset") {
    Http((url(baseUrl) / "txn" / id.toString).DELETE OK as.String)
      .map(_ => {})
  }

  override def commit(id: TimeStamp): Future[Unit] = monitorFuture("RestmHttpClient.commit") {
    Http((url(baseUrl) / "txn" / id.toString).POST OK as.String).map(_ => {})
  }

  override def getPtr(id: PointerType): Future[Option[ValueType]] = monitorFuture("RestmHttpClient.getPtr") {
    val req: Req = url(baseUrl) / "mem" / id.toString
    Http(req > { response => {
      response.getStatusCode match {
        case 200 => Success(Option(new ValueType(response.getResponseBody)))
        case 304 => Success(None)
        case 404 => Success(None)
        case 409 => Failure(new TransactionConflict(response.getResponseBody))
      }
    }}).map(_.get)
  }

  override def getPtr(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] = monitorFuture("RestmHttpClient.getPtr") {
    var req: Req = (url(baseUrl) / "mem" / id.toString).addQueryParameter("time", time.toString)
    req = ifModifiedSince.map(ifModifiedSince => req.addQueryParameter("ifModifiedSince", ifModifiedSince.toString)).getOrElse(req)
    Http(req > { response => {
      response.getStatusCode match {
        case 200 => Success(Option(new ValueType(response.getResponseBody)))
        case 304 => Success(None)
        case 404 => Success(None)
        case 409 => Failure(new TransactionConflict(response.getResponseBody))
      }
    }}).map(_.get)
  }

  override def newPtr(time: TimeStamp, value: ValueType): Future[PointerType] = monitorFuture("RestmHttpClient.newPtr") {
    Http((url(baseUrl) / "mem").addQueryParameter("time", time.toString).PUT << value.toString OK as.String)
      .map(new PointerType(_))
  }

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] = monitorFuture("RestmHttpClient.queueValue") {
    val request: Req = (url(baseUrl) / "mem" / id.toString).addQueryParameter("time", time.toString)
    Http(request.PUT << value.toString OK as.String).map(_ => {})
  }

  override def delete(id: PointerType, time: TimeStamp): Future[Unit] = monitorFuture("RestmHttpClient.delete") {
    val request: Req = (url(baseUrl) / "mem" / id.toString).addQueryParameter("time", time.toString)
    Http(request.DELETE OK as.String).map(_ => {})
  }

}
