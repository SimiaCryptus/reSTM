package storage.util

import java.nio.charset.Charset
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import dispatch.{as, url, _}
import storage.Restm
import storage.Restm._

import scala.concurrent.{ExecutionContext, Future}


class RestmProxy(val baseUrl: String)(implicit executionContext: ExecutionContext) extends Restm {

  val utf8: Charset = Charset.forName("UTF-8")

  override def newTxn(priority: Int): Future[TimeStamp] = {
    Http((url(baseUrl) / "txn").addQueryParameter("priority", priority.toString) OK as.String)
      .map(new TimeStamp(_))
  }

  override def lock(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] = {
    Http((url(baseUrl) / "mem" / id.toString).addQueryParameter("time", time.toString).POST > { response => {
      response.getStatusCode match {
        case 200 => None
        case 409 => Option(new TimeStamp(response.getResponseBody))
      }
    }
    })
  }

  override def reset(id: TimeStamp): Future[Unit] = {
    Http((url(baseUrl) / "txn" / id.toString).DELETE OK as.String)
      .map(_ => {})
  }

  override def commit(id: TimeStamp): Future[Unit] = {
    Http((url(baseUrl) / "txn" / id.toString).POST OK as.String).map(_ => {})
  }

  override def getPtr(id: PointerType): Future[Option[ValueType]] = {
    Http(url(baseUrl) / "mem" / id.toString > { response => {
      response.getStatusCode match {
        case 200 => Option(new ValueType(response.getResponseBody))
        case 404 => None
      }
    }
    })
  }

  override def getPtr(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] = {
    var req: Req = (url(baseUrl) / "mem" / id.toString).addQueryParameter("time", time.toString)
    req = ifModifiedSince.map(ifModifiedSince => req.addQueryParameter("ifModifiedSince", ifModifiedSince.toString)).getOrElse(req)
    Http(req > { response => {
      response.getStatusCode match {
        case 200 => Option(new ValueType(response.getResponseBody))
        case 304 => None
        case 404 => None
      }
    }
    })
  }

  override def newPtr(time: TimeStamp, value: ValueType): Future[PointerType] = {
    Http((url(baseUrl) / "mem").addQueryParameter("time", time.toString).PUT << value.toString OK as.String)
      .map(new PointerType(_))
  }

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] = {
    Http((url(baseUrl) / "mem" / id.toString).addQueryParameter("time", time.toString).PUT << value.toString OK as.String)
      .map(_ => {})
  }

}
