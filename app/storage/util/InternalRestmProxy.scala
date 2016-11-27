package storage.util

import dispatch.{as, url, _}
import storage.Restm._
import storage.{LockedException, RestmImpl, RestmInternal}

import scala.concurrent.{ExecutionContext, ExecutionException, Future}

trait InternalRestmProxyTrait extends RestmInternal {

  def baseUrl: String
  def executionContext: ExecutionContext

  override def _txnState(time: TimeStamp): Future[String] = {
    Http((url(baseUrl) / "txn" / time.toString).GET OK as.String)(executionContext)
  }

  override def _resetValue(id: PointerType, time: TimeStamp): Future[Unit] = {
    var req: Req = (url(baseUrl) / "_mem" / "reset" / id.toString).addQueryParameter("time", time.toString)
    Http(req.POST > { _ => Unit })(executionContext).map(_ => {})(executionContext)
  }

  override def _lockValue(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] = {
    Http((url(baseUrl) / "_mem" / "lock" / id.toString).addQueryParameter("time", time.toString).POST > { response =>
      Option(response.getResponseBody).filterNot(_.isEmpty).map(new TimeStamp(_))
    })(executionContext)
  }

  override def _commitValue(id: PointerType, time: TimeStamp): Future[Unit] = {
    var req: Req = (url(baseUrl) / "_mem" / "commit" / id.toString).addQueryParameter("time", time.toString)
    Http(req.POST > { _ => Unit })(executionContext).map(_ => {})(executionContext)
  }

  override def _getValue(id: PointerType): Future[Option[ValueType]] = {
    Http((url(baseUrl) / "_mem" / "get" / id.toString) > { response => {
      response.getStatusCode match {
        case 200 => Option(new ValueType(response.getResponseBody))
        case 409 => throw new LockedException(new TimeStamp(response.getResponseBody))
      }
    }})(executionContext).recoverWith({
      case e: ExecutionException if e.getCause != null && e.getCause != e => Future.failed(e.getCause)
    })(executionContext)
  }

  override def _getValue(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] = {
    var req: Req = (url(baseUrl) / "_mem" / "get" / id.toString).addQueryParameter("time", time.toString)
    req = ifModifiedSince.map(ifModifiedSince => req.addQueryParameter("ifModifiedSince", ifModifiedSince.toString))
      .getOrElse(req)
    Http(req > { response => {
      response.getStatusCode match {
        case 200 => Option(new ValueType(response.getResponseBody))
        case 409 => throw new LockedException(new TimeStamp(response.getResponseBody))
      }
    }
    })(executionContext).recoverWith({
      case e: ExecutionException if e.getCause != null && e.getCause != e => Future.failed(e.getCause)
    })(executionContext)
  }

  override def _addLock(id: PointerType, time: TimeStamp): Future[String] = {
    var req: Req = (url(baseUrl) / "_txn" / "addLock" / time.toString).addQueryParameter("id", id.toString)
    Http(req.POST > { response => response.getResponseBody })(executionContext)
  }

  override def _resetTxn(time: TimeStamp): Future[Set[PointerType]] = {
    Http((url(baseUrl) / "_txn" / "reset" / time.toString).POST > { response =>
      response.getResponseBody.split("\n").map(new PointerType(_)).toSet
    })(executionContext)
  }

  override def _commitTxn(time: TimeStamp): Future[Set[PointerType]] = {
    Http((url(baseUrl) / "_txn" / "commit" / time.toString).POST > { response =>
      response.getResponseBody.split("\n").filterNot(_.isEmpty).map(new PointerType(_)).toSet
    })(executionContext)
  }

  override def _initValue(time: TimeStamp, value: ValueType, id: PointerType): Future[Boolean] = {
    Http((url(baseUrl) / "_mem" / "init" / id.toString).addQueryParameter("time", time.toString).PUT << value.toString
      > { _.getStatusCode match {
        case 200 => true
        case 409 => false
      }})(executionContext)
  }

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] = {
    Http((url(baseUrl) / "mem" / id.toString).addQueryParameter("time", time.toString).PUT
      << value.toString OK as.String)(executionContext).map(_ => {})(executionContext)
  }

}

class InternalRestmProxy(val baseUrl: String) extends RestmImpl with InternalRestmProxyTrait
