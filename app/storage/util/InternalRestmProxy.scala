package storage.util

import java.nio.charset.Charset
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import dispatch.{as, url, _}
import storage.Restm._
import storage.{LockedException, Restm, RestmImpl, RestmInternal}

import scala.concurrent.{ExecutionContext, ExecutionException, Future}

trait InternalRestmProxyTrait extends RestmInternal {
  def baseUrl:String
  def executionContext : ExecutionContext
  //RestmInternal
  override def _txnState(time: TimeStamp): Future[String] = {
    Http((url(baseUrl) / "txn" / time.toString).GET OK as.String)(executionContext)
  }

  override def _resetPtr(id: PointerType, time: TimeStamp): Future[Unit] = {
    var req: Req = (url(baseUrl) / "_ptr"/"reset" / id.toString).addQueryParameter("version", time.toString)
    Http(req.POST > { _ => Unit})(executionContext).map(_=>{})(executionContext)
  }

  override def _lock(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] = {
    var req: Req = (url(baseUrl) / "_ptr"/"lock" / id.toString).addQueryParameter("version", time.toString)
    Http(req.POST > { response => Option(response.getResponseBody).filterNot(_.isEmpty).map(new TimeStamp(_))})(executionContext)
  }

  override def _commitPtr(id: PointerType, time: TimeStamp): Future[Unit] = {
    var req: Req = (url(baseUrl) / "_ptr"/"commit" / id.toString).addQueryParameter("version", time.toString)
    Http(req.POST > { _ => Unit})(executionContext).map(_=>{})(executionContext)
  }

  override def _getPtr(id: PointerType): Future[Option[ValueType]] = {
    var req: Req = (url(baseUrl) / "_ptr"/"get" / id.toString)
    Http(req > { response => {
      response.getStatusCode match {
        case 200 => Option(new ValueType(response.getResponseBody))
        case 409 => throw new LockedException(new TimeStamp(response.getResponseBody))
      }
    }})(executionContext).recoverWith({
      case e : ExecutionException if e.getCause != null && e.getCause != e => Future.failed(e.getCause)
    })(executionContext)
  }

  override def _getPtr(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] = {
    var req: Req = (url(baseUrl) / "_ptr"/"get" / id.toString).addQueryParameter("version", time.toString)
    req = ifModifiedSince.map(ifModifiedSince=>req.addQueryParameter("ifModifiedSince", ifModifiedSince.toString)).getOrElse(req)
    Http(req > { response => {
      response.getStatusCode match {
        case 200 => Option(new ValueType(response.getResponseBody))
        case 409 => throw new LockedException(new TimeStamp(response.getResponseBody))
      }
    }})(executionContext).recoverWith({
      case e : ExecutionException if e.getCause != null && e.getCause != e => Future.failed(e.getCause)
    })(executionContext)
  }

  override def _addLock(id: PointerType, time: TimeStamp): Future[String] = {
    var req: Req = (url(baseUrl) / "_txn"/"addLock" / time.toString).addQueryParameter("id", id.toString)
    Http(req.POST > { response => response.getResponseBody})(executionContext)
  }

  override def _reset(time: TimeStamp): Future[Set[PointerType]] = {
    var req: Req = (url(baseUrl) / "_txn"/"reset" / time.toString)
    Http(req.POST > { response => response.getResponseBody.split("\n").map(new PointerType(_)).toSet})(executionContext)
  }

  override def _commit(time: TimeStamp): Future[Set[PointerType]] = {
    var req: Req = (url(baseUrl) / "_txn"/"commit" / time.toString)
    Http(req.POST > { response => response.getResponseBody.split("\n").filterNot(_.isEmpty).map(new PointerType(_)).toSet})(executionContext)
  }

  override def _init(version: TimeStamp, value: ValueType, id: PointerType): Future[Boolean] = {
    Http((url(baseUrl) / "_ptr" / "init" / id.toString).addQueryParameter("version", version.toString).PUT << value.toString > {_.getStatusCode match {
      case 200 => true
      case 409 => false
    }})(executionContext)
  }

  override def queue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] = {
    Http((url(baseUrl) / "ptr" / id.toString).addQueryParameter("version", time.toString).PUT << value.toString OK as.String)(executionContext)
      .map(_=>{})(executionContext)
  }

}

class InternalRestmProxy(val baseUrl:String) extends RestmImpl with InternalRestmProxyTrait
