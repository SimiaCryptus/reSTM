package controllers

import java.net.InetAddress
import java.util.concurrent.Executors
import javax.inject._

import akka.actor.ActorSystem
import play.api.mvc._
import storage.Restm._
import storage._
import storage.util.{InternalRestmProxy, RestmInternalHashRouter}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

@Singleton
class RestmController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {

  val peers = new mutable.HashSet[String]()
  val localName: String = InetAddress.getLocalHost.getHostAddress
  val peerPort = 898
  def peerList: List[String] = (peers.toList ++ Set(localName)).sorted

  val storageService = new RestmImpl(new RestmInternalHashRouter {
    val local: RestmActors = new RestmActors()(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))

    override def shards: List[RestmInternal] = {
      peerList.map(name => {
        if (name == localName) local
        else new InternalRestmProxy(s"http://$name:$peerPort")
      })
    }

  })

  def listPeers() = Action { request => {
    Ok(peerList.reduceOption(_ + "\n" + _).getOrElse(""))
  }
  }

  def addPeer(peer: String) = Action { request => {
    peers += peer
    Ok(peerList.reduceOption(_ + "\n" + _).getOrElse(""))
  }
  }

  def delPeer(peer: String) = Action { request => {
    peers -= peer
    Ok(peerList.reduceOption(_ + "\n" + _).getOrElse(""))
  }
  }

  def newValue(time: String) = Action.async { request => {
    storageService.newPtr(new TimeStamp(time), request.body.asText.map(new ValueType(_)).get).map(x => Ok(x.toString))
  }
  }

  def getValue(id: String, time: Option[String], ifModifiedSince: Option[String]) = Action.async {
    val value = if (time.isDefined) {
      storageService.getPtr(new PointerType(id), new TimeStamp(time.get), ifModifiedSince.map(new TimeStamp(_)))
    } else {
      storageService.getPtr(new PointerType(id))
    }
    val map: Future[Result] = value.map(opt => opt.map(v => Ok(v.toString)).getOrElse(if (ifModifiedSince.isDefined) NotModified else NotFound(id)))
    map.recover({
      case e: LockedException => Conflict(e.conflitingTxn.toString)
      case e: Throwable if e.toString.contains("Write locked") => Conflict("Write locked")
      case e: Throwable => throw e
    })
  }

  def lockValue(id: String, time: String) = Action.async {
    storageService.lock(new PointerType(id), new TimeStamp(time)).map(x => {
      if (x.isEmpty) Ok("") else Conflict(x.toString)
    })
  }

  def writeValue(id: String, time: String) = Action.async { request => {
    storageService.queueValue(new PointerType(id), new TimeStamp(time), request.body.asText.map(new ValueType(_)).get).map(x => Ok(""))
  }
  }

  def newTxn(priority: Int) = Action.async {
    storageService.newTxn(priority.milliseconds).map(x => Ok(x.toString))
  }

  def getTxn(time: String) = Action.async {
    storageService.internal._txnState(new TimeStamp(time)).map(x => Ok(x.toString))
  }

  def commit(time: String) = Action.async {
    storageService.commit(new TimeStamp(time)).map(x => Ok(""))
  }

  def reset(time: String) = Action.async {
    storageService.reset(new TimeStamp(time)).map(x => Ok(""))
  }

  def _resetValue(id: String, time: String) = Action.async {
    storageService.internal._resetValue(new PointerType(id), new TimeStamp(time)).map(_ => Ok(""))
  }

  def _lock(id: String, time: String) = Action.async {
    storageService.internal._lockValue(new PointerType(id), new TimeStamp(time)).map(x => x.map(_.toString).map(Ok(_)).getOrElse(Ok("")))
  }

  def _commitValue(id: String, time: String) = Action.async {
    storageService.internal._commitValue(new PointerType(id), new TimeStamp(time)).map(_ => Ok(""))
  }

  def _init(id: String, time: String) = Action.async { request => {
    storageService.internal._initValue(new TimeStamp(time), request.body.asText.map(new ValueType(_)).get, new PointerType(id))
      .map(ok => if (ok) Ok("") else Conflict(""))
  }
  }

  def _getValue(id: String, time: Option[String], ifModifiedSince: Option[String]) = Action.async {
    if (time.isDefined) {
      storageService.internal._getValue(new PointerType(id), new TimeStamp(time.get), ifModifiedSince.map(new TimeStamp(_)))
        .map(x => Ok(x.map(_.toString).getOrElse(""))).recover({
        case e: LockedException => Conflict(e.conflitingTxn.toString)
        case e: Throwable => throw e
      })
    } else {
      storageService.internal._getValue(new PointerType(id)).map(x => Ok(x.map(_.toString).getOrElse("")))
    }
  }

  def _addLock(id: String, time: String) = Action.async {
    storageService.internal._addLock(new PointerType(id), new TimeStamp(time)).map(Ok(_))
  }

  def _reset(time: String) = Action.async {
    storageService.internal._resetTxn(new TimeStamp(time)).map(x => x.map(_.toString).reduceOption(_ + "\n" + _).map(Ok(_)).getOrElse(Ok("")))
  }

  def _commit(time: String) = Action.async {
    storageService.internal._commitTxn(new TimeStamp(time)).map(x => x.map(_.toString).reduceOption(_ + "\n" + _).map(Ok(_)).getOrElse(Ok("")))
  }
}
