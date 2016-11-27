package controllers

import java.net.InetAddress
import javax.inject._

import akka.actor.ActorSystem
import play.api.mvc._
import storage.Restm._
import storage._
import storage.util.{InternalRestmProxy, InternalRestmProxyTrait, RestmInternalHashRouter}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class RestmController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {

  val peers = new mutable.HashSet[String]()
  val localName : String = InetAddress.getLocalHost.getHostAddress
  def peerList: List[String] = (peers.toList ++ Set(localName)).sorted
  val peerPort = 898

  val storageService = new RestmImpl with RestmInternalHashRouter {
    val local: RestmActors with Object = new RestmActors {}
    override def shards: List[RestmInternal] = {
      peerList.map(name => {
        if(name == localName) local
        else new InternalRestmProxyTrait {
          override def baseUrl: String = s"http://$name:$peerPort"
          override def executionContext: ExecutionContext = exec
        }
      })
    }
  }

  def listPeers() = Action { request=>{
    Ok(peerList.reduceOption(_+"\n"+_).getOrElse(""))
  }}

  def addPeer(peer:String) = Action { request=>{
    peers += peer
    Ok(peerList.reduceOption(_+"\n"+_).getOrElse(""))
  }}

  def delPeer(peer:String) = Action { request=>{
    peers -= peer
    Ok(peerList.reduceOption(_+"\n"+_).getOrElse(""))
  }}

  def newItem(version:String) = Action.async { request=>{
    storageService.newPtr(new TimeStamp(version), request.body.asText.map(new ValueType(_)).get).map(x=>Ok(x.toString))
  }}

  def getItem(id:String, version:Option[String], ifModifiedSince:Option[String]) = Action.async {
    val value = if(version.isDefined) {
      storageService.getPtr(new PointerType(id), new TimeStamp(version.get), ifModifiedSince.map(new TimeStamp(_)))
    } else {
      storageService.getPtr(new PointerType(id))
    }
    val map: Future[Result] = value.map(opt => opt.map(v=>Ok(v.toString)).getOrElse(if(ifModifiedSince.isDefined) NotModified else NotFound(id)))
    map.recover({
      case e : LockedException => Conflict(e.conflitingTxn.toString)
      case e : Throwable if e.toString.contains("Write locked") => Conflict("Write locked")
      case e : Throwable => throw e
    })
  }

  def lockItem(id:String, version: String) = Action.async {
    storageService.lock(new PointerType(id),new TimeStamp(version)).map(x=>{
      if(x.isEmpty) Ok("") else Conflict(x.toString)
    })
  }

  def writeItem(id:String, version: String) = Action.async { request=>{
    storageService.queue(new PointerType(id),new TimeStamp(version),request.body.asText.map(new ValueType(_)).get).map(x=>Ok(""))
  }}

  def newTxn(priority:Int) = Action.async {
    storageService.newTxn(priority).map(x=>Ok(x.toString))
  }

  def getTxn(version:String) = Action.async {
    storageService._txnState(new TimeStamp(version)).map(x=>Ok(x.toString))
  }

  def commit(version:String) = Action.async {
    storageService.commit(new TimeStamp(version)).map(x=>Ok(""))
  }

  def reset(version:String) = Action.async {
    storageService.reset(new TimeStamp(version)).map(x=>Ok(""))
  }

  def _resetPtr(id: String, version: String) = Action.async {
    storageService._resetPtr(new PointerType(id), new TimeStamp(version)).map(_=>Ok(""))
  }

  def _lock(id: String, version: String) = Action.async {
    storageService._lock(new PointerType(id), new TimeStamp(version)).map(x=>x.map(_.toString).map(Ok(_)).getOrElse(Ok("")))
  }

  def _commitPtr(id: String, version: String) = Action.async {
    storageService._commitPtr(new PointerType(id), new TimeStamp(version)).map(_=>Ok(""))
  }

  def _init(id: String, version: String) = Action.async { request=>{
    storageService._init(new TimeStamp(version),request.body.asText.map(new ValueType(_)).get,new PointerType(id))
      .map(ok=>if(ok) Ok("") else Conflict(""))
  }}

  def _getPtr(id: String, version: Option[String], ifModifiedSince: Option[String]) = Action.async {
    if(version.isDefined) {
      storageService._getPtr(new PointerType(id), new TimeStamp(version.get), ifModifiedSince.map(new TimeStamp(_)))
        .map(x=>Ok(x.map(_.toString).getOrElse(""))).recover({
        case e : LockedException => Conflict(e.conflitingTxn.toString)
        case e : Throwable => throw e
      })
    } else {
      storageService._getPtr(new PointerType(id)).map(x=>Ok(x.map(_.toString).getOrElse("")))
    }
  }

  def _addLock(id: String, version: String) = Action.async {
    storageService._addLock(new PointerType(id), new TimeStamp(version)).map(Ok(_))
  }

  def _reset(version: String) = Action.async {
    storageService._reset(new TimeStamp(version)).map(x=>x.map(_.toString).reduceOption(_+"\n"+_).map(Ok(_)).getOrElse(Ok("")))
  }

  def _commit(version: String) = Action.async {
    storageService._commit(new TimeStamp(version)).map(x=>x.map(_.toString).reduceOption(_+"\n"+_).map(Ok(_)).getOrElse(Ok("")))
  }
}
