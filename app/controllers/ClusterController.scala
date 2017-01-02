package controllers

import javax.inject._

import _root_.util.Util
import akka.actor.ActorSystem
import controllers.RestmController._
import play.api.mvc._

import scala.concurrent.ExecutionContext

@Singleton
class ClusterController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {


  def listPeers() = Action { _ => Util.monitorBlock("ClusterController.listPeers") {
    Ok(peerList.reduceOption(_ + "\n" + _).getOrElse(""))
  }
  }

  def addPeer(peer: String) = Action { _ => Util.monitorBlock("ClusterController.addPeer") {
    peers += peer
    Ok(peerList.reduceOption(_ + "\n" + _).getOrElse(""))
  }
  }

  def delPeer(peer: String) = Action { _ => Util.monitorBlock("ClusterController.delPeer") {
    peers -= peer
    Ok(peerList.reduceOption(_ + "\n" + _).getOrElse(""))
  }
  }
}
