package controllers

import javax.inject._

import _root_.util.Metrics
import akka.actor.ActorSystem
import controllers.RestmController._
import play.api.mvc._

import scala.concurrent.ExecutionContext

@Singleton
class ClusterController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {


  def listPeers() = Action { request => Metrics.codeBlock("RestmController.listPeers") {
    Ok(peerList.reduceOption(_ + "\n" + _).getOrElse(""))
  }
  }

  def addPeer(peer: String) = Action { request => Metrics.codeBlock("RestmController.addPeer") {
    peers += peer
    Ok(peerList.reduceOption(_ + "\n" + _).getOrElse(""))
  }
  }

  def delPeer(peer: String) = Action { request => Metrics.codeBlock("RestmController.delPeer") {
    peers -= peer
    Ok(peerList.reduceOption(_ + "\n" + _).getOrElse(""))
  }
  }
}
