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

import javax.inject._

import _root_.util.Util
import akka.actor.ActorSystem
import play.api.mvc._

import scala.concurrent.ExecutionContext

@Singleton
class ClusterController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {


  def listPeers() = Action { _ =>
    Util.monitorBlock("ClusterController.listPeers") {
      Ok(RestmController.storageService.peerList.reduceOption(_ + "\n" + _).getOrElse(""))
    }
  }

  def addPeer(peer: String) = Action { _ =>
    Util.monitorBlock("ClusterController.addPeer") {
      RestmController.storageService.peers += peer
      Ok(RestmController.storageService.peerList.reduceOption(_ + "\n" + _).getOrElse(""))
    }
  }

  def delPeer(peer: String) = Action { _ =>
    Util.monitorBlock("ClusterController.delPeer") {
      RestmController.storageService.peers -= peer
      Ok(RestmController.storageService.peerList.reduceOption(_ + "\n" + _).getOrElse(""))
    }
  }
}
