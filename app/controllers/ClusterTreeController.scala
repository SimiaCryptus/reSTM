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

import akka.actor.ActorSystem
import play.api.mvc._
import stm.STMPtr
import stm.clustering._
import stm.clustering.strategy.ClassificationStrategy
import stm.task.Task
import storage.RestmImpl
import storage.types.JacksonValue
import util.Util._

import scala.concurrent.{ExecutionContext, Future}


@Singleton
class ClusterTreeController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {

  implicit val cluster: RestmImpl = RestmController.storageService


  def add(treeId: String, label: String): Action[AnyContent] = Action.async { request =>
    monitorFuture("ClusterTreeController.add") {
      val tree = ClassificationTree(treeId)
      val items = bodyAsItems(request)
      tree.atomic().addAll(label, items.toList).map(_ => {
        Ok(JacksonValue.simple(Map(
          "result" → "ok", "items" → items.size, "treeId" → treeId, "label" → label
        )).pretty).as("application/json")
      })
    }
  }

  private val digits = Set('0','1','2','3','4','5','6','7','8','9','-','.','e','+')

  private def getBodyAsItem(request: Request[AnyContent]) = {
    ClassificationTreeItem(attributes = bodyAsItem(request) ++ queryStringAsItem(request))
  }

  private def queryStringAsItem(request: Request[AnyContent]) = {
    val queryStringAttributes = request.queryString.mapValues(_.head).mapValues(str ⇒ {
      if (str.forall(digits.contains)) {
        try {
          java.lang.Double.parseDouble(str)
        } catch {
          case e: NumberFormatException ⇒ str
        }
      } else {
        str
      }
    })
    queryStringAttributes
  }

  private def bodyAsItems(request: Request[AnyContent]): Stream[ClassificationTreeItem] = {
    val body = request.body.asText.getOrElse("")
    val stream = JacksonValue.deserializeSimpleStream[AnyRef](body)
    stream.map(_.asInstanceOf[Map[String,Any]]).map(x⇒new ClassificationTreeItem(attributes = x))
  }

  private def bodyAsItem(request: Request[AnyContent]) = {
    val bodyAttributes = new JacksonValue(request.body.asText.getOrElse("")).deserialize[Map[String, Any]]().getOrElse(Map.empty)
    bodyAttributes
  }

  private def find(tree: ClassificationTree, item: ClassificationTreeItem): Future[Result] = {
    tree.atomic().getClusterId(item).flatMap(ptr => {
      info(tree, ptr)
    })
  }

  private def info(tree: ClassificationTree, ptr: STMPtr[ClassificationTreeNode]) = {
    tree.atomic().getClusterPath(ptr).flatMap(path => {
      tree.atomic().getClusterCount(ptr).map(counts => {
        val returnValue = Map("path" -> path, "counts" -> counts)
        Ok(JacksonValue.simple(returnValue).pretty).as("application/json")
      })
    })
  }

  def find(treeId: String): Action[AnyContent] = {
    Action.async {
      (request: Request[AnyContent]) =>
        monitorFuture("ClusterTreeController.add") {
          val tree: ClassificationTree = ClassificationTree(treeId)
          val item: ClassificationTreeItem = getBodyAsItem(request)
          val result: Future[Result] = find(tree, item)
          result
        }
    }
  }

  def config(treeId: String): Action[AnyContent] = Action.async { request =>
    monitorFuture("ClusterTreeController.config") {
      val tree = ClassificationTree(treeId)
      request.body.asText.map(body => {
        new JacksonValue(body).deserialize[ClassificationStrategy]().get
      }).map(newStrategy => {
        tree.atomic().setClusterStrategy(newStrategy).flatMap(_ => {
          tree.atomic().getClusterStrategy.map((strategy: ClassificationStrategy) => {
            Ok(JacksonValue(strategy).pretty).as("application/json")
          })
        })
      }).getOrElse(
        tree.atomic().getClusterStrategy.map((strategy: ClassificationStrategy) => {
          Ok(JacksonValue(strategy).pretty).as("application/json")
        })
      )
    }
  }

  def info(treeId: String, clusterId: String): Action[AnyContent] = Action.async { _ =>
    monitorFuture("ClusterTreeController.info") {
      val tree = ClassificationTree(treeId).atomic()
      tree.getClusterByTreeId(clusterId).flatMap((ptr: STMPtr[ClassificationTreeNode]) => {
        tree.getClusterPath(ptr).flatMap(path => {
          tree.getClusterCount(ptr).map(counts => {
            val returnValue = Map("path" -> path, "counts" -> counts)
            Ok(JacksonValue.simple(returnValue).pretty).as("application/json")
          })
        })
      })
    }
  }

  def list(treeId: String, clusterId: String, cursor: Int = 0, maxItems: Int = 100): Action[AnyContent] = Action.async { _ =>
    monitorFuture("ClusterTreeController.list") {
      ClassificationTree(treeId).atomic().getClusterByTreeId(clusterId)
        .flatMap((nodePtr: STMPtr[ClassificationTreeNode]) => {
          nodePtr.atomic.read.flatMap((node: ClassificationTreeNode) => {
            node.atomic().stream(nodePtr).map((items: Stream[LabeledItem]) => {
              Ok(JacksonValue.simple(items.toList).pretty).as("application/json")
            })
          })
        })
    }
  }

  def split(treeId: String, clusterId: String): Action[AnyContent] = Action.async { request =>
    monitorFuture("ClusterTreeController.split") {
      val tree = ClassificationTree(treeId).atomic()
      tree.getClusterByTreeId(clusterId).flatMap((ptr: STMPtr[ClassificationTreeNode]) => {
        val strategy = request.body.asText.flatMap(body => new JacksonValue(body).deserialize[ClassificationStrategy]())
        strategy.map(Future.successful).getOrElse(tree.getClusterStrategy).flatMap(strategy => {
          tree.splitCluster(ptr, strategy).map((result: Task[Int]) => {
            Ok(JacksonValue.simple(Map("task"→result.id)).pretty).as("application/json")
          })
        })
      })
    }
  }


}






