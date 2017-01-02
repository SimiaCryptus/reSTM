package controllers

import javax.inject._

import akka.actor.ActorSystem
import play.api.mvc._
import stm.STMPtr
import stm.collection.clustering.ClassificationTree.{ClassificationTreeItem, LabeledItem}
import stm.collection.clustering.{ClassificationStrategy, ClassificationTree, ClassificationTreeNode}
import storage.RestmImpl
import storage.types.JacksonValue
import util.Util._

import scala.concurrent.{ExecutionContext, Future}


@Singleton
class ClusterTreeController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {

  implicit val cluster: RestmImpl = RestmController.storageService


  def add(treeId: String, label: String = "_"): Action[AnyContent] = Action.async { request =>
    monitorFuture("ClusterTreeController.add") {
      val tree = ClassificationTree(treeId)
      val item: ClassificationTreeItem = getBodyAsItem(request)
      tree.atomic().add(label, item).flatMap(_ => {
        find(tree, item)
      })
    }
  }

  private def getBodyAsItem(request: Request[AnyContent]) = {
    val map = new JacksonValue(request.body.asText.get).deserialize[Map[String, Any]]().get
    ClassificationTreeItem(attributes = map)
  }

  private def find(tree: ClassificationTree, item: ClassificationTreeItem): Future[Result] = {
    tree.atomic().getClusterId(item).flatMap(ptr => {
      find(tree, ptr)
    })
  }

  private def find(tree: ClassificationTree, ptr: STMPtr[ClassificationTreeNode]) = {
    tree.atomic().getClusterPath(ptr).flatMap(path => {
      tree.atomic().getClusterCount(ptr).map(counts => {
        val returnValue = Map("path" -> path, "counts" -> counts)
        Ok(JacksonValue(returnValue).pretty).as("application/json")
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

  def info(treeId: String, clusterId: Int): Action[AnyContent] = Action.async { _ =>
    monitorFuture("ClusterTreeController.info") {
      val tree = ClassificationTree(treeId).atomic()
      tree.getClusterByTreeId(clusterId).flatMap((ptr: STMPtr[ClassificationTreeNode]) => {
        tree.getClusterPath(ptr).flatMap(path => {
          tree.getClusterCount(ptr).map(counts => {
            val returnValue = Map("path" -> path, "counts" -> counts)
            Ok(JacksonValue(returnValue).pretty).as("application/json")
          })
        })
      })
    }
  }

  def list(treeId: String, clusterId: Int, cursor: Int = 0, maxItems: Int = 100): Action[AnyContent] = Action.async { _ =>
    monitorFuture("ClusterTreeController.list") {
      ClassificationTree(treeId).atomic().getClusterByTreeId(clusterId)
        .flatMap((nodePtr: STMPtr[ClassificationTreeNode]) => {
          nodePtr.atomic.read.flatMap((node: ClassificationTreeNode) => {
            node.atomic().stream(nodePtr).map((items: Stream[LabeledItem]) => {
              Ok(JacksonValue(items.toList).pretty).as("application/json")
            })
          })
        })
    }
  }

  def split(treeId: String, clusterId: Int): Action[AnyContent] = Action.async { request =>
    monitorFuture("ClusterTreeController.split") {
      val tree = ClassificationTree(treeId).atomic()
      tree.getClusterByTreeId(clusterId).flatMap((ptr: STMPtr[ClassificationTreeNode]) => {
        val strategy = request.body.asText.flatMap(body => new JacksonValue(body).deserialize[ClassificationStrategy]())
        strategy.map(Future.successful).getOrElse(tree.getClusterStrategy).flatMap(strategy => {
          tree.splitCluster(ptr, strategy).map(result => {
            Ok(JacksonValue(result).pretty).as("application/json")
          })
        })
      })
    }
  }


}






