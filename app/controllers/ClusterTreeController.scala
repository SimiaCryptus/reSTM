package controllers

import javax.inject._

import akka.actor.ActorSystem
import play.api.mvc._
import stm.STMPtr
import stm.collection.ClassificationTree.{ClassificationTreeItem, ClassificationTreeNode}
import stm.collection.{ClassificationStrategy, ClassificationTree}
import storage.types.JacksonValue
import util.Util._

import scala.concurrent.{ExecutionContext, Future}


@Singleton
class ClusterTreeController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {

  implicit val cluster = RestmController.storageService


  def add(treeId:String, label:String="_") = Action.async { request => monitorFuture("ClusterTreeController.add"){
    val tree = ClassificationTree(treeId)
    val item: ClassificationTreeItem = getBodyAsItem(request)
    tree.atomic().add(label, item).flatMap(_=>{
      find(tree, item)
    })
  }}

  private def getBodyAsItem(request: Request[AnyContent]) = {
    val map = new JacksonValue(request.body.asText.get).deserialize[Map[String,Any]]().get
    new ClassificationTreeItem(attributes = map)
  }

  def find(treeId:String): Action[AnyContent] = {
    Action.async {
      (request: Request[AnyContent]) => monitorFuture("ClusterTreeController.add") {
        val tree: ClassificationTree = ClassificationTree(treeId)
        val item: ClassificationTreeItem = getBodyAsItem(request)
        val result: Future[Result] = find(tree, item)
        result
      }
    }
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

  def config(treeId:String): Action[AnyContent] = Action.async { request => monitorFuture("ClusterTreeController.config"){
    val tree = ClassificationTree(treeId)
    request.body.asText.map(body=>{
      new JacksonValue(body).deserialize[ClassificationStrategy]().get
    }).map(newStrategy=>{
      tree.atomic().setClusterStrategy(newStrategy).flatMap(_=>{
        tree.atomic().getClusterStrategy().map((strategy: ClassificationStrategy) =>{
          Ok(JacksonValue(strategy).pretty).as("application/json")
        })
      })
    }).getOrElse(
      tree.atomic().getClusterStrategy().map((strategy: ClassificationStrategy) =>{
        Ok(JacksonValue(strategy).pretty).as("application/json")
      })
    )
  }}

  def info(treeId:String, clusterId:Int) = Action.async { request => monitorFuture("ClusterTreeController.info"){
    val tree = ClassificationTree(treeId).atomic()
    tree.getClusterByTreeId(clusterId).flatMap((ptr: STMPtr[ClassificationTreeNode]) => {
      tree.getClusterPath(ptr).flatMap(path => {
        tree.getClusterCount(ptr).map(counts => {
          val returnValue = Map("path" -> path, "counts" -> counts)
          Ok(JacksonValue(returnValue).pretty).as("application/json")
        })
      })
    })
  }}

  def list(treeId:String, clusterId:Int, cursor:Int=0, maxItems:Int=100) = Action.async { request => monitorFuture("ClusterTreeController.list"){
    val tree = ClassificationTree(treeId).atomic()
    tree.getClusterByTreeId(clusterId).flatMap((ptr: STMPtr[ClassificationTreeNode]) => {
      tree.iterateCluster(ptr, maxItems, cursor).map( result => {
          Ok(JacksonValue(result).pretty).as("application/json")
      })
    })
  }}

  def split(treeId:String, clusterId:Int) = Action.async { request => monitorFuture("ClusterTreeController.split"){
    val tree = ClassificationTree(treeId).atomic()
    tree.getClusterByTreeId(clusterId).flatMap((ptr: STMPtr[ClassificationTreeNode]) => {
      val strategy = request.body.asText.flatMap(body=>new JacksonValue(body).deserialize[ClassificationStrategy]())
      strategy.map(Future.successful(_)).getOrElse(tree.getClusterStrategy()).flatMap(strategy=>{
        tree.splitCluster(ptr,strategy).map( result => {
          Ok(JacksonValue(result).pretty).as("application/json")
        })
      })
    })
  }}


}





