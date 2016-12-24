package controllers

import javax.inject._

import _root_.util.Util
import akka.actor.ActorSystem
import controllers.RestmController._
import play.api.mvc._
import stm.concurrent.{Task, TaskStatusTrace}
import storage.Restm._
import storage.data.JacksonValue

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class ExecutionController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {

  def taskResult(id: String) = Action.async {
    Util.monitorFuture("ExecutionController.taskResult") {
      val task: Task[AnyRef] = new Task[AnyRef](new PointerType(id))
      val future: Future[AnyRef] = task.future(storageService, exec)
      future.map(result=>Ok(JacksonValue(result).pretty).as("application/json"))
    }
  }

  def taskInfo(id: String) = Action.async {
    Util.monitorFuture("ExecutionController.taskInfo") {
      val task: Task[AnyRef] = new Task(new PointerType(id))
      val trace: Future[TaskStatusTrace] = task.atomic()(storageService, exec).getStatusTrace()
      trace.map(result=>Ok(JacksonValue(result).pretty).as("application/json"))
    }
  }

}
