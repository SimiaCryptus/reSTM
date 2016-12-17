package controllers

import java.util.UUID
import javax.inject._

import _root_.util.Metrics
import akka.actor.ActorSystem
import controllers.RestmController._
import play.api.mvc._
import stm.collection.TreeCollection
import storage.Restm._

import scala.concurrent.ExecutionContext

@Singleton
class DemoController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {

  def demoSort(n: Int) = Action.async {
    Metrics.codeFuture("DemoController.demoSort") {
      val collection = TreeCollection.static[String](new PointerType)
      Stream.continually(UUID.randomUUID().toString.take(8)).take(n).foreach((x:String)=>collection.atomic(storageService,exec).sync.add(x))
      collection.atomic(storageService, exec).sort()
        .map(task=>Ok(s"""<html><body><a href="/task/result/${task.id}">Task ${task.id} started</a></body></html>""")
          .as("text/html"))
    }
  }

}
