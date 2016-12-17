package controllers

import javax.inject._

import _root_.util.Config._
import _root_.util.Metrics
import akka.actor.ActorSystem
import controllers.RestmController._
import play.api.mvc._
import stm.concurrent.{StmDaemons, StmExecutionQueue}
import storage.data.JacksonValue

import scala.concurrent.ExecutionContext

@Singleton
class SystemController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {
  def shutdown() = Action.async {
    Metrics.codeFuture("RestmController.shutdown") {
      StmDaemons.stop().map(_=>Ok("Node down"))
    }
  }

  def threadDump() = Action {
    Metrics.codeBlock("RestmController.threadDump") {
      import scala.collection.JavaConverters._
      Ok(JacksonValue.simple(
        Thread.getAllStackTraces().asScala.mapValues(_.map(s=>s"${s.getClass.getCanonicalName}.${s.getMethodName}(${s.getFileName}:${s.getLineNumber})"))
      ).pretty).as("application/json")
    }
  }

  def metrics() = Action {
    Metrics.codeBlock("RestmController.metrics") {
      Ok(JacksonValue.simple(Metrics.get()).pretty).as("application/json")
    }
  }

  def about() = Action {
    Metrics.codeBlock("RestmController.about") {
      Ok(JacksonValue.simple(Map(
        "peers" -> peers,
        "workers" -> workers,
        "table" -> table,
        "peerPort" -> peerPort
      )).pretty).as("application/json")
    }
  }

  private[this] val workers = getConfig("workers").map(Integer.parseInt(_)).getOrElse(8)

  def init() = Action {
    Metrics.codeBlock("RestmController.init") {
      StmDaemons.start()(storageService,exec)
      StmExecutionQueue.registerDaemons(workers)(storageService,exec)
      Ok("Node started")
    }
  }

}
