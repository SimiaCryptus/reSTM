package controllers

import java.io.File
import javax.inject._

import _root_.util.Config._
import _root_.util.Util
import akka.actor.ActorSystem
import akka.util.ByteString
import controllers.RestmController._
import play.api.http.HttpEntity
import play.api.mvc._
import stm.task.{StmDaemons, StmExecutionQueue}
import storage.types.JacksonValue

import scala.concurrent.ExecutionContext

@Singleton
class SystemController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {
  def shutdown() = Action.async {
    Util.monitorFuture("SystemController.shutdown") {
      StmDaemons.stop().map(_=>Ok("Node down"))
    }
  }

  def threadDump() = Action {
    Util.monitorBlock("SystemController.threadDump") {
      import scala.collection.JavaConverters._
      Ok(JacksonValue.simple(
        Thread.getAllStackTraces().asScala.mapValues(_.map(s=>s"${s.getClass.getCanonicalName}.${s.getMethodName}(${s.getFileName}:${s.getLineNumber})"))
      ).pretty).as("application/json")
    }
  }

  def metrics() = Action {
    Util.monitorBlock("SystemController.metrics") {
      Ok(JacksonValue.simple(Util.getMetrics()).pretty).as("application/json")
    }
  }

  def about() = Action {
    Util.monitorBlock("SystemController.about") {
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
    Util.monitorBlock("SystemController.init") {
      StmDaemons.start()(storageService,exec)
      StmExecutionQueue.registerDaemons(workers)(storageService,exec)
      Ok("Node started")
    }
  }

  def listLogs() = Action {
    Util.monitorBlock("SystemController.listLogs") {
      val fileListing: String = new File("logs").listFiles()
        .map(_.getName)
        .map(name =>s"""<a href="$name">$name</a> - <a href="$name?search=setState">Txn Summary</a>""")
        .map(link =>s"""<li>$link</li>""")
        .reduceOption(_ + _)
        .map(body =>s"""<ul>$body</ul>""")
        .getOrElse("")
      Ok(s"""<html><body>$fileListing</body></html>""").as("text/html")
    }
  }

  def listLog(name: String, search:Option[String]) = Action {
    import akka.stream.scaladsl.Source
    Util.monitorBlock("SystemController.listLogs") {
      val searchR = search.map(search=>s"(?<![01-9a-z])$search".r)
      val text = scala.io.Source.fromFile(new File(new File("logs"), name)).getLines.toStream
        .filter(line=>searchR.isEmpty || searchR.get.findFirstIn(line).isDefined )
        .map(_.replaceAll("([01-9a-f]{8,8}-[01-9a-f]{4,4}-[01-9a-f]{4,4}-[01-9a-f]{4,4}-[01-9a-f]{12,12})", """<a href="?search=$1">$1</a>"""))
        .map(_.replaceAll("([01-9]{2,},[01-9]{1,2})", """<a href="?search=$1">$1</a>"""))
        .map(_.replaceAll("([01-9a-zA-Z\\+]{32,}={0,2})", """..."""))
        .map(line =>s"""<p>$line</p>""")
      val segments = List(
        Stream("<html><body>"),
        text,
        Stream("</body></html>")
      )
      Result(
        header = ResponseHeader(200, Map.empty),
        body = HttpEntity.Streamed(
          Source[ByteString](segments.flatten.map(html => ByteString(html))),
          None,
          Some("text/html"))
      )
    }
  }

}
