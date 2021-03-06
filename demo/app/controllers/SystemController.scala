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
  private[this] val workers = getConfig("workers").map(Integer.parseInt).getOrElse(16)

  def shutdown(): Action[AnyContent] = Action.async {
    Util.monitorFuture("SystemController.shutdown") {
      StmDaemons.stop().map(_ => Ok("Node down"))
    }
  }

  def threadDump() = Action {
    Util.monitorBlock("SystemController.threadDump") {
      import scala.collection.JavaConverters._
      Ok(JacksonValue.simple(
        Thread.getAllStackTraces.asScala.mapValues(_.map(s => s"${s.getClass.getCanonicalName}.${s.getMethodName}(${s.getFileName}:${s.getLineNumber})"))
      ).pretty).as("application/json")
    }
  }

  def metrics() = Action {
    Util.monitorBlock("SystemController.metrics") {
      Ok(JacksonValue.simple(Util.getMetrics).pretty).as("application/json")
    }
  }

  def about() = Action {
    Util.monitorBlock("SystemController.about") {
      Ok(JacksonValue.simple(Map(
        "peers" -> RestmController.storageService.peers,
        "workers" -> workers,
        "table" -> RestmController.storageService.table,
        "peerPort" -> RestmController.storageService.peerPort
      )).pretty).as("application/json")
    }
  }

  def init() = Action {
    Util.monitorBlock("SystemController.init") {
      StmDaemons.start()(storageService)
      StmExecutionQueue.get().registerDaemons(workers)(storageService)
      Ok("Node started")
    }
  }

  def listLogs() = Action {
    Util.monitorBlock("SystemController.listLogs") {
      val fileListing: String = new File("logs").listFiles()
        .map(_.getName)
        .map(name =>s"""<a href="$name">$name</a> - <a href="$name?search=setState">Actor Txn Summary</a> <a href="$name?search=TXN+END">Client Txn Summary</a>""")
        .map(link =>s"""<li>$link</li>""")
        .reduceOption(_ + _)
        .map(body =>s"""<ul>$body</ul>""")
        .getOrElse("")
      Ok(s"""<html><body>$fileListing</body></html>""").as("text/html")
    }
  }

  def listLog(name: String, search: Option[String]) = Action {
    import akka.stream.scaladsl.Source
    Util.monitorBlock("SystemController.listLogs") {
      val searchR = search.map(search => s"(?<![01-9a-z])$search".r)
      val text: Stream[String] = (scala.io.Source.fromFile(new File(new File("logs"), name)).getLines.toStream ++ Stream(""))
        .scanLeft[(Option[String],String),Stream[(Option[String],String)]](None→"")((t,x)⇒{
          if(x.startsWith("\t")) {
            None→(t._2 + "\n" + x)
          } else {
            Option(t._2)→x
          }
        }).flatMap(_._1)
        .filter(line => searchR.isEmpty || searchR.get.findFirstIn(line).isDefined)
        .map(_.replaceAll("([01-9a-f]{8,8}-[01-9a-f]{4,4}-[01-9a-f]{4,4}-[01-9a-f]{4,4}-[01-9a-f]{12,12})", """<a href="?search=$1">$1</a>"""))
        .map(_.replaceAll("([01-9]{2,}:[01-9]{1,}:[01-9]{1,})", """<a href="?search=$1">$1</a>"""))
        .map(_.replaceAll("([01-9]{2,}:[01-9]{1,})", """<a href="?search=$1">$1</a>"""))
        .map(_.replaceAll("([01-9a-zA-Z\\+/]{32,}={0,2})", """..."""))
        .map(_.replaceAll("\n", "<br/>"))
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
