package controllers

import java.io.File
import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.Executors
import javax.inject._

import _root_.util.Metrics
import akka.actor.ActorSystem
import org.apache.commons.io.FileUtils
import play.api.mvc._
import stm.lib0._
import storage.Restm._
import storage._
import storage.data.JacksonValue
import storage.util.{DynamoColdStorage, InternalRestmProxy}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object RestmController {
  val properties: Map[String, String] = System.getProperties.asScala.toMap
  lazy val configFile: Map[String, String] = Option(new File("restm.config"))
    .filter(_.exists())
    .map(FileUtils.readLines(_, "UTF-8"))
    .map(_.asScala.toList).getOrElse(List.empty)
    .map(_.trim).filterNot(_.startsWith("//"))
    .map(_.split("=").map(_.trim)).filter(2 == _.size)
    .map(split => split(0) -> split(1)).toMap
  def getConfig(key:String) = {
    configFile.get(key).orElse(properties.get(key))
  }
}
import controllers.RestmController._

@Singleton
class RestmController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {

  val peers = new mutable.HashSet[String]()
  val localName: String = InetAddress.getLocalHost.getHostAddress
  val table = getConfig("dynamoTable")
  private lazy val dynamo: Option[DynamoColdStorage] = table.map(new DynamoColdStorage(_))
  private val coldStorage: ColdStorage = dynamo.getOrElse(new HeapColdStorage)
  private lazy val local: RestmActors = new RestmActors(coldStorage)(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
  val peerPort = getConfig("peerPort").map(Integer.parseInt(_)).getOrElse(898)
  val workers = getConfig("workers").map(Integer.parseInt(_)).getOrElse(8)

  def peerList: List[String] = (peers.toList ++ Set(localName)).sorted

  val storageService = new RestmImpl(new RestmInternalStaticListRouter {
    override def shards: List[RestmInternal] = {
      peerList.map(name => {
        if (name == localName) local
        else new InternalRestmProxy(s"http://$name:$peerPort")
      })
    }
  })

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

  def newValue(time: String) = Action.async { request => Metrics.codeFuture("RestmController.newValue") {
    storageService.newPtr(new TimeStamp(time), request.body.asText.map(new ValueType(_)).get).map(x => Ok(x.toString))
  }
  }

  def getValue(id: String, time: Option[String], ifModifiedSince: Option[String]) = Action.async {
    Metrics.codeFuture("RestmController.getValue"){
      val value = if (time.isDefined) {
        storageService.getPtr(new PointerType(id), new TimeStamp(time.get), ifModifiedSince.map(new TimeStamp(_)))
      } else {
        storageService.getPtr(new PointerType(id))
      }
      val map: Future[Result] = value.map(opt => opt.map(v => Ok(v.toString)).getOrElse(if (ifModifiedSince.isDefined) NotModified else NotFound(id)))
      map.recover({
        case e: LockedException => Conflict(e.conflitingTxn.toString)
        case e: Throwable if e.toString.contains("Write locked") => Conflict("Write locked")
        case e: Throwable => throw e
      })
    }
  }

  def lockValue(id: String, time: String) = Action.async {
    Metrics.codeFuture("RestmController.lockValue") {
      storageService.lock(new PointerType(id), new TimeStamp(time)).map(x => {
        if (x.isEmpty) Ok("") else Conflict(x.toString)
      })
    }
  }

  def writeValue(id: String, time: String) = Action.async { request => Metrics.codeFuture("RestmController.writeValue") {
    storageService.queueValue(new PointerType(id), new TimeStamp(time), request.body.asText.map(new ValueType(_)).get).map(x => Ok(""))
  }}

  def delValue(id: String, time: String) = Action.async { request => Metrics.codeFuture("RestmController.delValue") {
    storageService.delete(new PointerType(id), new TimeStamp(time)).map(x => Ok(""))
  }}

  def newTxn(priority: Int) = Action.async {
    Metrics.codeFuture("RestmController.newTxn") {
      storageService.newTxn(priority.milliseconds).map(x => Ok(x.toString))
    }
  }

  def getTxn(time: String) = Action.async {
    Metrics.codeFuture("RestmController.getTxn") {
      storageService.internal._txnState(new TimeStamp(time)).map(x => Ok(x.toString))
    }
  }

  def commit(time: String) = Action.async {
    Metrics.codeFuture("RestmController.commit") {
      storageService.commit(new TimeStamp(time)).map(x => Ok(""))
    }
  }

  def reset(time: String) = Action.async {
    Metrics.codeFuture("RestmController.reset") {
      storageService.reset(new TimeStamp(time)).map(x => Ok(""))
    }
  }

  def _resetValue(id: String, time: String) = Action.async {
    Metrics.codeFuture("RestmController._resetValue") {
      storageService.internal._resetValue(new PointerType(id), new TimeStamp(time)).map(_ => Ok(""))
    }
  }

  def _lock(id: String, time: String) = Action.async {
    Metrics.codeFuture("RestmController._lock") {
      storageService.internal._lockValue(new PointerType(id), new TimeStamp(time)).map(x => x.map(_.toString).map(Ok(_)).getOrElse(Ok("")))
    }
  }

  def _commitValue(id: String, time: String) = Action.async {
    Metrics.codeFuture("RestmController._commitValue") {
      storageService.internal._commitValue(new PointerType(id), new TimeStamp(time)).map(_ => Ok(""))
    }
  }

  def _init(id: String, time: String) = Action.async { request => Metrics.codeFuture("RestmController._init") {
    storageService.internal._initValue(new TimeStamp(time), request.body.asText.map(new ValueType(_)).get, new PointerType(id))
      .map(ok => if (ok) Ok("") else Conflict(""))
  }
  }

  def _getValue(id: String, time: Option[String], ifModifiedSince: Option[String]) = Action.async {
    Metrics.codeFuture("RestmController._getValue") {
      if (time.isDefined) {
        storageService.internal._getValue(new PointerType(id), new TimeStamp(time.get), ifModifiedSince.map(new TimeStamp(_)))
          .map(x => Ok(x.map(_.toString).getOrElse(""))).recover({
          case e: LockedException => Conflict(e.conflitingTxn.toString)
          case e: Throwable => throw e
        })
      } else {
        storageService.internal._getValue(new PointerType(id)).map(x => Ok(x.map(_.toString).getOrElse("")))
      }
    }
  }

  def _addLock(id: String, time: String) = Action.async {
    Metrics.codeFuture("RestmController._addLock") {
      storageService.internal._addLock(new PointerType(id), new TimeStamp(time)).map(Ok(_))
    }
  }

  def _reset(time: String) = Action.async {
    Metrics.codeFuture("RestmController._reset") {
      storageService.internal._resetTxn(new TimeStamp(time)).map(x => x.map(_.toString).reduceOption(_ + "\n" + _).map(Ok(_)).getOrElse(Ok("")))
    }
  }

  def _commit(time: String) = Action.async {
    Metrics.codeFuture("RestmController._commit") {
      storageService.internal._commitTxn(new TimeStamp(time)).map(x => x.map(_.toString).reduceOption(_ + "\n" + _).map(Ok(_)).getOrElse(Ok("")))
    }
  }

  def shutdown() = Action.async {
    Metrics.codeFuture("RestmController.shutdown") {
      StmDaemons.stop().map(_=>Ok("Node down"))
    }
  }

  def taskResult(id: String) = Action.async {
    Metrics.codeFuture("RestmController.taskResult") {
      val task: Task[AnyRef] = Task[AnyRef](new PointerType(id))
      val future: Future[AnyRef] = task.future(storageService, exec)
      future.map(result=>Ok(JacksonValue(result).pretty).as("application/json"))
    }
  }

  def taskInfo(id: String) = Action.async {
    Metrics.codeFuture("RestmController.taskInfo") {
      Task(new PointerType(id)).atomic(storageService,exec).getStatusTrace.map(result=>Ok(JacksonValue(result).pretty).as("application/json"))
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
        "table" -> table,
        "peerPort" -> peerPort
      )).pretty).as("application/json")
    }
  }

  def init() = Action {
    Metrics.codeBlock("RestmController.init") {
      StmDaemons.start()(storageService,exec)
      StmExecutionQueue.registerDaemons(workers)(storageService,exec)
      Ok("Node started")
    }
  }

  def demoSort(n: Int) = Action.async {
    Metrics.codeFuture("RestmController.demoSort") {
      val collection = TreeCollection.static[String](new PointerType)
      Stream.continually(UUID.randomUUID().toString.take(8)).take(n).foreach((x:String)=>collection.atomic(storageService,exec).sync.add(x))
      collection.atomic(storageService, exec).sort()
        .map(task=>Ok(s"""<html><body><a href="/task/result/${task.id}">Task ${task.id} started</a></body></html>""")
          .as("text/html"))
    }
  }
}
