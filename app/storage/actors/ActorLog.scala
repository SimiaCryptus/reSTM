package storage.actors

import java.io.{File, FileOutputStream, PrintWriter}

import util.Config

import scala.concurrent.{ExecutionContext, Future}


object ActorLog extends ActorQueue {

  private def now = System.currentTimeMillis

  private val file: File = new File(s"logs/actors.$now.log")
  private lazy val writer: PrintWriter = new PrintWriter(new FileOutputStream(file))
  var enabled = Config.getConfig("ActorLog").map(java.lang.Boolean.parseBoolean(_)).getOrElse(false)
  def logMsg(msg: String)(implicit exeCtx: ExecutionContext) = log(s"ActorLog: $msg")

  private val start = now
  private def elapsed = (now - start) / 1000.0

  override def log(str: String)(implicit exeCtx: ExecutionContext): Future[Unit] = if(!enabled) Future.successful(Unit) else withActor {
    writer.println(s"[$elapsed] " + str)
    writer.flush()
  }
}
