package storage.actors

import java.io.{File, FileOutputStream, PrintWriter}

import util.Config

import scala.concurrent.{ExecutionContext, Future}


object ActorLog extends ActorQueue {

  private lazy val writer: PrintWriter = new PrintWriter(new FileOutputStream(file))
  val enabled: Boolean = Config.getConfig("ActorLog").exists(java.lang.Boolean.parseBoolean)
  private val file: File = new File(s"logs/actors.$now.log")
  private val start = now

  def logMsg(msg: String)(implicit exeCtx: ExecutionContext): Unit = log(s"ActorLog: $msg")

  override def log(str: String)(implicit exeCtx: ExecutionContext): Future[Unit] = if (!enabled) Future.successful(Unit) else withActor {
    writer.println(s"[$elapsed] " + str)
    writer.flush()
  }

  private def elapsed = (now - start) / 1000.0

  private def now = System.currentTimeMillis
}
