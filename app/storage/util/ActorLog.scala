package storage.util

import java.io.{File, FileOutputStream, PrintWriter}

import util.ActorQueue

import scala.concurrent.{ExecutionContext, Future}


object ActorLog extends ActorQueue {

  private def now = System.currentTimeMillis
  private val file: File = new File(s"logs/actors.$now.log")
  private lazy val writer: PrintWriter = new PrintWriter(new FileOutputStream(file))

  override def log(str: String)(implicit exeCtx: ExecutionContext) : Future[Unit] = withActor {
    writer.println(str);writer.flush()
  }
}
