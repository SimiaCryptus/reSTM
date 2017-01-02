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
