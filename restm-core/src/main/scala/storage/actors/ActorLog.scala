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
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.{Executors, TimeUnit}

import util.Config

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


object ActorLog extends ActorQueue {
  val pool = Executors.newSingleThreadExecutor()
  implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(pool)

  private var writer: PrintWriter = _
  var enabled: Boolean = Config.getConfig("ActorLog").exists(java.lang.Boolean.parseBoolean)

  private def dateString = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now()).replaceAll(":","_")
  private var logname: String = ""
  private def file: File = {
    val str = if(logname.isEmpty) {
      s"logs/actors.$dateString.log"
    } else if(!logname.startsWith(".")) {
      s"logs/actors.$logname$dateString.log"
    } else {
      s"logs/actors$logname.$dateString.log"
    }
    new File(str)
  }
  private val start = now

  def logMsg(msg: String): Unit = log(s"ActorLog: $msg")

  def reset(name:String = dateString): Unit = withActor {
    logname = name
    writer = null
  }

  private var byteCounter = 0
  private var startAt = now

  override def log(str: String): Future[Unit] = if (!enabled) Future.successful(Unit) else withActor {
    if(byteCounter > 128*1024*1024 || (now - startAt) > 1.hour) {
      writer = null
    }
    if(null == writer) {
      startAt = now
      byteCounter = 0
      writer = new PrintWriter(new FileOutputStream(file))
    }
    writer.println(s"[$elapsed] " + str.replaceAll("\n","\n\t"))
    writer.flush()
    byteCounter = byteCounter + str.length
  }

  private def elapsed = (now - start).toUnit(TimeUnit.SECONDS)

  private def now = System.nanoTime().nanoseconds
}
