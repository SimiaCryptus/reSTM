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

import javax.inject._

import _root_.util.Util
import akka.actor.ActorSystem
import controllers.RestmController._
import play.api.mvc._
import stm.task.{StmExecutionQueue, Task, TaskStatusTrace}
import storage.Restm._
import storage.types.JacksonValue

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class ExecutionController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {

  def taskResult(id: String): Action[AnyContent] = Action.async {
    Util.monitorFuture("ExecutionController.taskResult") {
      val task: Task[AnyRef] = new Task[AnyRef](new PointerType(id))
      val future: Future[AnyRef] = task.future(storageService)
      future.map(result => Ok(JacksonValue(result).pretty).as("application/json"))
    }
  }

  def taskInfo(id: String): Action[AnyContent] = Action.async {
    Util.monitorFuture("ExecutionController.taskInfo") {
      val task: Task[AnyRef] = new Task(new PointerType(id))
      val trace: Future[TaskStatusTrace] = task.atomic()(storageService).getStatusTrace(StmExecutionQueue.get())
      trace.map(result => Ok(JacksonValue(result).pretty).as("application/json"))
    }
  }

}
