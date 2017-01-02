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

import java.util.UUID
import javax.inject._

import _root_.util.Util
import akka.actor.ActorSystem
import controllers.RestmController._
import play.api.mvc._
import stm.collection.TreeCollection
import storage.Restm._

import scala.concurrent.ExecutionContext

@Singleton
class DemoController @Inject()(actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends Controller {

  def demoSort(n: Int): Action[AnyContent] = Action.async {
    Util.monitorFuture("DemoController.demoSort") {
      val collection = new TreeCollection[String](new PointerType)
      Stream.continually(UUID.randomUUID().toString.take(8)).take(n).foreach((x: String) => collection.atomic()(storageService, exec).sync.add(x))
      collection.atomic()(storageService, exec).sort()
        .map(task => Ok(s"""<html><body><a href="/task/result/${task.id}">Task ${task.id} started</a></body></html>""")
          .as("text/html"))
    }
  }

}
