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

package stm.clustering.strategy

import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import stm.STMTxnCtx
import stm.clustering.{KeyValue, Page, PageTree}

import scala.concurrent.ExecutionContext

case class RuleData(fn : (KeyValue[String,Any]) => Boolean, name : String = "???")

object ClassificationStrategy {
  val workerPool: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("rule-pool-%d").build()))

}

trait ClassificationStrategy {

  def getRule(values: Stream[Page]): RuleData

  def split(buffer: PageTree)(implicit ctx: STMTxnCtx): Boolean

}


