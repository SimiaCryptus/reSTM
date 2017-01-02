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

package stm

import storage.Restm

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

abstract class AtomicApiBase(priority: Duration = 0.seconds, maxRetries: Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) {
  def atomic[T](f: STMTxnCtx => Future[T]): Future[T] = new STMTxn[T] {
    override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[T] = f(ctx)
  }.txnRun(cluster, priority = priority, maxRetry = maxRetries)(executionContext)
}
