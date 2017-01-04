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

import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.ExecutionContext

object StmPool {
  private[stm] lazy val pool = Executors.newFixedThreadPool(8, new ThreadFactoryBuilder().setNameFormat("stm-pool-%d").build())
  private[stm] lazy val executionContext: ExecutionContext = ExecutionContext.fromExecutor(pool)
}
