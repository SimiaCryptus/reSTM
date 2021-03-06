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

package storage.types

import java.util.UUID

class StringPtr(val id: String) {

  def this() = this(UUID.randomUUID().toString)
  def this(txn : TxnTime) = this(txn.toString + ":" + UUID.randomUUID().toString.take(4))

  override def toString: String = id.toString

  override def hashCode(): Int = id.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: StringPtr => id == x.id
    case _ => false
  }

}
