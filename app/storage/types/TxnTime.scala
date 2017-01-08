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
import java.util.concurrent.atomic.AtomicLong
import java.util.function.LongUnaryOperator

import scala.concurrent.duration._

object TxnTime {
  def now: Long = System.nanoTime()
  val start = now
  def processTime = now - start

  val safeLast: AtomicLong = new AtomicLong(0)
  def safeNow: Long = safeLast.updateAndGet(new LongUnaryOperator {
    override def applyAsLong(prev: Long): Long = Math.max(processTime, prev+1)
  })
  def next(priority: Duration = 0.seconds): TxnTime = new TxnTime(safeNow, priority.toMillis.toInt)
  val vmInstanceId = UUID.randomUUID().toString.split("-").head
}

case class TxnTime(time: Long, priorityMs: Int) extends Ordered[TxnTime] {
  val instance: String = TxnTime.vmInstanceId

  def this() = this(TxnTime.processTime, 0)
  def this(str: String) = this(
    java.lang.Long.parseLong(str.split(":")(0)),
    java.lang.Integer.parseInt(str.split(":")(1))
  )

  private def effectiveTime = time + priorityMs * 1000000
  def age = (TxnTime.processTime - time).nanoseconds
  override def compare(o: TxnTime): Int = {
    val x = java.lang.Long.compare(effectiveTime, o.effectiveTime)
    if(x != 0) x else {
      instance.compareTo(o.instance)
    }
  }
  override def toString: String = s"$time:$priorityMs"
}
