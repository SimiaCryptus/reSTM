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

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration

object TxnTime {
  val start: Long = timeVal()
  val recent: TrieMap[TxnTime, AnyRef] = new scala.collection.concurrent.TrieMap[TxnTime, AnyRef]()
  val safeLast: AtomicLong = new AtomicLong(0)

  def next(priority: Duration): TxnTime = {
    recent.filterKeys(_.age > 1000).keys.foreach(recent.remove)
    Stream.iterate(new TxnTime(safeNow + priority.toMillis, 0))(_.next)
      .find(id => {
        val uuid = UUID.randomUUID()
        uuid == recent.getOrElseUpdate(id, uuid)
      }).get
  }

  def safeNow: Long = safeLast.updateAndGet(new LongUnaryOperator {
    override def applyAsLong(prev: Long): Long = Math.max(now, prev)
  })

  def now: Long = timeVal() - start

  def timeVal(): Long = System.currentTimeMillis()

}

case class TxnTime(epochMs: Long, sequence: Int) extends Ordered[TxnTime] {
  def age = TxnTime.safeNow - epochMs

  def this() = this(TxnTime.safeNow, 0)

  def this(epochMs: Long) = this(epochMs, 0)

  def this(str: String) = this(
    java.lang.Long.parseLong(str.split(",")(0)),
    java.lang.Integer.parseInt(str.split(",")(1)))

  def next: TxnTime = copy(sequence = sequence + 1)

  override def compare(o: TxnTime): Int = {
    if (epochMs != o.epochMs) {
      java.lang.Long.compare(epochMs, o.epochMs)
    } else {
      java.lang.Long.compare(sequence, o.sequence)
    }
  }

  override def toString: String = s"$epochMs,$sequence"

  //  def fields: List[AnyVal] = List(epochMs, sequence)
  //  override def equals(obj: scala.Any): Boolean = obj match {
  //    case x:TxnTime => x.fields == this.fields
  //    case _ => false
  //  }
  //  override def hashCode(): Int = fields.hashCode()
}
