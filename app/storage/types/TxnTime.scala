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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

import scala.concurrent.duration._

object ExactTime {
  implicit def apply(duration: Duration) = {
    new ExactTime(duration.toSeconds, duration.toNanos % 1000000000)
  }
}

case class ExactTime(epoch: Long, nanos: Long) extends Ordered[ExactTime] {
  def this() = this(System.currentTimeMillis() / 1000,(System.nanoTime() % 1000000000).toInt)

  def duration = Duration(nanos + (epoch * 1000000000), TimeUnit.NANOSECONDS)

  private[ExactTime] def carry : ExactTime = nanos match {
    case nanos if nanos < 0 ⇒ copy(epoch=epoch-1,nanos=nanos+1000000000).carry
    case nanos if nanos >= 1000000000 ⇒ copy(epoch=epoch+1,nanos=nanos-1000000000).carry
    case _ ⇒ this
  }

  private[ExactTime] def isNormal : Boolean = nanos match {
    case nanos if nanos < 0 ⇒ false
    case nanos if nanos >= 1000000000 ⇒ false
    case _ ⇒ true
  }

  def -(right:ExactTime) : ExactTime = {
    ExactTime(epoch - right.epoch, nanos - right.nanos).carry
  }

  def +(right:ExactTime) : ExactTime = {
    ExactTime(epoch + right.epoch, nanos + right.nanos).carry
  }

  override def compare(o: ExactTime): Int = {
    if(!isNormal) carry.compare(o)
    else if(!o.isNormal) this.compare(o.carry)
    else {
      val y = java.lang.Long.compare(epoch, o.epoch)
      if(y != 0) y else {
        java.lang.Long.compare(nanos, o.nanos)
      }
    }
  }


  override def toString: String = s"$epoch:$nanos"
}

object TxnTime {
  val safeLast = new AtomicReference(new ExactTime())
  def safeNow = safeLast.updateAndGet(new UnaryOperator[ExactTime] {
    override def apply(prev: ExactTime): ExactTime = {
      val time = new ExactTime()
      if(prev < time) {
        time
      } else {
        ExactTime(prev.epoch, prev.nanos+1)
      }
    }
  })
  def next(priority: Duration = 0.seconds): TxnTime = new TxnTime(safeNow, priority.toMillis.toInt)
  val vmInstanceId = UUID.randomUUID().toString.split("-").head
}

case class TxnTime(time : ExactTime, priorityMs: Int) extends Ordered[TxnTime] {
  val instance: String = TxnTime.vmInstanceId

  def this(epoch: Long, nanos: Long, pri:Int) = this(ExactTime(epoch,nanos), pri)
  def this() = this(TxnTime.safeNow, 0)
  def this(str: String) = this(
    java.lang.Long.parseLong(str.split(":")(0)),
    java.lang.Long.parseLong(str.split(":")(1)),
    java.lang.Integer.parseInt(str.split(":")(2))
  )

  def age = (new ExactTime() - time).duration
  def effectiveTime = time + priorityMs.milliseconds
  override def compare(o: TxnTime): Int = effectiveTime.compare(o.effectiveTime)
  override def toString: String = s"$time:$priorityMs"
}
