package storage.data

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.function.{IntUnaryOperator, LongUnaryOperator}

import storage.Restm._

import scala.concurrent.duration._
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration

object TxnTime {
  val start = timeVal()

  def timeVal(): Long = System.currentTimeMillis()

  val recent: TrieMap[TxnTime, AnyRef] = new scala.collection.concurrent.TrieMap[TxnTime, AnyRef]()

  def now = timeVal() - start
  val safeLast: AtomicLong = new AtomicLong(0)
  def safeNow = safeLast.updateAndGet(new LongUnaryOperator {
    override def applyAsLong(prev: Long): Long = Math.max(now, prev)
  })

  def next(priority: Duration): TxnTime = {
    recent.filterKeys(_.age > 1.seconds).keys.foreach(recent.remove)
    Stream.iterate(new TxnTime(safeNow + priority.toMillis,0))(_.next)
      .find(id => {
        val uuid = UUID.randomUUID()
        uuid == recent.getOrElseUpdate(id, uuid)
      }).get
  }

}

import storage.data.TxnTime._

case class TxnTime(epochMs: Long, sequence: Int) extends Ordered[TxnTime] {
  def age = Duration(TxnTime.safeNow - epochMs, TimeUnit.MILLISECONDS)

  def this() = this(TxnTime.safeNow, 0)
  def this(epochMs: Long) = this(epochMs, 0)
  def this(str: String) = this(
    java.lang.Long.parseLong(str.split(",")(0)),
    java.lang.Integer.parseInt(str.split(",")(1)))
  def next = copy(sequence = sequence+1)

  override def compare(o: TxnTime): Int = {
    if(epochMs != o.epochMs) {
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
