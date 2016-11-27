package storage.data

import java.util.UUID
import java.util.concurrent.TimeUnit

import storage.Restm._

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration

object TxnTime {
  val start = timeVal()

  def timeVal(): Long = System.currentTimeMillis()

  val recent: TrieMap[Long, AnyRef] = new scala.collection.concurrent.TrieMap[Long, AnyRef]()

  def now = timeVal() - start

  def next(priority: Int): TxnTime = {
    require(now >= 0)
    recent.filterKeys(_ < now - 100).keys.foreach(recent.remove)
    (now to (now + 1000))
      //.map(_+priorityMs(priority))
      .find(id => {
      val uuid = UUID.randomUUID()
      uuid == recent.getOrElseUpdate(id, uuid)
    }).map(new TimeStamp(_)).get
  }

  def priorityMs(priority: Int): Int = {
    val alpha = 0.001
    val result = (Math.log(1 + alpha * 20.0 * priority * Math.random()) / alpha).toInt
    require(result >= 0)
    result
  }

}

import storage.data.TxnTime._;

case class TxnTime(epochMs: Long = TxnTime.now) extends Ordered[TxnTime] with Comparable[TxnTime] {
  def age = Duration(now - epochMs, TimeUnit.MILLISECONDS)

  def this(str: String) = this(java.lang.Long.parseLong(str))

  override def compareTo(o: TxnTime): Int = java.lang.Long.compare(epochMs, o.epochMs)

  override def compare(that: TxnTime): Int = compareTo(that)

  override def toString: String = epochMs.toString
}
