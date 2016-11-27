package storage

import storage.Restm._
import storage.data.{JacksonValue, TxnTime, UUIDPtr}

import scala.concurrent.Future

object Restm {
  type TimeStamp = TxnTime
  type ValueType = JacksonValue
  type PointerType = UUIDPtr
}
class LockedException(val conflitingTxn:TimeStamp) extends Exception("Already locked by " + conflitingTxn)

trait Restm {
  def newPtr(version: TimeStamp, value: ValueType) : Future[PointerType]
  @throws[LockedException] def getPtr(id: PointerType) : Future[Option[ValueType]]
  @throws[LockedException] def getPtr(id: PointerType, time: TimeStamp, ifModifiedSince:Option[TimeStamp] = None) : Future[Option[ValueType]]

  def newTxn(priority: Int = 0) : Future[TimeStamp]
  def lock(id: PointerType, time: TimeStamp) : Future[Option[TimeStamp]]
  def queue(id: PointerType, time: TimeStamp, value: ValueType) : Future[Unit]
  def commit(time: TimeStamp) : Future[Unit]
  def reset(time: TimeStamp) : Future[Unit]

}





