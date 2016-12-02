package storage

import storage.Restm._
import storage.data._

import scala.concurrent.Future
import scala.concurrent.duration._

object Restm {
  type TimeStamp = TxnTime
  type ValueType = KryoValue
  type PointerType = StringPtr
}

class LockedException(val conflitingTxn: TimeStamp) extends Exception("Already locked by " + conflitingTxn)

trait Restm {
  def newPtr(time: TimeStamp, value: ValueType): Future[PointerType]

  @throws[LockedException] def getPtr(id: PointerType): Future[Option[ValueType]]

  @throws[LockedException] def getPtr(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp] = None): Future[Option[ValueType]]

  def newTxn(priority: Duration = 0.seconds): Future[TimeStamp]

  def lock(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]]

  def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit]

  def commit(time: TimeStamp): Future[Unit]

  def reset(time: TimeStamp): Future[Unit]

}





