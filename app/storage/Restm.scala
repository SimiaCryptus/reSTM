package storage

import storage.Restm._
import storage.types._

import scala.concurrent.Future
import scala.concurrent.duration._

object Restm {
  def value(value: AnyRef): ValueType = JacksonValue(value)

  type TimeStamp = TxnTime
  type ValueType = JacksonValue
  type PointerType = StringPtr
}

class TransactionConflict(val msg:String, val cause : Throwable, val conflitingTxn: TimeStamp)
  extends RuntimeException(msg, cause) {
  def this(conflitingTxn: TimeStamp, cause:Throwable = null) = this({require(null != conflitingTxn);"Already locked by " + conflitingTxn}, cause, conflitingTxn)
  def this(msg:String) = this(msg, null, null)
  def this(msg:String, cause:Throwable) = this(msg, cause, {
    Option(cause).filter(_.isInstanceOf[TransactionConflict]).map(_.asInstanceOf[TransactionConflict].conflitingTxn).orNull
  })
}

trait Restm {
  def newPtr(time: TimeStamp, value: ValueType): Future[PointerType]

  @throws[TransactionConflict] def getPtr(id: PointerType): Future[Option[ValueType]]

  @throws[TransactionConflict] def getPtr(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp] = None): Future[Option[ValueType]]

  def newTxn(priority: Duration = 0.seconds): Future[TimeStamp]

  def lock(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]]

  def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit]

  def delete(id: PointerType, time: TimeStamp): Future[Unit]

  def commit(time: TimeStamp): Future[Unit]

  def reset(time: TimeStamp): Future[Unit]

}





