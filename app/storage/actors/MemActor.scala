package storage.actors

import storage.LockedException
import storage.Restm._
import util.ActorQueue
import util.OperationMetrics._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

class MemActor(name: PointerType)(implicit exeCtx: ExecutionContext) extends ActorQueue {

  private[this] val history = new scala.collection.mutable.ArrayBuffer[(TimeStamp, ValueType)]
  private[this] var lastRead: Option[TimeStamp] = None
  private[this] var writeLock: Option[TimeStamp] = None
  private[this] var committed: Boolean = false
  private[this] var queuedValue: Option[ValueType] = None

  private[this] var msg = 0

  private[this] def nextMsg = {
    msg += 1
    msg
  }

  private[this] def logMsg(msg: String) = {
    log(s"ptr@$name#$nextMsg $msg")
  }

  def getCurrentValue(): Future[Option[(TimeStamp, ValueType)]] = qos("ptr") {
    withActor {
      msg += 1
      Option(history).filterNot(_.isEmpty).map(_.maxBy(_._1))
    }.andThen({
      case Success(result) =>
        logMsg(s"getCurrentValue")
      case _ =>
    })
  }

  def getValue(time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] = qos("ptr") {
    withActor {
      writeLock.foreach(writeLock => if (writeLock < time) throw new LockedException(writeLock))
      lastRead = lastRead.filter(_ > time).orElse(Option(time))
      Option(history.filter(_._1 <= time).filter(_._1 >= ifModifiedSince.getOrElse(new TimeStamp(0l)))).filterNot(_.isEmpty).map(_.maxBy(_._1)._2)
    }.andThen({
      case Success(result) =>
        logMsg(s"getValue($time) $result")
      case _ =>
    })
  }

  def init(time: TimeStamp, value: ValueType) = qos("ptr") {
    withActor {
      if (history.nonEmpty) {
        false
      } else {
        history += time -> value
        lastRead = Option(time)
        writeLock = None
        queuedValue = None
        committed = false
        true
      }
    }
  }

  def writeLock(time: TimeStamp): Future[Option[TimeStamp]] = qos("ptr") {
    withActor {
      if (writeLock.isDefined) {
        if (writeLock.get == time) {
          // Write-locked
          logMsg(s"writeLock($time) ok - redundant")
          None
        } else {
          // Write-locked
          logMsg(s"writeLock($time) failed - write locked @ $writeLock")
          Option(writeLock.get)
        }
      } else if (lastRead.exists(_ > time)) {
        // Read-locked
        logMsg(s"writeLock($time) failed - read locked @ $lastRead")
        Option(lastRead.get)
      } else {
        logMsg(s"writeLock($time) ok")
        writeLock = Option(time)
        None
      }
    }
  }

  def writeBlob(time: TimeStamp, value: ValueType): Future[Unit] = qos("ptr") {
    withActor {
      require(writeLock.exists(time == _), s"Lock mismatch: $writeLock != $time")
      require(queuedValue.isEmpty, "Value already queued")
      if (committed) {
        history += writeLock.get -> value
        writeLock = None
        queuedValue = None
        committed = false
        true
      } else {
        queuedValue = Option(value)
        false
      }
    }.andThen({
      case Success(result) =>
        if (result) {
          logMsg(s"writeBlob($time, $value) written")
        } else {
          logMsg(s"writeBlob($time, $value) queued")
        }
      case _ =>
    }).map(_ => Unit)
  }

  def writeCommit(time: TimeStamp): Future[Unit] = qos("ptr") {
    withActor {
      require(writeLock.exists(time == _), "Lock mismatch")
      if (queuedValue.isDefined) {
        history += writeLock.get -> queuedValue.get
        writeLock = None
        queuedValue = None
        committed = false
        true
      } else {
        committed = true
        false
      }
    }.andThen({
      case Success(result) =>
        if (result) {
          logMsg(s"writeCommit($time) ok")
        } else {
          logMsg(s"writeCommit($time) pre-commit")
        }
      case _ =>
    }).map(_ => Unit)
  }

  def writeReset(time: TimeStamp = writeLock.get): Future[Unit] = qos("ptr") {
    withActor {
      require(writeLock.exists(time == _), "Lock mismatch")
      writeLock = None
      queuedValue = None
      committed = false
    }.andThen({
      case Success(result) =>
        logMsg(s"writeReset($time)")
      case _ =>
    })
  }


  override def toString = s"ptr@$name#$nextMsg"
}
