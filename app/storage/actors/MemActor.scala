package storage.actors

import storage.LockedException
import storage.Restm._
import util.OperationMetrics._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success


class HistoryRecord(val time : TimeStamp, val value : ValueType) {
  var coldStorageTs : Option[Long] = None
}

class MemActor(name: PointerType)(implicit exeCtx: ExecutionContext) extends ActorQueue {

  val history = new scala.collection.mutable.ArrayBuffer[HistoryRecord]
  private[this] var lastRead: Option[TimeStamp] = None
  private[this] var writeLock: Option[TimeStamp] = None
  private[this] var committed: Boolean = false
  private[this] var queuedValue: Option[ValueType] = None

  private[this] def logMsg(msg: String) = log(s"$this $msg")
  private def objId = Integer.toHexString(System.identityHashCode(MemActor.this))
  override def toString = s"ptr@$objId:$name#${history.size}#$messageNumber"

  def getCurrentValue: Future[Option[(TimeStamp, ValueType)]] = qos("ptr") {
    withActor {
      Option(history.toArray).filterNot(_.isEmpty).map(_.maxBy(_.time))
        .map(record=>record.time->record.value)
    }.andThen({
      case Success(result) =>
        logMsg(s"getCurrentValue")
      case e : Throwable => logMsg(s"getCurrentValue failed - $e")
    })
  }

  def getValue(time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] = qos("ptr") {
    withActor {
      writeLock.foreach(writeLock => if (writeLock < time) throw new LockedException(writeLock))
      lastRead = lastRead.filter(_ > time).orElse(Option(time))
      Option(history.toArray.filter(_.time <= time)
        .filter(_.time >= ifModifiedSince.getOrElse(new TimeStamp(0l))))
        .filterNot(_.isEmpty).map(_.maxBy(_.time).value)
    }.andThen({
      case Success(result) =>
        logMsg(s"getValue($time, $ifModifiedSince) $result")
      case e : Throwable => logMsg(s"getValue failed - $e")
    })
  }

  def init(time: TimeStamp, value: ValueType) = qos("ptr") {
    withActor {
      if (history.nonEmpty) {
        false
      } else {
        history += new HistoryRecord(time, value)
        lastRead = Option(time)
        writeLock = None
        queuedValue = None
        committed = false
        true
      }
    }.andThen({
      case Success(result) =>
        logMsg(s"init($time, $value) $result")
      case e : Throwable => logMsg(s"Init failed - $e")
    })
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
      require(writeLock.contains(time), s"Lock mismatch: $writeLock != $time")
      require(queuedValue.isEmpty, "Value already queued")
      if (committed) {
        history += new HistoryRecord(writeLock.get, value)
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
      case e : Throwable => logMsg(s"writeBlob failed - $e")
    }).map(_ => Unit)
  }

  def delete(time: TimeStamp): Future[Unit] = qos("ptr") {
    withActor {
      require(writeLock.contains(time), s"Lock mismatch: $writeLock != $time")
      require(queuedValue.isEmpty, "Value already queued")
      if (committed) {
        history += new HistoryRecord(writeLock.get, null)
        writeLock = None
        queuedValue = None
        committed = false
        true
      } else {
        queuedValue = Option(null)
        false
      }
    }.andThen({
      case Success(result) =>
        if (result) {
          logMsg(s"delete($time) written")
        } else {
          logMsg(s"delete($time) queued")
        }
      case e : Throwable => logMsg(s"delete failed - $e")
    }).map(_ => Unit)
  }

  def writeCommit(time: TimeStamp): Future[Unit] = qos("ptr") {
    withActor {
      require(writeLock.contains(time), "Lock mismatch")
      if (queuedValue.isDefined) {
        history += new HistoryRecord(writeLock.get, queuedValue.get)
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
      require(writeLock.contains(time), "Lock mismatch")
      writeLock = None
      queuedValue = None
      committed = false
    }.andThen({
      case Success(result) =>
        logMsg(s"writeReset($time)")
      case _ =>
    })
  }


}
