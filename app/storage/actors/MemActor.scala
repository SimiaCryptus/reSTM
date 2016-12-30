package storage.actors

import storage.Restm._
import storage.TransactionConflict
import util.Util

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success


class HistoryRecord(val time : TimeStamp, val value : ValueType) {
  var coldStorageTs : Option[Long] = None
}
object MemActor {
  val safeRead = false

}
class MemActor(name: PointerType)(implicit exeCtx: ExecutionContext) extends ActorQueue {


  val history = new scala.collection.mutable.ArrayBuffer[HistoryRecord]()
  private[this] var lastRead: Option[TimeStamp] = None
  private[actors] var writeLock: Option[TimeStamp] = None
  private[this] var committed: Boolean = false
  private[this] var queuedValue: Option[ValueType] = None

  private[this] def logMsg(msg: String) = log(s"$this $msg")
  private def objId = Integer.toHexString(System.identityHashCode(MemActor.this))
  override def toString = s"ptr@$objId:$name#${history.size}#$messageNumber"

  def getCurrentValue: Future[Option[(TimeStamp, ValueType)]] = Util.monitorFuture("MemActor.getCurrentValue") {
    {
      withActor {
        Option(history.toArray).filterNot(_.isEmpty).map(_.maxBy(_.time))
          .map(record=>record.time->record.value)
      }.andThen({
        case Success(result) =>
          logMsg(s"getCurrentValue")
        case e : Throwable => logMsg(s"getCurrentValue failed - $e")
      })
    }
  }
  val rwlock = new Object()


  def getValue(time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] = //Util.monitorFuture("MemActor.getValue")
  {
    if(MemActor.safeRead) {
      withActor {
        writeLock.foreach(writeLock => if (writeLock < time) {
          logMsg(s"getValue failed - txn lock $writeLock")
          throw new TransactionConflict(writeLock)
        })
        lastRead = lastRead.filter(_ > time).orElse(Option(time))
        val record: Option[HistoryRecord] = history.synchronized {
          Option(history.lastIndexWhere(_.time <= time)).filter(_>=0).map(history(_))
        }
        Option(history.lastIndexWhere(_.time <= time)).filter(_>=0).map(history(_))
        val result: Option[ValueType] = record.filter(_.time >= ifModifiedSince.getOrElse(new TimeStamp(0l))).map(_.value)
        logMsg(s"getValue($time, $ifModifiedSince) $result")
        result
      }
    } else {
      if(false && lastRead.filter(_ >= time).isDefined) //Util.monitorFuture("MemActor.getValue.1")
      {
        val record: Option[HistoryRecord] = history.synchronized {
          Option(history.lastIndexWhere(_.time <= time)).filter(_>=0).map(history(_))
        }
        val result: Option[ValueType] = record.filter(_.time >= ifModifiedSince.getOrElse(new TimeStamp(0l))).map(_.value)
        logMsg(s"getValue($time, $ifModifiedSince) $result")
        Future.successful(result)
      } else //Util.monitorFuture("MemActor.getValue.2")
      {
        rwlock.synchronized {
          writeLock.foreach(writeLock => if (writeLock < time) {
            logMsg(s"getValue failed - txn lock $writeLock")
            throw new TransactionConflict(writeLock)
          })
          lastRead = lastRead.filter(_ > time).orElse(Option(time))
        }
        val record: Option[HistoryRecord] = history.synchronized {
          Option(history.lastIndexWhere(_.time <= time)).filter(_>=0).map(history(_))
        }
        val result: Option[ValueType] = record.filter(_.time >= ifModifiedSince.getOrElse(new TimeStamp(0l))).map(_.value)
        logMsg(s"getValue($time, $ifModifiedSince) $result")
        Future.successful(result)
      }
    }
  }

  def init(time: TimeStamp, value: ValueType) = Util.monitorFuture("MemActor.init") {
    {
      withActor {
        if (history.nonEmpty) {
          false
        } else {
          history.synchronized {
            history += new HistoryRecord(time, value)
          }
          rwlock.synchronized {
            lastRead = Option(time)
            writeLock = None
          }
          queuedValue = None
          committed = false
          true
        }
      }.andThen({
        case Success(result) =>
          logMsg(s"init($time, $value) $result")
        case e: Throwable => logMsg(s"Init failed - $e")
      })
    }
  }

  def writeLock(time: TimeStamp): Future[Option[TimeStamp]] = Util.monitorFuture("MemActor.writeLock") {
    {
      withActor {
        rwlock.synchronized {
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
          } else if (history.exists(_.time > time)) {
            // Already written
            val record = history.find(_.time >= time).get
            logMsg(s"writeLock($time) failed - written @ ${record.time}")
            Option(record.time)
          } else {
            logMsg(s"writeLock($time) ok")
            writeLock = Option(time)
            None
          }
        }
      }
    }
  }

  def writeBlob(time: TimeStamp, value: ValueType): Future[Unit] = Util.monitorFuture("MemActor.writeBlob") {
    {
      withActor {
        if(!writeLock.contains(time)) throw new TransactionConflict(s"Lock mismatch: $writeLock != $time")
        require(queuedValue.isEmpty, "Value already queued")
        if (committed) {
          history.synchronized {
            history += new HistoryRecord(writeLock.get, value)
          }
          rwlock.synchronized {
            writeLock = None
          }
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
        case e: Throwable => logMsg(s"writeBlob failed - $e")
      }).map(_ => Unit)
    }
  }

  def delete(time: TimeStamp): Future[Unit] = Util.monitorFuture("MemActor.delete") {
    {
      withActor {
        require(writeLock.contains(time), s"Lock mismatch: $writeLock != $time")
        require(queuedValue.isEmpty, "Value already queued")
        if (committed) {
          history.synchronized {
            history += new HistoryRecord(writeLock.get, null)
          }
          rwlock.synchronized {
            writeLock = None
          }
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
        case e: Throwable => logMsg(s"delete failed - $e")
      }).map(_ => Unit)
    }
  }

  def writeCommit(time: TimeStamp): Future[Unit] = Util.monitorFuture("MemActor.writeCommit") {
    {
      withActor {
        require(writeLock.contains(time), "Lock mismatch")
        if (queuedValue.isDefined) {
          history.synchronized {
            history --= history.filter(_.time==time).toList // Remove duplicate times caused by init
            history += new HistoryRecord(writeLock.get, queuedValue.get)
          }
          rwlock.synchronized {
            writeLock = None
          }
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
  }

  def writeReset(time: TimeStamp = writeLock.get): Future[Unit] = Util.monitorFuture("MemActor.writeReset") {
    {
      withActor {
        require(writeLock.contains(time), "Lock mismatch")
        rwlock.synchronized {
          writeLock = None
        }
        queuedValue = None
        committed = false
      }.andThen({
        case Success(result) =>
          logMsg(s"writeReset($time)")
        case _ =>
      })
    }
  }


}
