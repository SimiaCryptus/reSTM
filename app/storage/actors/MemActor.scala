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

package storage.actors

import storage.Restm._
import storage.TransactionConflict
import util.Util

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


class HistoryRecord(val time: TimeStamp, val value: ValueType) {
  var coldStorageTs: Option[Long] = None
}

object MemActor {
  val safeRead = false

}

class MemActor(name: PointerType, var lastRead: Option[TimeStamp] = None)(implicit exeCtx: ExecutionContext) extends ActorQueue {


  val history = new scala.collection.mutable.ArrayBuffer[HistoryRecord]()
  val rwlock = new Object()
  //private[this] var lastRead: Option[TimeStamp] = None
  private[actors] var writeLock: Option[TimeStamp] = None
  private[this] var staleCommit: Option[TimeStamp] = None
  private[this] var queuedValue: Option[ValueType] = None

  override def toString = s"ptr@$objId:$name#${history.size}#$messageNumber"

  private def objId = Integer.toHexString(System.identityHashCode(MemActor.this))

  def collectGarbage(): Future[Int] = Util.monitorFuture("MemActor.gc") {
    withActor {
      val head = history.maxBy(_.time)
      val garbage = history.filter(_.time.age > 15.seconds).filter(_!=head).toArray
      garbage.map(item⇒history.remove(history.indexOf(item))).size
    }.andThen({
      case Success(items) =>
        logMsg(s"collectGarbage removed $items items")
      case Failure(e) => logMsg(s"collectGarbage failed - $e")
    })
  }

  def getCurrentValue: Future[Option[(TimeStamp, ValueType)]] = Util.monitorFuture("MemActor.getCurrentValue") {
    withActor {
      Option(history.toArray).filterNot(_.isEmpty).map(_.maxBy(_.time))
        .map(record => record.time -> record.value)
    }.andThen({
      case Success(_) =>
        logMsg(s"getCurrentValue")
      case Failure(e) => logMsg(s"getCurrentValue failed - $e")
    })
  }

  def getValue(time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] = //Util.monitorFuture("MemActor.getValue")
  {
    if (MemActor.safeRead) {
      withActor {
        writeLock.foreach(writeLock => if (writeLock < time) {
          logMsg(s"getValue failed - txn lock $writeLock")
          throw new TransactionConflict(writeLock)
        })
        lastRead = lastRead.filter(_ > time).orElse(Option(time))
        val record: Option[HistoryRecord] = history.synchronized {
          Option(history.lastIndexWhere(_.time <= time)).filter(_ >= 0).map(history(_))
        }
        Option(history.lastIndexWhere(_.time <= time)).filter(_ >= 0).map(history(_))
        val result: Option[ValueType] = record.filter(_.time >= ifModifiedSince.getOrElse(new TimeStamp(0l))).map(_.value).filterNot(_ == null)
        logMsg(s"getValue($time, $ifModifiedSince) $result")
        result
      }
    } else {
      if (lastRead.exists(_ >= time)) {
        val record: Option[HistoryRecord] = history.synchronized {
          Option(history.lastIndexWhere(_.time <= time)).filter(_ >= 0).map(history(_))
        }
        val result: Option[ValueType] = record.filter(_.time >= ifModifiedSince.getOrElse(new TimeStamp(0l))).map(_.value).filterNot(_ == null)
        logMsg(s"getValue($time, $ifModifiedSince) $result")
        Future.successful(result)
      } else {
        rwlock.synchronized {
          writeLock.foreach(writeLock => if (writeLock < time) {
            logMsg(s"getValue failed - txn lock $writeLock")
            throw new TransactionConflict(writeLock)
          })
          lastRead = lastRead.filter(_ > time).orElse(Option(time))
        }
        val record: Option[HistoryRecord] = history.synchronized {
          Option(history.lastIndexWhere(_.time <= time)).filter(_ >= 0).map(history(_))
        }
        val result: Option[ValueType] = record.filter(_.time >= ifModifiedSince.getOrElse(new TimeStamp(0l))).map(_.value).filterNot(_ == null)
        logMsg(s"getValue($time, $ifModifiedSince) $result")
        Future.successful(result)
      }
    }
  }

  def logMsg(msg: String): Unit = log(s"$this $msg")

  def init(time: TimeStamp, value: ValueType): Future[Boolean] = Util.monitorFuture("MemActor.init") {
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
          staleCommit = None
          true
        }
      }.andThen({
        case Success(result) =>
          logMsg(s"init($time, $value) $result")
        case Failure(e) => logMsg(s"Init failed - $e")
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
        if (!writeLock.contains(time) && !staleCommit.contains(time)) throw new TransactionConflict(s"Lock mismatch: $writeLock != $time")
        require(queuedValue.isEmpty, "Value already queued")
        if (staleCommit.contains(time)) {
          history.synchronized {
            history += new HistoryRecord(writeLock.get, value)
          }
          rwlock.synchronized {
            writeLock = None
          }
          queuedValue = None
          staleCommit = None
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
        case Failure(e) => logMsg(s"writeBlob failed - $e")
      }).map(_ => Unit)
    }
  }

  def delete(time: TimeStamp): Future[Unit] = Util.monitorFuture("MemActor.delete") {
    {
      withActor {
        if (!writeLock.contains(time) && !staleCommit.contains(time)) throw new TransactionConflict(s"Lock mismatch: $writeLock != $time")
        require(queuedValue.isEmpty, "Value already queued")
        if (staleCommit.contains(time)) {
          history.synchronized {
            history += new HistoryRecord(writeLock.get, null)
          }
          rwlock.synchronized {
            writeLock = None
          }
          queuedValue = Some(null)
          staleCommit = None
          true
        } else {
          queuedValue = Some(null)
          staleCommit = None
          false
        }
      }.andThen({
        case Success(result) =>
          if (result) {
            logMsg(s"delete($time) written")
          } else {
            logMsg(s"delete($time) queued")
          }
        case Failure(e) => logMsg(s"delete failed - $e")
      }).map(_ => Unit)
    }
  }

  def writeCommit(time: TimeStamp): Future[Unit] = Util.monitorFuture("MemActor.writeCommit") {
    {
      withActor {
        require(writeLock.contains(time), "Lock mismatch")
        //logMsg(s"writeCommit($time) - ${queuedValue} (${queuedValue.isDefined})")
        if (queuedValue.isDefined) {
          history.synchronized {
            history --= history.filter(_.time == time).toList // Remove duplicate times caused by init
            history += new HistoryRecord(writeLock.get, queuedValue.get)
          }
          staleCommit = None
          queuedValue = None
          rwlock.synchronized {
            writeLock = None
          }
          true
        } else {
          rwlock.synchronized {
            staleCommit = writeLock
            writeLock = None
          }
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
        staleCommit = None
      }.andThen({
        case Success(_) =>
          logMsg(s"writeReset($time)")
        case _ =>
      })
    }
  }


}
