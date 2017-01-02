package storage.actors

import storage.Restm._
import util.Util

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

class TxnActor(name: String)(implicit exeCtx: ExecutionContext) extends ActorQueue {

  private[this] val locks = new mutable.HashSet[PointerType]()
  private[this] var state = "OPEN"

  private def objId = Integer.toHexString(System.identityHashCode(TxnActor.this))
  def logMsg(msg: String)(implicit exeCtx: ExecutionContext): Unit = log(s"$this $msg")
  override def toString = s"txn@$objId:$name#$messageNumber"

  def addLock(id: PointerType): Future[String] = Util.monitorFuture("TxnActor.addLock") {
    {
      withActor {
        if (state == "OPEN") locks += id
        state
      }.andThen({
        case Success(result) =>
          logMsg(s"addLock($id) $result")
        case _ =>
      })
    }
  }


  def setState(s: String): Future[Set[PointerType]] = Util.monitorFuture("TxnActor.setState") {
    {
      withActor {
        if (state != s) {
          require(state == "OPEN", s"State is $state")
          require(s != "OPEN", "Cannot reopen")
          state = s
        }
        locks.toArray.toSet[PointerType]
      }.andThen({
        case Success(result) =>
          logMsg(s"setState($s) $result")
        case _ =>
      })
    }
  }

  def getState: Future[String] = Util.monitorFuture("TxnActor.getState") {
    {
      withActor {
        state
      }
    }
  }

}
