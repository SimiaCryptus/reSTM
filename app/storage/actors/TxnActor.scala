package storage.actors

import storage.Restm._
import util.AOP._
import util.ActorQueue

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

class TxnActor(name:String) extends ActorQueue {

  private[this] val locks = new mutable.HashSet[PointerType]()
  private[this] var state = "OPEN"
  private[this] var msg = 0
  private[this] def nextMsg = {
    msg += 1
    msg
  }
  private[this] def logMsg(msg: String)(implicit exeCtx: ExecutionContext) = {
    log(s"txn@$name#$nextMsg $msg")
  }

  def addLock(id: PointerType)(implicit exeCtx: ExecutionContext): Future[String] = qos("txn") { withActor {
    if(state == "OPEN") locks += id
    state
  }.andThen({
    case Success(result) =>
      logMsg(s"addLock($id) $result")
    case _ =>
  })}

  def setState(s: String)(implicit exeCtx: ExecutionContext) : Future[Set[PointerType]] = qos("txn") { withActor {
    if(state != s) {
      require(state == "OPEN", s"State is $state")
      require(s != "OPEN", "Cannot reopen")
      state = s
    }
    locks.toArray.toSet[PointerType]
  }.andThen({
    case Success(result) =>
      logMsg(s"setState($s) $result")
    case _ =>
  })}

  override def toString = s"txn@$name#$msg"

  def getState()(implicit exeCtx: ExecutionContext) = qos("txn") { withActor {
    state
  }}

}
