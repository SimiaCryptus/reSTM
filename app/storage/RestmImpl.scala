package storage

import storage.Restm._
import storage.actors.ActorLog
import storage.types.TxnTime

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object RestmImpl {
  var failChainedCalls = false
}

class RestmImpl(val internal: RestmInternal)(implicit executionContext: ExecutionContext) extends Restm {

  val txnTimeout: FiniteDuration = 7.seconds

  override def getPtr(id: PointerType): Future[Option[ValueType]] = internal._getValue(id).recoverWith({
    case e: TransactionConflict if e.conflitingTxn.age > txnTimeout =>
      cleanup(e.conflitingTxn).flatMap(_ => Future.failed(e))
    case e: TransactionConflict =>
      Future.failed(e)
    case e: Throwable =>
      //e.printStackTrace(System.err);
      Future.failed(e)
  })

  override def getPtr(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] =
    internal._getValue(id, time, ifModifiedSince).recoverWith({
      case e: TransactionConflict if null != e.conflitingTxn && e.conflitingTxn.age > txnTimeout =>
        cleanup(e.conflitingTxn).flatMap(_ => Future.failed(e))
      case e: TransactionConflict =>
        Future.failed(e)
      case e: Throwable =>
        //e.printStackTrace(System.err);
        Future.failed(e)
    })

  override def newPtr(time: TimeStamp, value: ValueType): Future[PointerType] = {
    def newPtrAttempt: Future[Option[PointerType]] = {
      val id: PointerType = new PointerType()
      internal._initValue(time, value, id).map(ok => Option(id).filter(_ => ok))
    }
    def recursiveNewPtr: Future[PointerType] = newPtrAttempt.flatMap(attempt => attempt.map(ptr => Future.successful(ptr))
      .getOrElse(recursiveNewPtr))
    recursiveNewPtr
  }

  override def newTxn(priority: Duration): Future[TimeStamp] = Future {
    TxnTime.next(priority)
  }

  override def lock(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] = {
    internal._lockValue(id, time).flatMap(result => {
      if (result.isEmpty) {
        internal._addLock(id, time).map({
          case "OPEN" => result
          case "RESET" => internal._resetValue(id, time); result
          case "COMMIT" =>
            System.err.println(s"Transaction committed before lock returned: ptr=$id, txn=$time")
            ActorLog.log(s"Transaction committed before lock returned: ptr=$id, txn=$time")
            internal._commitValue(id, time)
            result
        })
      } else {
        if (result.get.age > txnTimeout) {
          cleanup(result.get).map(_ => result)
        } else {
          Future.successful(result)
        }
      }
    })
  }

  def cleanup(time: TimeStamp): Future[Unit] = {
    val state: Future[String] = internal._txnState(time)
    state.map({
      case "COMMIT" => commit(time)
      case _ => reset(time)
    }).map(_ => Unit)
  }

  override def reset(time: TimeStamp): Future[Unit] = {
    internal._resetTxn(time).map(locks => if (!RestmImpl.failChainedCalls)
      Future.sequence(locks.map(internal._resetValue(_, time).recover({ case _ => Unit }))))
  }

  override def commit(time: TimeStamp): Future[Unit] = {
    internal._commitTxn(time).map(locks => if (!RestmImpl.failChainedCalls)
      Future.sequence(locks.map(internal._commitValue(_, time))))
  }

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] =
    internal.queueValue(id, time, value)

  override def delete(id: PointerType, time: TimeStamp): Future[Unit] =
    internal.delete(id, time)
}
