package storage

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import storage.Restm._
import storage.data.TxnTime

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object RestmImpl {
  var failChainedCalls = false
}

abstract class RestmImpl extends Restm with RestmInternal {
  implicit val executionContext = ExecutionContext.fromExecutor(new ThreadPoolExecutor(16, 16, 10, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable]()))

  override def getPtr(id: PointerType): Future[Option[ValueType]] = _getValue(id).recoverWith({
    case e: LockedException if (e.conflitingTxn.age > 5.seconds) =>
      cleanup(e.conflitingTxn).flatMap(_ => Future.failed(e))
    case e: LockedException =>
      Future.failed(e)
    case e: Throwable =>
      e.printStackTrace(System.err); Future.failed(e)
  })

  override def getPtr(id: PointerType, time: TimeStamp, ifModifiedSince: Option[TimeStamp]): Future[Option[ValueType]] =
    _getValue(id, time, ifModifiedSince).recoverWith({
      case e: LockedException if (e.conflitingTxn.age > 5.seconds) =>
        cleanup(e.conflitingTxn).flatMap(_ => Future.failed(e))
      case e: LockedException =>
        Future.failed(e)
      case e: Throwable =>
        e.printStackTrace(System.err); Future.failed(e)
    })

  override def newPtr(time: TimeStamp, value: ValueType): Future[PointerType] = {
    def newPtrAttempt: Future[Option[PointerType]] = {
      val id: PointerType = new PointerType()
      _initValue(time, value, id).map(ok => Option(id).filter(_ => ok))
    }
    def recursiveNewPtr: Future[PointerType] = newPtrAttempt.flatMap(_.map(Future.successful(_)).getOrElse(recursiveNewPtr))
    recursiveNewPtr
  }

  override def newTxn(priority: Int): Future[TimeStamp] = Future {
    TxnTime.next(priority)
  }

  override def lock(id: PointerType, time: TimeStamp): Future[Option[TimeStamp]] = {
    _lockValue(id, time).flatMap(result => {
      if (result.isEmpty) {
        _addLock(id, time).map(_ match {
          case "OPEN" => result
          case "RESET" => _resetValue(id, time); result
          case "COMMIT" =>
            System.err.println(s"Transaction committed before lock returned: ptr=$id, txn=$time")
            util.ActorLog.log(s"Transaction committed before lock returned: ptr=$id, txn=$time")
            _commitValue(id, time)
            result
        })
      } else {
        if (result.get.age > 5.seconds) {
          cleanup(result.get).map(_ => result)
        } else {
          Future.successful(result)
        }
      }
    })
  }


  def cleanup(time: TimeStamp): Future[Unit] = {
    val state: Future[String] = _txnState(time)
    state.map(_ match {
      case "COMMIT" => commit(time)
      case _ => reset(time)
    }).map(_ => Unit)
  }

  override def reset(time: TimeStamp): Future[Unit] = {
    _resetTxn(time).map(locks => if (!RestmImpl.failChainedCalls) Future.sequence(locks.map(_resetValue(_, time).recover({ case _ => Unit }))))
  }

  override def commit(time: TimeStamp): Future[Unit] = {
    _commitTxn(time).map(locks => if (!RestmImpl.failChainedCalls) Future.sequence(locks.map(_commitValue(_, time))))
  }

}
