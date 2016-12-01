package stm

import storage.Restm
import storage.Restm._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag

class STMTxnCtx(cluster: Restm, priority: Duration, prior: Option[STMTxnCtx]) {
  private[stm] val defaultTimeout: Duration = 5.seconds

  def newPtr[T <: AnyRef](value: T)(implicit executionContext: ExecutionContext): Future[PointerType] = txnId.flatMap(cluster.newPtr(_, new ValueType(value)))

  private[stm] def commit()(implicit executionContext: ExecutionContext): Future[Unit] =
  //if(writeLocks.isEmpty) Future.successful(Unit) else
    txnId.flatMap(cluster.commit)

  private[stm] def revert()(implicit executionContext: ExecutionContext): Future[Unit] =
  //if(writeLocks.isEmpty) Future.successful(Unit) else
    txnId.flatMap(cluster.reset)

  private lazy val txnId = cluster.newTxn(priority)
  private[this] val writeLocks = new mutable.HashSet[PointerType]()


  private[stm] def write[T <: AnyRef: ClassTag](id: PointerType, value: T)(implicit executionContext: ExecutionContext): Future[Unit] = txnId.flatMap(txnId => {
    readOpt(id).flatMap(prior => {
      if (value != prior.orNull) {
        val lockF = if (writeLocks.contains(id)) {
          Future.successful(Unit)
        } else {
          lock(id).map(success => if (!success) throw new RuntimeException(s"Lock failed: $id in txn $txnId"))
        }
        lockF.map(_ => cluster.queueValue(id, txnId, new ValueType(value)))
      } else {
        Future.successful(Unit)
      }
    })
  })

  val readCache: TrieMap[PointerType, Future[Option[_]]] = new TrieMap()

  private[stm] def readOpt[T <: AnyRef : ClassTag](id: PointerType)(implicit executionContext: ExecutionContext): Future[Option[T]] = {
    readCache.getOrElseUpdate(id,
      txnId.flatMap(txnId => {
        def previousValue: Option[T] = prior.flatMap(_.readCache.get(id)
          .filter(_.isCompleted)
          .map(_.recover({ case _ => None }))
          .flatMap(Await.result(_, 0.millisecond))
          .map(_.asInstanceOf[T]))
        val previousTime: Option[TimeStamp] = prior.map(_.txnId)
          .map(_.recover({ case _ => None }))
          .filter(_.isCompleted)
          .map(Await.result(_, 0.millisecond))
          .map(_.asInstanceOf[TimeStamp])
        cluster.getPtr(id, txnId, previousTime).map(_.flatMap(_.deserialize[T]()).orElse(previousValue))
      })
    ).map(_.map(_.asInstanceOf[T]))
  }

  private[stm] def lock(id: PointerType)(implicit executionContext: ExecutionContext): Future[Boolean] = txnId.flatMap(txnId => {
    cluster.lock(id, txnId)
  }).map(result => {
    if (result.isEmpty) writeLocks += id; result.isEmpty
  })


  override def toString = {
    "txn@" + Option(txnId).filter(_.isCompleted).map(future => Await.result(future, 1.second))
      .map(_.toString).getOrElse("???")
  }
}
