package stm

import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}


object SimpleSet {
  def empty[T <: Comparable[T]] = new STMTxn[SimpleSet[T]] {
    override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[SimpleSet[T]] = create[T]
  }
  def create[T <: Comparable[T]](implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = STMPtr.dynamic[Option[BinaryTreeNode[T]]](None).map(new SimpleSet(_))

  def static[T <: Comparable[T]](id:PointerType) = new SimpleSet(STMPtr.static[Option[BinaryTreeNode[T]]](id, None))
}

class SimpleSet[T <: Comparable[T]](rootPtr : STMPtr[Option[BinaryTreeNode[T]]]) {
  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) {

    class SyncApi(duration:Duration) {
      def add(value: T) = Await.result(AtomicApi.this.add(value), duration)
      def contains(value: T) = Await.result(AtomicApi.this.contains(value), duration)
    }
    def sync(duration:Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)

    def add(value: T) = new STMTxn[Unit] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] =
        SimpleSet.this.add(value).map(_=>Unit)
    }.txnRun(cluster)(executionContext)

    def contains(value: T) = new STMTxn[Boolean] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Boolean] =
        SimpleSet.this.contains(value)
    }.txnRun(cluster)(executionContext)
  }
  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi

  def add(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatMap(x => x)).map(prev => {
      prev.map(r => r += value).getOrElse(new BinaryTreeNode[T](value))
    }).flatMap(newRootData=>rootPtr.write(Option(newRootData)))
  }
  def contains(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatMap(x => x)).map(prev => {
      prev.map(_.contains(value)).getOrElse(false)
    })
  }
}

private case class BinaryTreeNode[T <: Comparable[T]]
(
  value: T,
  left: Option[STMPtr[BinaryTreeNode[T]]] = None,
  right: Option[STMPtr[BinaryTreeNode[T]]] = None
) {

  def +=(newValue: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): BinaryTreeNode[T] = {
    if (value.compareTo(newValue) < 0) {
      left.map(leftPtr => {
        leftPtr.sync <= (leftPtr.sync.get += newValue)
        BinaryTreeNode.this
      }).getOrElse({
        this.copy(left = Option(STMPtr.dynamicSync(BinaryTreeNode(newValue))))
      })
    } else {
      right.map(rightPtr => {
        rightPtr.sync <= (rightPtr.sync.get += newValue)
        BinaryTreeNode.this
      }).getOrElse({
        this.copy(right = Option(STMPtr.dynamicSync(BinaryTreeNode(newValue))))
      })
    }
  }

  def contains(newValue: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Boolean = {
    if (value.compareTo(newValue) == 0) {
      true
    } else if (value.compareTo(newValue) < 0) {
      left.exists(_.sync.get.contains(newValue))
    } else {
      right.exists(_.sync.get.contains(newValue))
    }
  }

  private def equalityFields = List(value, left, right)

  override def hashCode(): Int = equalityFields.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: BinaryTreeNode[_] => x.equalityFields == equalityFields
    case _ => false
  }
}
