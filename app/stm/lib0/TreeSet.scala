package stm.lib0

import stm.{STMPtr, STMTxn, STMTxnCtx}
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}


object TreeSet {
  def empty[T <: Comparable[T]] = new STMTxn[TreeSet[T]] {
    override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[TreeSet[T]] = create[T]
  }

  def create[T <: Comparable[T]](implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = STMPtr.dynamic[Option[BinaryTreeNode[T]]](None).map(new TreeSet(_))

  def static[T <: Comparable[T]](id: PointerType) = new TreeSet(STMPtr.static[Option[BinaryTreeNode[T]]](id, None))
}

class TreeSet[T <: Comparable[T]](rootPtr: STMPtr[Option[BinaryTreeNode[T]]]) {

  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) {

    class SyncApi(duration: Duration) {
      def add(value: T) = Await.result(AtomicApi.this.add(value), duration)

      def remove(value: T) = Await.result(AtomicApi.this.remove(value), duration)

      def contains(value: T) = Await.result(AtomicApi.this.contains(value), duration)
    }

    def sync(duration: Duration) = new SyncApi(duration)

    def sync = new SyncApi(10.seconds)

    def add(value: T) = new STMTxn[Unit] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] =
        TreeSet.this.add(value).map(_ => Unit)
    }.txnRun(cluster)(executionContext)

    def remove(value: T) = new STMTxn[Unit] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] =
        TreeSet.this.remove(value).map(_ => Unit)
    }.txnRun(cluster)(executionContext)

    def contains(value: T) = new STMTxn[Boolean] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Boolean] =
        TreeSet.this.contains(value)
    }.txnRun(cluster)(executionContext)
  }

  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi

  def add(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(r => r += value).getOrElse(new BinaryTreeNode[T](value))
    }).flatMap(newRootData => rootPtr.write(Option(newRootData)))
  }

  def remove(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.flatMap(r => r -= value)
    }).flatMap(newRootData => rootPtr.write(newRootData))
  }

  def contains(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.exists(_.contains(value))
    })
  }

  def min(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(_.min()).getOrElse(None)
    })
  }

  def max(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(_.max()).getOrElse(None)
    })
  }
}

private case class BinaryTreeNode[T <: Comparable[T]]
(
  value: T,
  left: Option[STMPtr[BinaryTreeNode[T]]] = None,
  right: Option[STMPtr[BinaryTreeNode[T]]] = None
) {

  def min()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): T = {
    right.map(_.sync.get.min).getOrElse(value)
  }

  def max()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): T = {
    left.map(_.sync.get.min).getOrElse(value)
  }

  def -=(newValue: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Option[BinaryTreeNode[T]] = {
    val compare: Int = value.compareTo(newValue)
    if (compare == 0) {
      if (left.isEmpty && right.isEmpty) {
        None
      } else if (left.isDefined) {
        left.map(leftPtr => {
          val prevNode: BinaryTreeNode[T] = leftPtr.sync.get
          val newValue: T = prevNode.min()
          val maybeNode: Option[BinaryTreeNode[T]] = prevNode -= newValue
          maybeNode.map(newNode => {
            leftPtr.sync <= newNode
            BinaryTreeNode.this.copy(value = newValue)
          }).getOrElse(BinaryTreeNode.this.copy(left = None, value = newValue))
        })
      } else {
        right.map(rightPtr => {
          val prevNode: BinaryTreeNode[T] = rightPtr.sync.get
          val newValue: T = prevNode.max()
          val maybeNode: Option[BinaryTreeNode[T]] = prevNode -= newValue
          maybeNode.map(newNode => {
            rightPtr.sync <= newNode
            BinaryTreeNode.this.copy(value = newValue)
          }).getOrElse(BinaryTreeNode.this.copy(right = None, value = newValue))
        })
      }
    } else if (compare < 0) {
      left.map(leftPtr => {
        Option((leftPtr.sync.get -= newValue).map(newLeft => {
          leftPtr.sync <= newLeft
          BinaryTreeNode.this
        }).getOrElse(BinaryTreeNode.this.copy(left = None)))
      }).getOrElse({
        throw new NoSuchElementException
      })
    } else {
      right.map(rightPtr => {
        Option((rightPtr.sync.get -= newValue).map(newRight => {
          rightPtr.sync <= newRight
          BinaryTreeNode.this
        }).getOrElse(BinaryTreeNode.this.copy(right = None)))
      }).getOrElse({
        throw new NoSuchElementException
      })
    }
  }

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
