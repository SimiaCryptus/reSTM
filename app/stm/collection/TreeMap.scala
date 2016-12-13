package stm.collection

import stm._
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object TreeMap {
  def empty[T <: Comparable[T],V] = new STMTxn[TreeMap[T,V]] {
    override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[TreeMap[T,V]] = create[T,V]
  }

  def create[T <: Comparable[T],V](implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = STMPtr.dynamic[Option[BinaryTreeMapNode[T,V]]](None).map(new TreeMap(_))

  def static[T <: Comparable[T],V](id: PointerType) = new TreeMap(STMPtr.static[Option[BinaryTreeMapNode[T,V]]](id, None))
}

class TreeMap[T <: Comparable[T],V](rootPtr: STMPtr[Option[BinaryTreeMapNode[T,V]]]) {

  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(key: T, value: V) = sync { AtomicApi.this.add(key, value) }
      def remove(value: T) = sync { AtomicApi.this.remove(value) }
      def contains(value: T) = sync { AtomicApi.this.contains(value) }
      def get(value: T) = sync { AtomicApi.this.get(value) }
    }

    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)

    def add(key: T, value: V) = atomic { TreeMap.this.add(key, value)(_,executionContext).map(_ => Unit) }
    def remove(key: T) = atomic { TreeMap.this.remove(key)(_,executionContext) }
    def contains(key: T) = atomic { TreeMap.this.contains(key)(_,executionContext) }
    def get(key: T) = atomic { TreeMap.this.get(key)(_,executionContext) }
  }

  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi

  def add(key: T, value: V)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(r => r += key -> value).getOrElse(new BinaryTreeMapNode[T,V](key, value))
    }).flatMap(newRootData => rootPtr.write(Option(newRootData)))
  }

  def remove(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(_.flatMap(r => r -= value))
      .flatMap(newRootData => rootPtr.write(newRootData))
  }

  def contains(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(_.exists(_.contains(value)))
  }

  def get(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Option[V]] = {
    rootPtr.readOpt().map(_.flatten).flatMap(_.map(_.get(value)).getOrElse(Future.successful(None)))
  }

  def min(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(_.map(_.min()).getOrElse(None))
  }

  def max(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(_.map(_.max()).getOrElse(None))
  }
}

private case class BinaryTreeMapNode[T <: Comparable[T],V]
(
  key: T,
  value: V,
  left: Option[STMPtr[BinaryTreeMapNode[T,V]]] = None,
  right: Option[STMPtr[BinaryTreeMapNode[T,V]]] = None
) {

  def min()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): BinaryTreeMapNode[T,V] = {
    right.map(_.sync.read.min).getOrElse(this)
  }

  def max()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): BinaryTreeMapNode[T,V] = {
    left.map(_.sync.read.min).getOrElse(this)
  }

  def -=(toRemove: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Option[BinaryTreeMapNode[T,V]] = {
    val compare: Int = key.compareTo(toRemove)
    if (compare == 0) {
      if (left.isEmpty && right.isEmpty) {
        None
      } else if (left.isDefined) {
        left.map(leftPtr => {
          val prevNode: BinaryTreeMapNode[T,V] = leftPtr.sync.read
          val promotedNode = prevNode.min()
          val maybeNode: Option[BinaryTreeMapNode[T,V]] = prevNode -= promotedNode.key
          maybeNode.map(newNode => {
            leftPtr.sync <= newNode
            BinaryTreeMapNode.this.copy(key = promotedNode.key, value = promotedNode.value)
          }).getOrElse(BinaryTreeMapNode.this.copy(left = None, key = promotedNode.key, value = promotedNode.value))
        })
      } else {
        right.map(rightPtr => {
          val prevNode: BinaryTreeMapNode[T,V] = rightPtr.sync.read
          val promotedNode = prevNode.max()
          val maybeNode: Option[BinaryTreeMapNode[T,V]] = prevNode -= promotedNode.key
          maybeNode.map(newNode => {
            rightPtr.sync <= newNode
            BinaryTreeMapNode.this.copy(key = promotedNode.key, value = promotedNode.value)
          }).getOrElse(BinaryTreeMapNode.this.copy(right = None, key = promotedNode.key, value = promotedNode.value))
        })
      }
    } else if (compare < 0) {
      left.map(leftPtr => {
        Option((leftPtr.sync.read -= toRemove).map(newLeft => {
          leftPtr.sync <= newLeft
          BinaryTreeMapNode.this
        }).getOrElse(BinaryTreeMapNode.this.copy(left = None)))
      }).getOrElse({
        throw new NoSuchElementException
      })
    } else {
      right.map(rightPtr => {
        Option((rightPtr.sync.read -= toRemove).map(newRight => {
          rightPtr.sync <= newRight
          BinaryTreeMapNode.this
        }).getOrElse(BinaryTreeMapNode.this.copy(right = None)))
      }).getOrElse({
        throw new NoSuchElementException
      })
    }
  }

  def +=(newValue: (T,V))(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): BinaryTreeMapNode[T,V] = {
    if (key.compareTo(newValue._1) < 0) {
      left.map(leftPtr => {
        leftPtr.sync <= (leftPtr.sync.read += newValue)
        BinaryTreeMapNode.this
      }).getOrElse({
        this.copy(left = Option(STMPtr.dynamicSync(BinaryTreeMapNode(newValue._1, newValue._2))))
      })
    } else {
      right.map(rightPtr => {
        rightPtr.sync <= (rightPtr.sync.read += newValue)
        BinaryTreeMapNode.this
      }).getOrElse({
        this.copy(right = Option(STMPtr.dynamicSync(BinaryTreeMapNode(newValue._1, newValue._2))))
      })
    }
  }

  def contains(newValue: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Boolean = {
    if (key.compareTo(newValue) == 0) {
      true
    } else if (key.compareTo(newValue) < 0) {
      left.exists(_.sync.read.contains(newValue))
    } else {
      right.exists(_.sync.read.contains(newValue))
    }
  }

  def get(newValue: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[V]] = {
    if (key.compareTo(newValue) == 0) {
      Future.successful(Option(value))
    } else if (key.compareTo(newValue) < 0) {
      left.map(_.read().flatMap(_.get(newValue))).getOrElse(Future.successful(None))
    } else {
      right.map(_.read().flatMap(_.get(newValue))).getOrElse(Future.successful(None))
    }
  }

  private def equalityFields = List(key, left, right)

  override def hashCode(): Int = equalityFields.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: BinaryTreeMapNode[_,_] => x.equalityFields == equalityFields
    case _ => false
  }
}
