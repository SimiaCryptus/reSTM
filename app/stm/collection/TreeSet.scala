package stm.collection

import stm._
import stm.collection.TreeSet.TreeSetNode
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


object TreeSet {
  def empty[T <: Comparable[T]] = new STMTxn[TreeSet[T]] {
    override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[TreeSet[T]] = create[T]
  }

  def create[T <: Comparable[T]](implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = STMPtr.dynamic[Option[TreeSetNode[T]]](None).map(new TreeSet(_))


  private case class TreeSetNode[T <: Comparable[T]]
  (
    value: T,
    left: Option[STMPtr[TreeSetNode[T]]] = None,
    right: Option[STMPtr[TreeSetNode[T]]] = None
  ) {

    def min()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): T = {
      right.map(_.sync.read.min).getOrElse(value)
    }

    def max()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): T = {
      left.map(_.sync.read.min).getOrElse(value)
    }

    def -=(newValue: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Option[TreeSetNode[T]] = {
      val compare: Int = value.compareTo(newValue)
      if (compare == 0) {
        if (left.isEmpty && right.isEmpty) {
          None
        } else if (left.isDefined) {
          left.map(leftPtr => {
            val prevNode: TreeSetNode[T] = leftPtr.sync.read
            val newValue: T = prevNode.min()
            val maybeNode: Option[TreeSetNode[T]] = prevNode -= newValue
            maybeNode.map(newNode => {
              leftPtr.sync <= newNode
              TreeSetNode.this.copy(value = newValue)
            }).getOrElse(TreeSetNode.this.copy(left = None, value = newValue))
          })
        } else {
          right.map(rightPtr => {
            val prevNode: TreeSetNode[T] = rightPtr.sync.read
            val newValue: T = prevNode.max()
            val maybeNode: Option[TreeSetNode[T]] = prevNode -= newValue
            maybeNode.map(newNode => {
              rightPtr.sync <= newNode
              TreeSetNode.this.copy(value = newValue)
            }).getOrElse(TreeSetNode.this.copy(right = None, value = newValue))
          })
        }
      } else if (compare < 0) {
        left.map(leftPtr => {
          Option((leftPtr.sync.read -= newValue).map(newLeft => {
            leftPtr.sync <= newLeft
            TreeSetNode.this
          }).getOrElse(TreeSetNode.this.copy(left = None)))
        }).getOrElse({
          throw new NoSuchElementException
        })
      } else {
        right.map(rightPtr => {
          Option((rightPtr.sync.read -= newValue).map(newRight => {
            rightPtr.sync <= newRight
            TreeSetNode.this
          }).getOrElse(TreeSetNode.this.copy(right = None)))
        }).getOrElse({
          throw new NoSuchElementException
        })
      }
    }

    def +=(newValue: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): TreeSetNode[T] = {
      if (value.compareTo(newValue) < 0) {
        left.map(leftPtr => {
          leftPtr.sync <= (leftPtr.sync.read += newValue)
          TreeSetNode.this
        }).getOrElse({
          this.copy(left = Option(STMPtr.dynamicSync(TreeSetNode(newValue))))
        })
      } else {
        right.map(rightPtr => {
          rightPtr.sync <= (rightPtr.sync.read += newValue)
          TreeSetNode.this
        }).getOrElse({
          this.copy(right = Option(STMPtr.dynamicSync(TreeSetNode(newValue))))
        })
      }
    }

    def contains(newValue: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Boolean = {
      if (value.compareTo(newValue) == 0) {
        true
      } else if (value.compareTo(newValue) < 0) {
        left.exists(_.sync.read.contains(newValue))
      } else {
        right.exists(_.sync.read.contains(newValue))
      }
    }

    private def equalityFields = List(value, left, right)

    override def hashCode(): Int = equalityFields.hashCode()

    override def equals(obj: scala.Any): Boolean = obj match {
      case x: TreeSetNode[_] => x.equalityFields == equalityFields
      case _ => false
    }
  }

}

class TreeSet[T <: Comparable[T]](rootPtr: STMPtr[Option[TreeSetNode[T]]]) {

  def this(ptr:PointerType) = this(new STMPtr[Option[TreeSetNode[T]]](ptr))
  private def this() = this(new PointerType)

  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(key: T) = sync { AtomicApi.this.add(key) }
      def remove(value: T) = sync { AtomicApi.this.remove(value) }
      def contains(value: T) = sync { AtomicApi.this.contains(value) }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)

    def add(key: T) = atomic { TreeSet.this.add(key)(_,executionContext).map(_ => Unit) }
    def remove(key: T) = atomic { TreeSet.this.remove(key)(_,executionContext) }
    def contains(key: T) = atomic { TreeSet.this.contains(key)(_,executionContext) }
  }
  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def add(key: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { TreeSet.this.add(key) }
    def remove(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { TreeSet.this.remove(value) }
    def contains(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { TreeSet.this.contains(value) }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def add(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(r => r += value).getOrElse(new TreeSetNode[T](value))
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
