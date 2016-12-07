package stm.lib0

import stm.{STMPtr, STMTxn, STMTxnCtx}
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random


object TreeCollection {
  def empty[T] = new STMTxn[TreeCollection[T]] {
    override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[TreeCollection[T]] = create[T]
  }

  def create[T](implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = STMPtr.dynamic[Option[BinaryTreeNode[T]]](None).map(new TreeCollection(_))

  def static[T](id: PointerType) = new TreeCollection(STMPtr.static[Option[BinaryTreeNode[T]]](id, None))


  private case class BinaryTreeNode[T]
  (
    value: T,
    left: Option[STMPtr[BinaryTreeNode[T]]] = None,
    right: Option[STMPtr[BinaryTreeNode[T]]] = None
  ) {

    def min()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): T = {
      right.map(_.sync.read.min).getOrElse(value)
    }

    def max()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): T = {
      left.map(_.sync.read.min).getOrElse(value)
    }

    def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): (Option[BinaryTreeNode[T]],T) = {
      val childResult = if(Random.nextBoolean()) {
        left.map(ptr => {
          val (newVal, revVal) = ptr.sync.read.get()
          newVal.map(newVal => {
            ptr.sync <= newVal
            (Option(BinaryTreeNode.this), revVal)
          }).getOrElse({
            (Option(BinaryTreeNode.this.copy(left = None)), revVal)
          })
        }).orElse(right.map(ptr => {
          val (newVal, revVal) = ptr.sync.read.get()
          newVal.map(newVal => {
            ptr.sync <= newVal
            (Option(BinaryTreeNode.this), revVal)
          }).getOrElse({
            (Option(BinaryTreeNode.this.copy(right = None)), revVal)
          })
        }))
      } else {
        right.map(ptr => {
          val (newVal, revVal) = ptr.sync.read.get()
          newVal.map(newVal => {
            ptr.sync <= newVal
            (Option(BinaryTreeNode.this), revVal)
          }).getOrElse({
            (Option(BinaryTreeNode.this.copy(right = None)), revVal)
          })
        }).orElse(left.map(ptr => {
          val (newVal, revVal) = ptr.sync.read.get()
          newVal.map(newVal => {
            ptr.sync <= newVal
            (Option(BinaryTreeNode.this), revVal)
          }).getOrElse({
            (Option(BinaryTreeNode.this.copy(left = None)), revVal)
          })
        }))
      }
      childResult.getOrElse((None, value))
    }

    def +=(newValue: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): BinaryTreeNode[T] = {
      if (Random.nextBoolean()) {
        left.map(leftPtr => {
          leftPtr.sync <= (leftPtr.sync.read += newValue)
          BinaryTreeNode.this
        }).getOrElse({
          this.copy(left = Option(STMPtr.dynamicSync(BinaryTreeNode(newValue))))
        })
      } else {
        right.map(rightPtr => {
          rightPtr.sync <= (rightPtr.sync.read += newValue)
          BinaryTreeNode.this
        }).getOrElse({
          this.copy(right = Option(STMPtr.dynamicSync(BinaryTreeNode(newValue))))
        })
      }
    }

    private def equalityFields = List(value, left, right)

    override def hashCode(): Int = equalityFields.hashCode()

    override def equals(obj: scala.Any): Boolean = obj match {
      case x: BinaryTreeNode[_] => x.equalityFields == equalityFields
      case _ => false
    }
  }
}
import stm.lib0.TreeCollection._

class TreeCollection[T](rootPtr: STMPtr[Option[BinaryTreeNode[T]]]) {

  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(key: T) = sync { AtomicApi.this.add(key) }
      def get() = sync { AtomicApi.this.get() }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)

    def add(key: T) = atomic { TreeCollection.this.add(key)(_,executionContext).map(_ => Unit) }
    def get() = atomic { TreeCollection.this.get()(_,executionContext) }
  }
  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def add(key: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { TreeCollection.this.add(key) }
    def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { TreeCollection.this.get() }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def add(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(r => r += value).getOrElse(new BinaryTreeNode[T](value))
    }).flatMap(newRootData => rootPtr.write(Option(newRootData)))
  }

  def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]]  = {
    rootPtr.readOpt().map(_.flatten).map(_.map(_.get())
      .map(newRootData => {
        rootPtr.write(newRootData._1)
        newRootData._2
      }))
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

