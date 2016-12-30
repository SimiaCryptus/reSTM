package stm.collection

import stm._
import stm.collection.BatchedTreeCollection._
import storage.Restm
import storage.Restm.PointerType
import storage.types.KryoValue

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Random


object BatchedTreeCollection {

  case class TreeCollectionNode[T]
  (
    value: STMPtr[KryoValue[List[T]]],
    left: Option[STMPtr[TreeCollectionNode[T]]] = None,
    right: Option[STMPtr[TreeCollectionNode[T]]] = None
  ) {

    def apxSize(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Long] = {
      val child = if(Random.nextBoolean()) left.orElse(right) else right.orElse(left)
      child.map(_.read().flatMap(_.apxSize).map(_*2)).getOrElse(value.read().map(_.deserialize()).map(_.size))
    }

    def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[(Option[TreeCollectionNode[T]], List[T])] = {
      val childResult: Option[Future[(Option[TreeCollectionNode[T]], List[T])]] = if(Random.nextBoolean()) {
        left.map(ptr => {
          ptr.read.flatMap(_.get()).flatMap(f => {
            val (newVal, revVal: List[T]) = f
            val result: Future[(Option[TreeCollectionNode[T]], List[T])] = newVal.map(newVal => {
              ptr.write(newVal).map(_ => (Option(TreeCollectionNode.this), revVal))
            }).getOrElse({
              Future.successful((Option(TreeCollectionNode.this.copy(left = None)), revVal))
            })
            result
          })
        }).orElse(right.map(ptr => {
          val result: Future[(Option[TreeCollectionNode[T]], List[T])] = ptr.read.flatMap(_.get()).flatMap(f => {
            val (newVal, revVal: List[T]) = f
            newVal.map(newVal => {
              ptr.write(newVal).map(_ => (Option(TreeCollectionNode.this), revVal))
            }).getOrElse({
              Future.successful((Option(TreeCollectionNode.this.copy(right = None)), revVal))
            })
          })
          result
        }))
      } else {
        right.map(ptr => {
          val result: Future[(Option[TreeCollectionNode[T]], List[T])] = ptr.read.flatMap(_.get()).flatMap(f => {
            val (newVal, revVal: List[T]) = f
            newVal.map(newVal => {
              ptr.write(newVal).map(_ => (Option(TreeCollectionNode.this), revVal))
            }).getOrElse({
              Future.successful((Option(TreeCollectionNode.this.copy(right = None)), revVal))
            })
          })
          result
        }).orElse(left.map(ptr => {
          val result: Future[(Option[TreeCollectionNode[T]], List[T])] = ptr.read.flatMap(_.get()).flatMap(f => {
            val (newVal, revVal: List[T]) = f
            newVal.map(newVal => {
              ptr.write(newVal).map(_ => (Option(TreeCollectionNode.this), revVal))
            }).getOrElse({
              Future.successful((Option(TreeCollectionNode.this.copy(left = None)), revVal))
            })
          })
          result
        }))
      }
      childResult.getOrElse(value.read.map(x=>(None, x.deserialize().get)))
    }

    def +=(newValue: List[T])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[TreeCollectionNode[T]] = {
      if (Random.nextBoolean()) {
        left.map(leftPtr => {
          leftPtr.read.flatMap(_ += newValue).flatMap(leftPtr.write(_)).map(_=>TreeCollectionNode.this)
        }).getOrElse({
          STMPtr.dynamic(KryoValue(newValue)).flatMap(ptr=>STMPtr.dynamic(TreeCollectionNode(ptr))).map(x=>this.copy(left = Option(x)))
        })
      } else {
        right.map(rightPtr => {
          rightPtr.read.flatMap(_ += newValue).flatMap(rightPtr.write(_)).map(_=>TreeCollectionNode.this)
        }).getOrElse({
          STMPtr.dynamic(KryoValue(newValue)).flatMap(ptr=>STMPtr.dynamic(TreeCollectionNode(ptr))).map(x=>this.copy(right = Option(x)))
        })
      }
    }

    private def equalityFields = List(value, left, right)

    override def hashCode(): Int = equalityFields.hashCode()

    override def equals(obj: scala.Any): Boolean = obj match {
      case x: TreeCollectionNode[_] => x.equalityFields == equalityFields
      case _ => false
    }

    def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) : Future[Stream[T]] = {
      Future.sequence(List(
        right.map(_.read().flatMap[Stream[T]](x => x.stream()(ctx, executionContext, classTag))).getOrElse(Future.successful(Stream.empty)),
        value.read().map(_.deserialize().get).map(Stream(_:_*)),
        left.map(_.read().flatMap[Stream[T]](x => x.stream()(ctx, executionContext, classTag))).getOrElse(Future.successful(Stream.empty))
      )).map(_.reduce(_++_))
    }

  }

  def apply[T]()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new BatchedTreeCollection(STMPtr.dynamicSync[Option[TreeCollectionNode[T]]](None))

}

class BatchedTreeCollection[T](val rootPtr: STMPtr[Option[TreeCollectionNode[T]]]) {

  def this(ptr:PointerType) = this(new STMPtr[Option[TreeCollectionNode[T]]](ptr))
  private def this() = this(new PointerType)

  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def get() = sync { AtomicApi.this.get() }
      def toList()(implicit classTag: ClassTag[T]) = sync { AtomicApi.this.toList() }
      def apxSize() = sync { AtomicApi.this.apxSize() }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)

    def get() = atomic { BatchedTreeCollection.this.get()(_,executionContext) }
    def apxSize() = atomic { BatchedTreeCollection.this.apxSize()(_,executionContext) }
    def toList()(implicit classTag: ClassTag[T]) : Future[List[T]] = {
      atomic { (ctx: STMTxnCtx) => {
          BatchedTreeCollection.this.stream()(ctx, executionContext, classTag).map(_.toList)
        }
      }
    }

  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority,maxRetries)

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { BatchedTreeCollection.this.get() }
    def apxSize()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { BatchedTreeCollection.this.apxSize() }
    def size()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = sync { BatchedTreeCollection.this.size() }
    def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = sync { BatchedTreeCollection.this.stream() }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def add(value: List[T])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).flatMap((prev: Option[TreeCollectionNode[T]]) => {
      prev.map(r => r += value).getOrElse(STMPtr.dynamic(KryoValue(value)).map(new TreeCollectionNode[T](_)))
    }).flatMap(newRootData => rootPtr.write(Option(newRootData)))
  }

  def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[List[T]]]  = {
    rootPtr.readOpt().map(_.flatten).flatMap(value=>{
      value.map(_.get()).map(_.flatMap(newRootData => {
        rootPtr.write(newRootData._1).map(_ => Option(newRootData._2))
      })).getOrElse(Future.successful(None))
    })
  }

  def apxSize()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Long] = {
    rootPtr.readOpt().map(_.flatten).flatMap(_.map(_.apxSize).getOrElse(Future.successful(0)))
  }

  def size()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Int] = {
    rootPtr.readOpt().map(_.flatten).flatMap(_.map(_.stream().map(_.size)).getOrElse(Future.successful(0)))
  }

  def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = {
    rootPtr.readOpt().map(_.flatten).flatMap(x=>{
      x.map(_.stream()).getOrElse(Future.successful(Stream.empty))
    })
  }
}

