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
    parent: Option[STMPtr[TreeCollectionNode[T]]],
    value: STMPtr[KryoValue[List[T]]],
    left: Option[STMPtr[TreeCollectionNode[T]]] = None,
    right: Option[STMPtr[TreeCollectionNode[T]]] = None
  ) {

    def apxSize(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Long] = {
      val child = if(Random.nextBoolean()) left.orElse(right) else right.orElse(left)
      child.map(_.read().flatMap(_.apxSize).map(_*2)).getOrElse(value.read().map(_.deserialize()).map(_.size))
    }

    def get(self : STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[List[T]] = {
      val (a,b) = if(Random.nextBoolean()) (left,right) else (right,left)
      a.map(ptr => {
        ptr.read.flatMap(_.get(ptr))
      }).orElse(b.map(ptr => {
        ptr.read.flatMap(_.get(ptr))
      })).getOrElse({
        unlinkParent(self)
          .flatMap(_ => self.delete())
          .flatMap(_ => value.read.map(_.deserialize().get))
      })
    }

    private def unlinkParent[T](self: STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
      parent.map(parentPtr => {
        parentPtr.read().flatMap(parentValue => {
          if (parentValue.left == Option(self)) {
            parentPtr.write(parentValue.copy(left = None))
          } else if (parentValue.right == Option(self)) {
            parentPtr.write(parentValue.copy(right = None))
          } else {
            throw new RuntimeException("Child Link Not Found")
          }
        })
      }).getOrElse(Future.successful(Unit))
    }

    def add(newValue: List[T], self : STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
      if (Random.nextBoolean()) {
        left.map(leftPtr => {
          leftPtr.read.flatMap(_.add(newValue, leftPtr))
        }).getOrElse({
          STMPtr.dynamic(KryoValue(newValue))
            .flatMap(ptr => STMPtr.dynamic(new TreeCollectionNode(Some(self), ptr)))
            .flatMap(x =>self.write(this.copy(left = Option(x))))
        })
      } else {
        right.map(rightPtr => {
          rightPtr.read.flatMap(_.add(newValue, rightPtr))
        }).getOrElse({
          STMPtr.dynamic(KryoValue(newValue))
            .flatMap(ptr => STMPtr.dynamic(new TreeCollectionNode(Some(self), ptr)))
            .flatMap(x =>self.write(this.copy(right = Option(x))))
        })
      }
    }

    private def equalityFields = List(value, left, right)

    override def hashCode(): Int = equalityFields.hashCode()

    override def equals(obj: scala.Any): Boolean = obj match {
      case x: TreeCollectionNode[_] => x.equalityFields == equalityFields
      case _ => false
    }

    def stream(self : STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) : Future[Stream[T]] = {
      def rightFuture = right.map(rightPtr=>rightPtr.read().flatMap[Stream[T]](x => x.stream(rightPtr)(ctx, executionContext, classTag))).getOrElse(Future.successful(Stream.empty))
      def leftFuture = left.map(leftPtr=>leftPtr.read().flatMap[Stream[T]](x => x.stream(leftPtr)(ctx, executionContext, classTag))).getOrElse(Future.successful(Stream.empty))
      def thisFuture: Future[Stream[T]] = value.read().map(_.deserialize().get).map(Stream(_: _*))
      rightFuture.flatMap(right=>{
        leftFuture.flatMap(left=>{
          thisFuture.map(center=>{
            List(left, center, right).reduce(_++_)
          })
        })
      })
    }



    def getByTreeId(cursor: Int, self : STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[STMPtr[TreeCollectionNode[T]]] = {
      require(0 <= cursor)
      if(0 == cursor) {
        Future.successful(self)
      } else {
        value.read().map(_.deserialize().get).flatMap(itemBuffer=>{
          val childCursor: Int = Math.floorDiv(cursor, 2)
          val cursorBit: Int = cursor % 2
          cursorBit match {
            case 0 =>
              left.get.read().flatMap(_.getByTreeId(childCursor, left.get))
            case 1 =>
              right.get.read().flatMap(_.getByTreeId(childCursor, right.get))
          }
        })
      }
    }

    def getTreeBit(self:STMPtr[TreeCollectionNode[T]], node:STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Int = {
      if(left.filter(_==node).isDefined) 0
      else if(right.filter(_==node).isDefined) 1
      else throw new RuntimeException()
    }

    def getTreeId(self:STMPtr[TreeCollectionNode[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[(Int,Int)] = {
      parent.map(_.read().flatMap(parentNode => {
        parentNode.getTreeId(parent.get).map(x=>{
          val (depth:Int,parentId:Int) = x
          val bit: Int = parentNode.getTreeBit(parent.get, self)
          val tuple: (Int, Int) = (depth + 1) -> (parentId+Math.pow(2,depth).toInt*bit)
          tuple
        })
      })).getOrElse(Future.successful(0->0))
    }


  }

  def apply[T]()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new BatchedTreeCollection(STMPtr.dynamicSync[TreeCollectionNode[T]](null))

}

class BatchedTreeCollection[T](val rootPtr: STMPtr[TreeCollectionNode[T]]) {

  def this(ptr:PointerType) = this(new STMPtr[TreeCollectionNode[T]](ptr))
  private def this() = this(new PointerType)

  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def get() = sync { AtomicApi.this.get() }
      def toList()(implicit classTag: ClassTag[T]) = sync { AtomicApi.this.toList() }
      def apxSize() = sync { AtomicApi.this.apxSize() }
      def add(value: List[T]) = sync { AtomicApi.this.add(value) }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)

    def get() = atomic { BatchedTreeCollection.this.get()(_,executionContext) }
    def add(value: List[T]) = atomic { BatchedTreeCollection.this.add(value)(_,executionContext) }
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
    def add(value: List[T])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = sync { BatchedTreeCollection.this.add(value) }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def add(value: List[T])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
    rootPtr.readOpt().flatMap(rootOpt=>{
      rootOpt.map(root => root.add(value, rootPtr))
        .getOrElse({
          STMPtr.dynamic(KryoValue(value))
            .map(new TreeCollectionNode[T](None, _))
            .flatMap(rootPtr.write(_))
        })
    })
  }

  def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[List[T]]]  = {
    rootPtr.readOpt().flatMap(rootOpt=>{
      rootOpt.map(root=>root.get(rootPtr).map(Option(_)))
        .getOrElse(Future.successful(None))
    })
  }

  def apxSize()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Long] = {
    rootPtr.readOpt().flatMap(_.map(_.apxSize).getOrElse(Future.successful(0)))
  }

  def size()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Int] = {
    rootPtr.readOpt().flatMap(_.map(_.stream(rootPtr).map(_.size)).getOrElse(Future.successful(0)))
  }

  def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = {
    rootPtr.readOpt().flatMap(_.map(_.stream(rootPtr)).getOrElse(Future.successful(Stream.empty)))
  }
}

