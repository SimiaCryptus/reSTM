package stm.collection

import stm._
import stm.collection.TreeCollection.TreeCollectionNode
import stm.concurrent.Task.{TaskResult, TaskSuccess}
import stm.concurrent.{StmExecutionQueue, Task}
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Random


object TreeCollection {

  case class TreeCollectionNode[T]
  (
    value: T,
    left: Option[STMPtr[TreeCollectionNode[T]]] = None,
    right: Option[STMPtr[TreeCollectionNode[T]]] = None
  ) {
    def apxSize(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Long] = {
      val child = if(Random.nextBoolean()) left.orElse(right) else right.orElse(left)
      child.map(_.read().flatMap(_.apxSize).map(_*2)).getOrElse(Future.successful(1))
    }

    def min()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): T = {
      right.map(_.sync.read.min).getOrElse(value)
    }

    def max()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): T = {
      left.map(_.sync.read.min).getOrElse(value)
    }

    def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): (Option[TreeCollectionNode[T]],T) = {
      val childResult = if(Random.nextBoolean()) {
        left.map(ptr => {
          val (newVal, revVal) = ptr.sync.read.get()
          newVal.map(newVal => {
            ptr.sync <= newVal
            (Option(TreeCollectionNode.this), revVal)
          }).getOrElse({
            (Option(TreeCollectionNode.this.copy(left = None)), revVal)
          })
        }).orElse(right.map(ptr => {
          val (newVal, revVal) = ptr.sync.read.get()
          newVal.map(newVal => {
            ptr.sync <= newVal
            (Option(TreeCollectionNode.this), revVal)
          }).getOrElse({
            (Option(TreeCollectionNode.this.copy(right = None)), revVal)
          })
        }))
      } else {
        right.map(ptr => {
          val (newVal, revVal) = ptr.sync.read.get()
          newVal.map(newVal => {
            ptr.sync <= newVal
            (Option(TreeCollectionNode.this), revVal)
          }).getOrElse({
            (Option(TreeCollectionNode.this.copy(right = None)), revVal)
          })
        }).orElse(left.map(ptr => {
          val (newVal, revVal) = ptr.sync.read.get()
          newVal.map(newVal => {
            ptr.sync <= newVal
            (Option(TreeCollectionNode.this), revVal)
          }).getOrElse({
            (Option(TreeCollectionNode.this.copy(left = None)), revVal)
          })
        }))
      }
      childResult.getOrElse((None, value))
    }

    def +=(newValue: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): TreeCollectionNode[T] = {
      if (Random.nextBoolean()) {
        left.map(leftPtr => {
          leftPtr.sync <= (leftPtr.sync.read += newValue)
          TreeCollectionNode.this
        }).getOrElse({
          this.copy(left = Option(STMPtr.dynamicSync(TreeCollectionNode(newValue))))
        })
      } else {
        right.map(rightPtr => {
          rightPtr.sync <= (rightPtr.sync.read += newValue)
          TreeCollectionNode.this
        }).getOrElse({
          this.copy(right = Option(STMPtr.dynamicSync(TreeCollectionNode(newValue))))
        })
      }
    }

    private def equalityFields = List(value, left, right)

    override def hashCode(): Int = equalityFields.hashCode()

    override def equals(obj: scala.Any): Boolean = obj match {
      case x: TreeCollectionNode[_] => x.equalityFields == equalityFields
      case _ => false
    }

    def sortTask(cluster: Restm, executionContext: ExecutionContext)(implicit ordering: Ordering[T]) : Task[LinkedList[T]] = {
      implicit val _cluster = cluster
      implicit val _executionContext = executionContext
      StmExecutionQueue.atomic.sync.add(sort());
    }

    def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) : Future[Stream[T]] = {
      Future.sequence(List(
        right.map(_.read().flatMap[Stream[T]](x => x.stream()(ctx, executionContext, classTag))).getOrElse(Future.successful(Stream.empty)),
        Future.successful(Stream(value)),
        left.map(_.read().flatMap[Stream[T]](x => x.stream()(ctx, executionContext, classTag))).getOrElse(Future.successful(Stream.empty))
      )).map(_.reduce(_++_))
    }

    def sort()(cluster: Restm, executionContext: ExecutionContext)(implicit ordering: Ordering[T]) : TaskResult[LinkedList[T]] = {
      val tasks: List[Task[LinkedList[T]]] = {
        implicit val _cluster = cluster
        implicit val _executionContext = executionContext
        val leftList: Option[Task[LinkedList[T]]] = left.flatMap(_.atomic.sync.readOpt).map(_.sortTask(cluster,executionContext))
        val rightList: Option[Task[LinkedList[T]]] = right.flatMap(_.atomic.sync.readOpt).map(_.sortTask(cluster,executionContext))
        List(leftList, rightList).filter(_.isDefined).map(_.get)
      }
      new Task.TaskContinue(newFunction = (cluster,executionContext) =>{
        implicit val _cluster = cluster
        implicit val _executionContext = executionContext
        val sources = tasks.map(_.atomic().sync.result())
        def read(list: LinkedList[T]): Option[(T, Option[LinkedList[T]])] = list.atomic().sync.remove().map(_ -> Option(list))
        var cursors = (sources.map(list => read(list)).filter(_.isDefined).map(_.get) ++ List(value->None))
        val result = LinkedList.static[T](new PointerType)
        while(!cursors.isEmpty) {
          val (nextValue, optList) = cursors.minBy(_._1)
          result.atomic().sync.add(nextValue)
          cursors = cursors.filterNot(_._2 == optList)
          cursors = optList.flatMap(list=>read(list)).map(cursors++List(_)).getOrElse(cursors)
        }
        new TaskSuccess(result)
      }, queue = StmExecutionQueue, newTriggers = tasks)
    }

  }

  def apply[T]()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new TreeCollection(STMPtr.dynamicSync[Option[TreeCollectionNode[T]]](None))

}

class TreeCollection[T](val rootPtr: STMPtr[Option[TreeCollectionNode[T]]]) {

  def this(ptr:PointerType) = this(new STMPtr[Option[TreeCollectionNode[T]]](ptr))
  private def this() = this(new PointerType)

  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(key: T) = sync { AtomicApi.this.add(key) }
      def get() = sync { AtomicApi.this.get() }
      def toList()(implicit classTag: ClassTag[T]) = sync { AtomicApi.this.toList() }
      def apxSize() = sync { AtomicApi.this.apxSize() }
      def sort()(implicit ordering: Ordering[T]) = sync { AtomicApi.this.sort() }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)

    def add(key: T) = atomic { TreeCollection.this.add(key)(_,executionContext).map(_ => Unit) }
    def get() = atomic { TreeCollection.this.get()(_,executionContext) }
    def apxSize() = atomic { TreeCollection.this.apxSize()(_,executionContext) }
    def sort()(implicit ordering: Ordering[T]) = atomic { TreeCollection.this.sort()(_,executionContext, ordering) }
    def toList()(implicit classTag: ClassTag[T]) : Future[List[T]] = {
      atomic { (ctx: STMTxnCtx) => {
          TreeCollection.this.stream()(ctx, executionContext, classTag).map(_.toList)
        }
      }
    }

  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority,maxRetries)

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def add(key: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { TreeCollection.this.add(key) }
    def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { TreeCollection.this.get() }
    def apxSize()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { TreeCollection.this.apxSize() }
    def sort()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, ordering: Ordering[T]) = sync { TreeCollection.this.sort() }
    def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = sync { TreeCollection.this.stream() }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def add(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(r => r += value).getOrElse(new TreeCollectionNode[T](value))
    }).flatMap(newRootData => rootPtr.write(Option(newRootData)))
  }

  def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]]  = {
    rootPtr.readOpt().map(_.flatten).flatMap(value=>{
      value.map(_.get()).map(newRootData => {
          rootPtr.write(newRootData._1).map(_ => Option(newRootData._2))
        }).getOrElse(Future.successful(None))
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

  def sort()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, ordering: Ordering[T]) = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(_.sortTask(ctx.cluster, executionContext)(ordering)).get
    })
  }

  def apxSize()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(_.apxSize).get
    })
  }

  def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = {
    rootPtr.readOpt().map(_.flatten).flatMap(x=>{
      x.map(_.stream()).getOrElse(Future.successful(Stream.empty))
    })
  }
}

