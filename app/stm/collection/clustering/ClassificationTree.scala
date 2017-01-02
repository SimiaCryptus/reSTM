package stm.collection.clustering

import stm.collection.BatchedTreeCollection
import stm.collection.clustering.ClassificationTree.{ClassificationTreeItem, LabeledItem, NodeInfo}
import stm.task.Task.{TaskContinue, TaskResult, TaskSuccess}
import stm.task.{StmExecutionQueue, Task}
import stm.{STMPtr, _}
import storage.Restm
import storage.Restm.PointerType
import util.Util

import scala.collection.immutable.Iterable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}


object ClassificationTree {

  def newClassificationTreeNode(parent: Option[STMPtr[ClassificationTreeNode]] = None)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new ClassificationTreeNode(parent, itemBuffer = Option(BatchedTreeCollection[LabeledItem]()))

  def applyStrategy(self: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy): (Restm, ExecutionContext) => TaskResult[Int] =
    (cluster, executionContext) => {
      val task: Future[TaskContinue[Int]] = {
        lazy val stateFuture: Future[Option[ClassificationTreeNode]] = {
          implicit val _executionContext = executionContext
          ClassificationTreeNode.split(self, strategy)(cluster, executionContext)
            .flatMap(_ => self.atomic(cluster, executionContext).readOpt)
        }
        stateFuture.map(_.get)(executionContext).flatMap(newState => {
          new STMTxn[List[(Restm, ExecutionContext) => TaskResult[Int]]] {
            override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[List[(Restm, ExecutionContext) => TaskResult[Int]]] = {
              {
                val values: Map[String, STMPtr[ClassificationTreeNode]] = Map(
                  "pass" -> newState.pass, "fail" -> newState.fail, "exception" -> newState.exception
                ).filter(_._2.isDefined).mapValues(_.get)
                val list: Iterable[Future[Option[(Restm, ExecutionContext) => TaskResult[Int]]]] = values.map((t: (String, STMPtr[ClassificationTreeNode])) => {
                  val child: STMPtr[ClassificationTreeNode] = t._2
                  child.readOpt.map(childValue => {
                    childValue.filter((childState: ClassificationTreeNode) => childState.itemBuffer.forall(_ => {
                      strategy.split(childState.itemBuffer.get)
                    })).map((_: ClassificationTreeNode) => applyStrategy(child, strategy))
                  })
                })
                Future.sequence(list).map(_.filter(_.isDefined).map(_.get))
              }.map(_.toList)
            }
          }.txnRun(cluster)(executionContext)
        })(executionContext).flatMap(list => {
          implicit var _exe = executionContext
          Future.sequence(list.map((x: (Restm, ExecutionContext) => TaskResult[Int]) => {
            StmExecutionQueue.get().atomic(cluster, executionContext).add(x)
          }))
        })(executionContext)
      }.map(tasks => {
        new Task.TaskContinue[Int](newFunction = (cluster, executionContext) => {
          implicit val _cluster = cluster
          implicit val _executionContext = executionContext
          TaskSuccess(Await.result(Future.sequence(tasks.map(_.atomic() result())), 90.seconds).sum)
        }, queue = StmExecutionQueue.get(), newTriggers = tasks)
      })(executionContext)
      Await.result(task, 5.minutes)
    }

  def apply()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new ClassificationTree(STMPtr.dynamicSync(ClassificationTree.newClassificationTreeData()))

  def newClassificationTreeData()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): ClassificationTreeData = {
    ClassificationTreeData(STMPtr.dynamicSync(ClassificationTreeNode(None)), new DefaultClassificationStrategy())
  }

  def apply(id: String) =
    new ClassificationTree(new STMPtr[ClassificationTree.ClassificationTreeData](new PointerType(id)))

  case class ClassificationTreeItem(attributes: Map[String, Any])

  case class LabeledItem(label: String, value: ClassificationTreeItem)

  case class ClassificationTreeData
  (
    root: STMPtr[ClassificationTreeNode],
    strategy: ClassificationStrategy
  ) {

    def find(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[ClassificationTreeNode]] =
      root.read().flatMap(_.find(value).map(_.getOrElse(root)))

    def add(value: List[LabeledItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
      root.read().flatMap(_.add(value, root, strategy).map(_ => Unit))
    }

    private def this() = this(new STMPtr[ClassificationTreeNode](new PointerType), new DefaultClassificationStrategy())

  }

  case class NodeInfo
  (
    node: STMPtr[ClassificationTreeNode],
    treeId: Long,
    parent: Option[NodeInfo] = None
  )
}

class ClassificationTree(val dataPtr: STMPtr[ClassificationTree.ClassificationTreeData]) {

  def this(id: PointerType) = this(new STMPtr[ClassificationTree.ClassificationTreeData](id))

  def atomic(priority: Duration = 0.seconds, maxRetries: Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority, maxRetries)

  def sync(duration: Duration) = new SyncApi(duration)

  def sync = new SyncApi(10.seconds)

  def add(label: String, value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = Util.monitorFuture("ClassificationTree.add") {
    dataPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData()))
        .map(state => state.add(List(LabeledItem(label, value))).map(state -> _)).get
    }).flatMap(newRootData => dataPtr.write(newRootData._1).map(_ => newRootData._2))
  }

  def addAll(label: String, value: List[ClassificationTreeItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = Util.monitorFuture("ClassificationTree.add") {
    dataPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData()))
        .map(state => state.add(value.map(LabeledItem(label, _))).map(state -> _)).get
    }).flatMap(newRootData => dataPtr.write(newRootData._1).map(_ => newRootData._2))
  }

  def setClusterStrategy(strategy: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = Util.monitorFuture("ClassificationTree.setClusterStrategy") {
    dataPtr.readOpt().map(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData()))
        .map(state => state.copy(strategy = strategy))
        .get
    }).flatMap(newRootData => dataPtr.write(newRootData).map(_ => Unit))
  }

  def getClusterStrategy()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[ClassificationStrategy] = Util.monitorFuture("ClassificationTree.getClusterStrategy") {
    dataPtr.readOpt().map(_.map(_.strategy).getOrElse(new DefaultClassificationStrategy))
  }

  def getClusterId(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[ClassificationTreeNode]] = Util.monitorFuture("ClassificationTree.getClusterId") {
    dataPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData())).map(_.find(value)).get
    })
  }

  def getClusterByTreeId(value: Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[ClassificationTreeNode]] = Util.monitorFuture("ClassificationTree.getClusterByTreeId") {
    dataPtr.read().map(_.root).flatMap(root => {
      root.read().flatMap(_.getByTreeId(value, root))
    })
  }

  def getClusterPath(ptr: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[NodeInfo] = Util.monitorFuture("ClassificationTree.getClusterPath") {
    dataPtr.read().map(_.root).flatMap(root => {
      ptr.read().flatMap(_.getInfo(ptr, root))
    })
  }

  def getClusterCount(ptr: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Map[String, Int]] = Util.monitorFuture("ClassificationTree.getClusterCount") {
    ptr.read().map(node => {
      node.stream(ptr).groupBy(_.label).mapValues(_.size)
    })
  }

  def splitTree(strategy: ClassificationStrategy)
               (implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Task[Int]] = {
    dataPtr.readOpt().map(_.orElse(Option(ClassificationTree.newClassificationTreeData()))).flatMap(innerData => Util.monitorFuture("ClassificationTree.splitTree") {
      splitCluster(innerData.get.root, strategy)
    })
  }

  def splitCluster(node: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy)
                  (implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Task[Int]] = Util.monitorFuture("ClassificationTree.splitCluster") {
    StmExecutionQueue.get().add(ClassificationTree.applyStrategy(node, strategy))
  }

  def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Stream[LabeledItem]] = {
    dataPtr.read().map(_.root).flatMap((rootPtr: STMPtr[ClassificationTreeNode]) => {
      rootPtr.read().map(root => {
        root.stream(rootPtr)
      })
    })
  }

  private def this() = this(new PointerType)

  class AtomicApi(priority: Duration = 0.seconds, maxRetries: Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority, maxRetries) {

    def sync(duration: Duration) = new SyncApi(duration)

    def sync = new SyncApi(10.seconds)

    def setClusterStrategy(value: ClassificationStrategy): Future[Unit] = atomic {
      ClassificationTree.this.setClusterStrategy(value)(_, executionContext)
    }

    def getClusterStrategy: Future[ClassificationStrategy] = atomic {
      ClassificationTree.this.getClusterStrategy()(_, executionContext)
    }

    def add(label: String, value: ClassificationTreeItem): Future[Unit] = atomic {
      ClassificationTree.this.add(label, value)(_, executionContext)
    }

    def addAll(label: String, value: List[ClassificationTreeItem]): Future[Unit] = atomic {
      ClassificationTree.this.addAll(label, value)(_, executionContext)
    }

    def getClusterId(value: ClassificationTreeItem): Future[STMPtr[ClassificationTreeNode]] = atomic {
      ClassificationTree.this.getClusterId(value)(_, executionContext)
    }

    def getClusterPath(value: STMPtr[ClassificationTreeNode]): Future[NodeInfo] = atomic {
      ClassificationTree.this.getClusterPath(value)(_, executionContext)
    }

    def getClusterByTreeId(value: Int): Future[STMPtr[ClassificationTreeNode]] = atomic {
      ClassificationTree.this.getClusterByTreeId(value)(_, executionContext)
    }

    def getClusterCount(value: STMPtr[ClassificationTreeNode]): Future[Map[String, Int]] = atomic {
      ClassificationTree.this.getClusterCount(value)(_, executionContext)
    }

    def splitCluster(node: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy): Future[Task[Int]] = atomic {
      ClassificationTree.this.splitCluster(node, strategy)(_, executionContext)
    }

    def splitTree(strategy: ClassificationStrategy): Future[Task[Int]] = atomic {
      ClassificationTree.this.splitTree(strategy)(_, executionContext)
    }

    def stream(): Future[Stream[LabeledItem]] = {
      dataPtr.atomic.read.map(_.root).flatMap((rootPtr: STMPtr[ClassificationTreeNode]) => {
        rootPtr.atomic.read.flatMap((root: ClassificationTreeNode) => {
          root.atomic().stream(rootPtr)
        })
      })
    }

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def getClusterStrategy: ClassificationStrategy = sync {
        AtomicApi.this.getClusterStrategy
      }

      def setClusterStrategy(value: ClassificationStrategy): Unit = sync {
        AtomicApi.this.setClusterStrategy(value)
      }

      def stream(): Stream[LabeledItem] = sync {
        AtomicApi.this.stream()
      }

      def add(label: String, value: ClassificationTreeItem): Unit = sync {
        AtomicApi.this.add(label, value)
      }

      def addAll(label: String, value: List[ClassificationTreeItem]): Unit = sync {
        AtomicApi.this.addAll(label, value)
      }

      def getClusterId(value: ClassificationTreeItem): STMPtr[ClassificationTreeNode] = sync {
        AtomicApi.this.getClusterId(value)
      }

      def getClusterPath(value: STMPtr[ClassificationTreeNode]): NodeInfo = sync {
        AtomicApi.this.getClusterPath(value)
      }

      def getClusterByTreeId(value: Int): STMPtr[ClassificationTreeNode] = sync {
        AtomicApi.this.getClusterByTreeId(value)
      }

      def getClusterCount(value: STMPtr[ClassificationTreeNode]): Map[String, Int] = sync {
        AtomicApi.this.getClusterCount(value)
      }

      def splitCluster(node: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy): Task[Int] = sync {
        AtomicApi.this.splitCluster(node, strategy)
      }

      def splitTree(strategy: ClassificationStrategy): Task[Int] = sync {
        AtomicApi.this.splitTree(strategy)
      }
    }

  }

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def setClusterStrategy(value: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Unit = sync {
      ClassificationTree.this.setClusterStrategy(value)
    }

    def getClusterStrategy()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): ClassificationStrategy = sync {
      ClassificationTree.this.getClusterStrategy()
    }

    def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Stream[LabeledItem] = sync {
      ClassificationTree.this.stream()
    }

    def add(label: String, value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Unit = sync {
      ClassificationTree.this.add(label, value)
    }

    def addAll(label: String, value: List[ClassificationTreeItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Unit = sync {
      ClassificationTree.this.addAll(label, value)
    }

    def getClusterId(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): STMPtr[ClassificationTreeNode] = sync {
      ClassificationTree.this.getClusterId(value)
    }

    def getClusterPath(value: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): NodeInfo = sync {
      ClassificationTree.this.getClusterPath(value)
    }

    def getClusterByTreeId(value: Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): STMPtr[ClassificationTreeNode] = sync {
      ClassificationTree.this.getClusterByTreeId(value)
    }

    def getClusterCount(value: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Map[String, Int] = sync {
      ClassificationTree.this.getClusterCount(value)
    }

    def splitCluster(node: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Task[Int] = sync {
      ClassificationTree.this.splitCluster(node, strategy)
    }

    def splitTree(strategy: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Task[Int] = sync {
      ClassificationTree.this.splitTree(strategy)
    }
  }


}

