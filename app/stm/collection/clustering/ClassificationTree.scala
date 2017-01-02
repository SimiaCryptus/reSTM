package stm.collection.clustering

import stm.collection.BatchedTreeCollection
import stm.collection.clustering.ClassificationTree.{ClassificationTreeItem, LabeledItem}
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

  case class ClassificationTreeItem(attributes:Map[String,Any])
  case class LabeledItem(label : String, value:ClassificationTreeItem)

  def newClassificationTreeData()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    {
      new ClassificationTreeData(STMPtr.dynamicSync(new ClassificationTreeNode(None)), new DefaultClassificationStrategy())
    }

  case class ClassificationTreeData
  (
    root: STMPtr[ClassificationTreeNode],
    strategy : ClassificationStrategy
  ) {

    private def this() = this(new STMPtr[ClassificationTreeNode](new PointerType), new DefaultClassificationStrategy())

    def find(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[ClassificationTreeNode]] =
      root.read().flatMap(_.find(value).map(_.getOrElse(root)))

    def add(value: List[LabeledItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
      root.read().flatMap(_.add(value, root, strategy, 1).map(_=>Unit))
    }

  }
  def newClassificationTreeNode(parent : Option[STMPtr[ClassificationTreeNode]] = None)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new ClassificationTreeNode(parent, itemBuffer = Option(BatchedTreeCollection[LabeledItem]()))


  def applyStrategy(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy) : (Restm, ExecutionContext) => TaskResult[Int] =
    (cluster, executionContext) => {
      val task: Future[TaskContinue[Int]] = {
        lazy val stateFuture: Future[Option[ClassificationTreeNode]] = {
          implicit val _executionContext = executionContext
          ClassificationTreeNode.split(self, strategy, 0)(cluster, executionContext)
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
                  val (childType, child: STMPtr[ClassificationTreeNode]) = t
                  child.readOpt.map(childValue => {
                    childValue.filter((childState: ClassificationTreeNode) => {
                      childState.itemBuffer.map(itemBuffer => {
                        strategy.split(childState.itemBuffer.get)
                      }).getOrElse(true)
                    }).map((childState: ClassificationTreeNode) => applyStrategy(child, strategy))
                  })
                })
                Future.sequence(list).map(_.filter(_.isDefined).map(_.get))
              }.map(_.toList)
            }
          }.txnRun(cluster)(executionContext)
        })(executionContext).flatMap(list=>{
          implicit var _exe = executionContext
          Future.sequence(list.map((x: (Restm, ExecutionContext) => TaskResult[Int]) => {
            StmExecutionQueue.get().atomic(cluster, executionContext).add(x)
          }))
        })(executionContext)
      }.map(tasks => {
        new Task.TaskContinue[Int](newFunction = (cluster, executionContext) => {
          implicit val _cluster = cluster
          implicit val _executionContext = executionContext
          new TaskSuccess(Await.result(Future.sequence(tasks.map(_.atomic()result())), 90.seconds).sum)
        }, queue = StmExecutionQueue.get(), newTriggers = tasks)
      })(executionContext)
      Await.result(task, 5.minutes)
  }



  case class NodeInfo
  (
    node : STMPtr[ClassificationTreeNode],
    treeId : Long,
    parent : Option[NodeInfo] = None
  )

  def apply()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new ClassificationTree(STMPtr.dynamicSync(ClassificationTree.newClassificationTreeData()))

  def apply(id:String) =
    new ClassificationTree(new STMPtr[ClassificationTree.ClassificationTreeData](new PointerType(id)))
}

class ClassificationTree(val dataPtr: STMPtr[ClassificationTree.ClassificationTreeData]) {

  def this(id : PointerType) = this(new STMPtr[ClassificationTree.ClassificationTreeData](id))
  private def this() = this(new PointerType)


  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def setClusterStrategy(value: ClassificationStrategy) = sync { AtomicApi.this.setClusterStrategy(value) }
      def getClusterStrategy() = sync { AtomicApi.this.getClusterStrategy() }
      def stream() = sync { AtomicApi.this.stream() }
      def add(label:String, value: ClassificationTreeItem) = sync { AtomicApi.this.add(label, value) }
      def addAll(label:String, value: List[ClassificationTreeItem]) = sync { AtomicApi.this.addAll(label, value) }
      def getClusterId(value: ClassificationTreeItem) = sync { AtomicApi.this.getClusterId(value) }
      def getClusterPath(value: STMPtr[ClassificationTreeNode]) = sync { AtomicApi.this.getClusterPath(value) }
      def getClusterByTreeId(value: Int) = sync { AtomicApi.this.getClusterByTreeId(value) }
      def getClusterCount(value: STMPtr[ClassificationTreeNode]) = sync { AtomicApi.this.getClusterCount(value) }
      def splitCluster(node : STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy) = sync { AtomicApi.this.splitCluster(node, strategy) }
      def splitTree(strategy: ClassificationStrategy) = sync { AtomicApi.this.splitTree(strategy) }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)

    def setClusterStrategy(value: ClassificationStrategy) = atomic { ClassificationTree.this.setClusterStrategy(value)(_,executionContext) }
    def getClusterStrategy() = atomic { ClassificationTree.this.getClusterStrategy()(_,executionContext) }
    def add(label:String, value: ClassificationTreeItem) = atomic { ClassificationTree.this.add(label, value)(_,executionContext) }
    def addAll(label:String, value: List[ClassificationTreeItem]) = atomic { ClassificationTree.this.addAll(label, value)(_,executionContext) }
    def getClusterId(value: ClassificationTreeItem) = atomic { ClassificationTree.this.getClusterId(value)(_,executionContext) }
    def getClusterPath(value: STMPtr[ClassificationTreeNode]) = atomic { ClassificationTree.this.getClusterPath(value)(_,executionContext) }
    def getClusterByTreeId(value: Int) = atomic { ClassificationTree.this.getClusterByTreeId(value)(_,executionContext) }
    def getClusterCount(value: STMPtr[ClassificationTreeNode]) = atomic { ClassificationTree.this.getClusterCount(value)(_,executionContext) }
    def splitCluster(node : STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy) = atomic { ClassificationTree.this.splitCluster(node, strategy)(_,executionContext) }
    def splitTree(strategy: ClassificationStrategy) = atomic { ClassificationTree.this.splitTree(strategy)(_,executionContext) }


    def stream(): Future[Stream[LabeledItem]] = {
      dataPtr.atomic.read.map(_.root).flatMap((rootPtr: STMPtr[ClassificationTreeNode]) =>{
        rootPtr.atomic.read.flatMap((root: ClassificationTreeNode) =>{
          root.atomic().stream(rootPtr)
        })
      })
    }

  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority,maxRetries)

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def setClusterStrategy(value: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.setClusterStrategy(value) }
    def getClusterStrategy()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterStrategy() }
    def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.stream() }
    def add(label:String, value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.add(label, value) }
    def addAll(label:String, value: List[ClassificationTreeItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.addAll(label, value) }
    def getClusterId(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterId(value) }
    def getClusterPath(value: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterPath(value) }
    def getClusterByTreeId(value: Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterByTreeId(value) }
    def getClusterCount(value: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterCount(value) }
    def splitCluster(node : STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.splitCluster(node, strategy) }
    def splitTree(strategy: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.splitTree(strategy) }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def add(label:String, value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = Util.monitorFuture("ClassificationTree.add") {
    dataPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData()))
        .map(state => state.add(List(new LabeledItem(label, value))).map(state -> _)).get
    }).flatMap(newRootData => dataPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def addAll(label:String, value: List[ClassificationTreeItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = Util.monitorFuture("ClassificationTree.add") {
    dataPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData()))
        .map(state => state.add(value.map(new LabeledItem(label, _))).map(state -> _)).get
    }).flatMap(newRootData => dataPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def setClusterStrategy(strategy: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Unit] = Util.monitorFuture("ClassificationTree.setClusterStrategy") {
    dataPtr.readOpt().map(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData()))
        .map(state=>state.copy(strategy = strategy))
        .get
    }).flatMap(newRootData => dataPtr.write((newRootData)).map(_=>Unit))
  }

  def getClusterStrategy()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[ClassificationStrategy] = Util.monitorFuture("ClassificationTree.getClusterStrategy") {
    dataPtr.readOpt().map(_.map(_.strategy).getOrElse(new DefaultClassificationStrategy))
  }

  def getClusterId(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[STMPtr[ClassificationTreeNode]] = Util.monitorFuture("ClassificationTree.getClusterId") {
    dataPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData())).map(_.find(value)).get
    })
  }

  def getClusterByTreeId(value: Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[ClassificationTreeNode]] = Util.monitorFuture("ClassificationTree.getClusterByTreeId") {
    dataPtr.read().map(_.root).flatMap(root => {
      root.read().flatMap(_.getByTreeId(value, root))
    })
  }

  def getClusterPath(ptr: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = Util.monitorFuture("ClassificationTree.getClusterPath") {
    dataPtr.read().map(_.root).flatMap(root=>{
      ptr.read().flatMap(_.getInfo(ptr, root))
    })
  }

  def getClusterCount(ptr: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Map[String, Int]] = Util.monitorFuture("ClassificationTree.getClusterCount") {
    ptr.read().map(node=>{
      node.stream(ptr).groupBy(_.label).mapValues(_.size)
    })
  }

  def splitCluster(node : STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy)
                    (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = Util.monitorFuture("ClassificationTree.splitCluster") {
    StmExecutionQueue.get().add(ClassificationTree.applyStrategy(node, strategy))
  }

  def splitTree(strategy: ClassificationStrategy)
                 (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    dataPtr.readOpt().map(_.orElse(Option(ClassificationTree.newClassificationTreeData()))).flatMap(innerData => Util.monitorFuture("ClassificationTree.splitTree") {
      splitCluster(innerData.get.root, strategy)
    })
  }

  def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Stream[LabeledItem]] = {
    dataPtr.read().map(_.root).flatMap((rootPtr: STMPtr[ClassificationTreeNode]) =>{
      rootPtr.read().map(root=>{
        root.stream(rootPtr)
      })
    })
  }


}

