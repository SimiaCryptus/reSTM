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


  def applyStrategy(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy)(implicit executionContext: ExecutionContext) : (Restm, ExecutionContext) => TaskResult[Int] =
    (cluster, executionContext) => {
      val task: Future[TaskContinue[Int]] = {
        lazy val stateFuture: Future[Option[ClassificationTreeNode]] = {
          implicit val _executionContext = executionContext
          self.atomic(cluster, executionContext).read
            .flatMap(_.atomic()(cluster, executionContext).split(self, strategy, 0))
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
            StmExecutionQueue.atomic(cluster, executionContext).add(x)
          }))
        })(executionContext)
      }.map(tasks => {
        new Task.TaskContinue[Int](newFunction = (cluster, executionContext) => {
          implicit val _cluster = cluster
          implicit val _executionContext = executionContext
          new TaskSuccess(Await.result(Future.sequence(tasks.map(_.atomic()result())), 90.seconds).sum)
        }, queue = StmExecutionQueue, newTriggers = tasks)
      })(executionContext)
      Await.result(task, 60.seconds)
  }



  case class NodeInfo
  (
    node : STMPtr[ClassificationTreeNode],
    treeId : Int,
    parent : Option[NodeInfo] = None
  )

  def apply()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new ClassificationTree(STMPtr.dynamicSync(ClassificationTree.newClassificationTreeData()))

  def apply(id:String) =
    new ClassificationTree(new STMPtr[ClassificationTree.ClassificationTreeData](new PointerType(id)))
}

class ClassificationTree(rootPtr: STMPtr[ClassificationTree.ClassificationTreeData]) {

  def this(id : PointerType) = this(new STMPtr[ClassificationTree.ClassificationTreeData](id))
  private def this() = this(new PointerType)


  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def setClusterStrategy(value: ClassificationStrategy) = sync { AtomicApi.this.setClusterStrategy(value) }
      def getClusterStrategy() = sync { AtomicApi.this.getClusterStrategy() }
      def add(label:String, value: ClassificationTreeItem) = sync { AtomicApi.this.add(label, value) }
      def addAll(label:String, value: List[ClassificationTreeItem]) = sync { AtomicApi.this.addAll(label, value) }
      def getClusterId(value: ClassificationTreeItem) = sync { AtomicApi.this.getClusterId(value) }
      def getClusterPath(value: STMPtr[ClassificationTreeNode]) = sync { AtomicApi.this.getClusterPath(value) }
      def getClusterByTreeId(value: Int) = sync { AtomicApi.this.getClusterByTreeId(value) }
      def getClusterCount(value: STMPtr[ClassificationTreeNode]) = sync { AtomicApi.this.getClusterCount(value) }
      def iterateCluster(node : STMPtr[ClassificationTreeNode], max: Int = 100, cursor : Int = 0) = sync { AtomicApi.this.iterateCluster(node, max, cursor) }
      def iterateTree(max: Int = 100, cursor : Int = 0) = sync { AtomicApi.this.iterateTree(max, cursor) }
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
    def iterateCluster(node : STMPtr[ClassificationTreeNode], max: Int = 100, cursor : Int = 0) = atomic { ClassificationTree.this.iterateCluster(node, max, cursor)(_,executionContext) }
    def iterateTree(max: Int = 100, cursor : Int = 0) = atomic { ClassificationTree.this.iterateTree(max, cursor)(_,executionContext) }
    def splitCluster(node : STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy) = atomic { ClassificationTree.this.splitCluster(node, strategy)(_,executionContext) }
    def splitTree(strategy: ClassificationStrategy) = atomic { ClassificationTree.this.splitTree(strategy)(_,executionContext) }
  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority,maxRetries)

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def setClusterStrategy(value: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.setClusterStrategy(value) }
    def getClusterStrategy()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterStrategy() }
    def add(label:String, value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.add(label, value) }
    def addAll(label:String, value: List[ClassificationTreeItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.addAll(label, value) }
    def getClusterId(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterId(value) }
    def getClusterPath(value: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterPath(value) }
    def getClusterByTreeId(value: Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterByTreeId(value) }
    def getClusterCount(value: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterCount(value) }
    def splitCluster(node : STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.splitCluster(node, strategy) }
    def splitTree(strategy: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.splitTree(strategy) }
    def iterateCluster(node : STMPtr[ClassificationTreeNode], value: PointerType, max: Int = 100, cursor : Int = 0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.iterateCluster(node, max, cursor) }
    def iterateTree(value: PointerType, max: Int = 100, cursor : Int = 0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.iterateTree(max, cursor) }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def add(label:String, value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = Util.monitorFuture("ClassificationTree.add") {
    rootPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData()))
        .map(state => state.add(List(new LabeledItem(label, value))).map(state -> _)).get
    }).flatMap(newRootData => rootPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def addAll(label:String, value: List[ClassificationTreeItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = Util.monitorFuture("ClassificationTree.add") {
    rootPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData()))
        .map(state => state.add(value.map(new LabeledItem(label, _))).map(state -> _)).get
    }).flatMap(newRootData => rootPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def setClusterStrategy(strategy: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Unit] = Util.monitorFuture("ClassificationTree.setClusterStrategy") {
    rootPtr.readOpt().map(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData()))
        .map(state=>state.copy(strategy = strategy))
        .get
    }).flatMap(newRootData => rootPtr.write((newRootData)).map(_=>Unit))
  }

  def getClusterStrategy()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[ClassificationStrategy] = Util.monitorFuture("ClassificationTree.getClusterStrategy") {
    rootPtr.readOpt().map(_.map(_.strategy).getOrElse(new DefaultClassificationStrategy))
  }

  def getClusterId(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[STMPtr[ClassificationTreeNode]] = Util.monitorFuture("ClassificationTree.getClusterId") {
    rootPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData())).map(state=>
        state.find(value).map(state -> _)).get
    }).flatMap(newRootData => rootPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def getClusterByTreeId(value: Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[ClassificationTreeNode]] = Util.monitorFuture("ClassificationTree.getClusterByTreeId") {
    rootPtr.read().map(_.root).flatMap(root => {
      root.read().flatMap(_.getByTreeId(value, root))
    })
  }

  def getClusterPath(ptr: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = Util.monitorFuture("ClassificationTree.getClusterPath") {
    ptr.read().flatMap(_.getInfo(ptr))
  }

  def getClusterCount(ptr: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = Util.monitorFuture("ClassificationTree.getClusterCount") {
    ptr.read().map(_.iterateClusterMembers(max=100,cursor=0,ptr)).map(r=>{
      require(-1 == r._1)
      r._2.groupBy(_.label).mapValues(_.size)
    })
  }

  def iterateCluster(node : STMPtr[ClassificationTreeNode], max: Int = 100, cursor : Int = 0)
                    (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[(Int, List[ClassificationTreeItem])]= Util.monitorFuture("ClassificationTree.iterateCluster") {
    node.read().map(_.iterateClusterMembers(max, cursor, node)).map(x=>x._1->x._2.map(_.value))
  }

  def iterateTree(max: Int = 100, cursor : Int = 0)
                           (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[(Int, List[ClassificationTreeItem])] = Util.monitorFuture("ClassificationTree.iterateTree") {
    rootPtr.readOpt().map(_.orElse(Option(ClassificationTree.newClassificationTreeData()))).flatMap(innerData => {
      iterateCluster(innerData.get.root, max = max, cursor = cursor)
    })
  }

  def splitCluster(node : STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy)
                    (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = Util.monitorFuture("ClassificationTree.splitCluster") {
    StmExecutionQueue.add(ClassificationTree.applyStrategy(node, strategy))
  }

  def splitTree(strategy: ClassificationStrategy)
                 (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.orElse(Option(ClassificationTree.newClassificationTreeData()))).flatMap(innerData => Util.monitorFuture("ClassificationTree.splitTree") {
      splitCluster(innerData.get.root, strategy)
    })
  }
}

