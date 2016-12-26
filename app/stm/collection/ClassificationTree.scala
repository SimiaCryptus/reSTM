package stm.collection

import stm.collection.ClassificationTree.{ClassificationTreeNode, _}
import stm.task.Task.{TaskResult, TaskSuccess}
import stm.task.{StmExecutionQueue, Task}
import stm.{STMPtr, _}
import storage.Restm
import storage.Restm._
import storage.types.KryoValue

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object ClassificationTree {

  case class ClassificationTreeItem(attributes:Map[String,Any])
  case class LabeledItem(label : String, value:ClassificationTreeItem)

  def newClassificationTreeData()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    {
      new ClassificationTreeData(STMPtr.dynamicSync(new ClassificationTreeNode(None)))
    }

  case class ClassificationTreeData
  (
    root: STMPtr[ClassificationTreeNode],
    strategy : ClassificationStrategy = new DefaultClassificationStrategy()
  ) {

    private def this() = this(new STMPtr[ClassificationTreeNode](new PointerType))

    def find(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[ClassificationTreeNode]] =
      root.read().flatMap(_.find(value).map(_.get))

    def add(label: String, value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
      root.read().flatMap(_.add(label, value, root, strategy, 1).map(_=>Unit))
    }

  }
  private def newClassificationTreeNode(parent : Option[STMPtr[ClassificationTreeNode]] = None)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new ClassificationTreeNode(parent, itemBuffer = Option(TreeCollection[LabeledItem]()))

  case class ClassificationTreeNode
  (
    parent : Option[STMPtr[ClassificationTreeNode]],
    pass : Option[STMPtr[ClassificationTreeNode]] = None,
    fail : Option[STMPtr[ClassificationTreeNode]] = None,
    exception : Option[STMPtr[ClassificationTreeNode]] = None,
    itemBuffer : Option[TreeCollection[LabeledItem]],
    rule : Option[KryoValue] = None
  ) {



    private def this() = this(None, itemBuffer = None)
    def this(parent : Option[STMPtr[ClassificationTreeNode]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
      this(parent, itemBuffer = Option(TreeCollection[LabeledItem]()))

    def apply = rule.get.deserialize[(ClassificationTreeItem)=>Boolean]().get

    def getNextClusterMembers(cursor: Int, self : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[(Int, List[LabeledItem])] = {
      require(0 <= cursor)
      if(itemBuffer.isDefined) {
        require(0 == cursor)
        itemBuffer.get.stream().map(stream=>{
          val list = stream.toList
          //println(s"Returning ${list.size} items from ${self.id}")
          -1 -> list
        })
      } else {
        val childCursor: Int = Math.floorDiv(cursor, 3)
        val cursorBit: Int = cursor % 3
        val childResult: Future[(Int, List[LabeledItem])] = cursorBit match {
          case 0 =>
            pass.get.read().flatMap(_.getNextClusterMembers(childCursor, pass.get))
          case 1 =>
            fail.get.read().flatMap(_.getNextClusterMembers(childCursor, fail.get))
          case 2 =>
            exception.get.read().flatMap(_.getNextClusterMembers(childCursor, exception.get))
        }
        childResult.map(childResult=> {
          val nextChildCursor: Int = childResult._1
          val nextCursor: Int = if(nextChildCursor < 0) {
            if(cursorBit == 2) -1 else cursorBit + 1
          } else {
            nextChildCursor * 3 + cursorBit
          }
          //println(s"Returning ${childResult._2.size} items from ${self.id}; cursor $cursor -> $nextCursor")
          nextCursor -> childResult._2
        })
      }
    }

    def getInfo(self:STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[NodeInfo] = {
      val x = new NodeInfo(self)
      val map: Option[Future[NodeInfo]] = parent.map(parent => parent.read().flatMap(_.getInfo(parent)).map(parent => x.copy(parent = Option(parent))))
      map.getOrElse(Future.successful(x))
    }

    def find(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[STMPtr[ClassificationTreeNode]]] = {
      if(rule.isDefined) {
        try {
          if(apply(value)) {
            pass.get.read().flatMap(_.find(value)).map(_.orElse(pass))
          } else {
            fail.get.read().flatMap(_.find(value)).map(_.orElse(fail))
          }
        } catch {
          case e : Throwable =>
            exception.get.read().flatMap(_.find(value)).map(_.orElse(exception))
        }
      } else {
        Future.successful(None)
      }
    }


    class NodeAtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {

      class SyncApi(duration: Duration) extends SyncApiBase(duration) {
        def splitTask(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy) = sync { NodeAtomicApi.this.splitTask(self, strategy) }
        def split(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 0) = sync { NodeAtomicApi.this.split(self, strategy, maxSplitDepth) }
      }
      def sync(duration: Duration) = new SyncApi(duration)
      def sync = new SyncApi(10.seconds)

      def splitTask(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy) = atomic { ClassificationTreeNode.this.splitTask(self, strategy)(_,executionContext) }
      def split(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 0) = atomic { ClassificationTreeNode.this.split(self, strategy, maxSplitDepth)(_,executionContext) }
    }
    def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new NodeAtomicApi(priority,maxRetries)


    def splitTask(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Task[Int]] = {
      StmExecutionQueue.add(splitFn(self, strategy))
    }

    def splitFn(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy)(cluster: Restm, executionContext: ExecutionContext) : TaskResult[Int] = {
      val tasks: List[Task[Int]] = {
        //implicit val _cluster = cluster
        //implicit val _executionContext = executionContext

        //println(s"Running split on ${self.id}")
        val newState: ClassificationTreeNode = Await.result({
          implicit val _executionContext = executionContext
          atomic()(cluster, executionContext).split(self, strategy, 0).flatMap(_ => self.atomic(cluster, executionContext).readOpt)
        }, 30.seconds).get

        Await.result(new STMTxn[List[Task[Int]]] {
          override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[List[Task[Int]]] = {
            Future.sequence(Map(
              "pass"->newState.pass, "fail"->newState.fail, "exception"->newState.exception
            ).filter(_._2.isDefined).mapValues(_.get).map(t=>{
              val (childType, child: STMPtr[ClassificationTreeNode]) = t
              child.sync.readOpt
                .filter((childState: ClassificationTreeNode) => {
                  childState.itemBuffer.map(itemBuffer=>{
                    lazy val size = itemBuffer.sync.size()
                    val recurse = strategy.split(childState.itemBuffer.get)
                    println(s"Split result for child $childType of ${self.id}: $size => $recurse (child id=${child.id})")
                    recurse
                  }).getOrElse(true)
                })
                .map((childState: ClassificationTreeNode) => childState.splitTask(child, strategy))
            }).filter(_.isDefined).map(_.get).toList)
          }
        }.txnRun(cluster)(executionContext), 30.seconds)

      }
      new Task.TaskContinue[Int](newFunction = (cluster,executionContext) =>{
        implicit val _cluster = cluster
        implicit val _executionContext = executionContext
        new TaskSuccess(tasks.map(_.atomic().sync.result()).sum)
      }, queue = StmExecutionQueue, newTriggers = tasks)
    }

    def split(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 0)
             (implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Int] = {
      itemBuffer.map(_.stream().map(_.toList).flatMap((bufferedValues: List[LabeledItem]) => {
        //println(s"Begin split node with ${bufferedValues.size} items id=${self.id}")
        val nextValue: ClassificationTreeNode = copy(
          itemBuffer = None,
          rule = Option(KryoValue(strategy.getRule(bufferedValues))),
          pass = Option(STMPtr.dynamicSync(newClassificationTreeNode(Option(self)))),
          fail = Option(STMPtr.dynamicSync(newClassificationTreeNode(Option(self)))),
          exception = Option(STMPtr.dynamicSync(newClassificationTreeNode(Option(self))))
        )
        val result = if(null != nextValue.rule) {
          self.write(nextValue).flatMap(_ => {
            Future.sequence(bufferedValues.map(item => {
              nextValue.add(item.label, item.value, self, strategy, maxSplitDepth-1)
            })).map(_.sum)
          })
        } else Future.successful(0)
        result
          .map(x=>{println(s"Finished spliting node ${self.id} -> (${nextValue.pass.get.id}, ${nextValue.fail.get.id}, ${nextValue.exception.get.id})");x})
      })).getOrElse({
        //println(s"Already split: ${self.id} -> (${pass.get.id}, ${fail.get.id}, ${exception.get.id})")
        Future.successful(0)
      })
    }

    def add(label: String, value: ClassificationTreeItem, self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 1)
           (implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Int] = {
      if(itemBuffer.isDefined) {
        //println(s"Adding an item ${value} - current size ${itemBuffer.get.sync.stream().size} id=${self.id}")
        itemBuffer.get.add(new LabeledItem(label,value)).flatMap(_=>{
          if (maxSplitDepth > 0 && strategy.split(itemBuffer.get.asInstanceOf[TreeCollection[ClassificationTree.LabeledItem]])) {
            self.lock().flatMap(locked=> {
              if(locked) {
                split(self, strategy, maxSplitDepth)
              } else {
                Future.successful(0)
              }
            })
          } else {
            Future.successful(0)
          }
        })
      } else {
        try {
          if(apply(value)) {
            pass.get.read().flatMap(_.add(label, value, pass.get, strategy, maxSplitDepth))
          } else {
            fail.get.read().flatMap(_.add(label, value, fail.get, strategy, maxSplitDepth))
          }
        } catch {
          case e : Throwable =>
            exception.get.read().flatMap(_.add(label, value, exception.get, strategy, maxSplitDepth))
        }
      }
    }

    def iterateClusterMembers(max: Int = 100, cursor : Int = 0, self : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
      def result(cursor : Int): (Int, List[LabeledItem]) = Await.result(getNextClusterMembers(cursor, self), 10.seconds)
      val stream: Seq[(Int, List[LabeledItem])] = Stream.iterate((cursor, List.empty[LabeledItem]))(t => {
        val (prevCursor, prevList) = t
        if(prevCursor < 0) t else {
          //println(s"Fetch cursor $prevCursor")
          val (nextCursor, thisList) = result(prevCursor)
          nextCursor -> (prevList ++ thisList)
        }
      })
      stream.find(t=>{
        val (cursor, cummulativeList) = t
        cursor < 0 || cummulativeList.size >= max
      }).getOrElse(stream.last)
    }

  }

  case class NodeInfo
  (
    node : STMPtr[ClassificationTreeNode],
    parent : Option[NodeInfo] = None
  )

  def apply()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new ClassificationTree(STMPtr.dynamicSync(ClassificationTree.newClassificationTreeData()))
}

class ClassificationTree(rootPtr: STMPtr[ClassificationTree.ClassificationTreeData]) {

  def this(id : PointerType) = this(new STMPtr[ClassificationTree.ClassificationTreeData](id))
  private def this() = this(new PointerType)


  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def setClusterStrategy(value: ClassificationStrategy) = sync { AtomicApi.this.setClusterStrategy(value) }
      def getClusterStrategy() = sync { AtomicApi.this.getClusterStrategy() }
      def add(label:String, value: ClassificationTreeItem) = sync { AtomicApi.this.add(label, value) }
      def getClusterId(value: ClassificationTreeItem) = sync { AtomicApi.this.getClusterId(value) }
      def getClusterPath(value: PointerType) = sync { AtomicApi.this.getClusterPath(value) }
      def getClusterCount(value: PointerType) = sync { AtomicApi.this.getClusterCount(value) }
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
    def getClusterId(value: ClassificationTreeItem) = atomic { ClassificationTree.this.getClusterId(value)(_,executionContext) }
    def getClusterPath(value: PointerType) = atomic { ClassificationTree.this.getClusterPath(value)(_,executionContext) }
    def getClusterCount(value: PointerType) = atomic { ClassificationTree.this.getClusterCount(value)(_,executionContext) }
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
    def getClusterId(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterId(value) }
    def getClusterPath(value: PointerType)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterPath(value) }
    def getClusterCount(value: PointerType)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterCount(value) }
    def splitCluster(node : STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.splitCluster(node, strategy) }
    def splitTree(strategy: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.splitTree(strategy) }
    def iterateCluster(node : STMPtr[ClassificationTreeNode], value: PointerType, max: Int = 100, cursor : Int = 0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.iterateCluster(node, max, cursor) }
    def iterateTree(value: PointerType, max: Int = 100, cursor : Int = 0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.iterateTree(max, cursor) }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def add(label:String, value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData()))
        .map(state => state.add(label, value).map(state -> _)).get
    }).flatMap(newRootData => rootPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def setClusterStrategy(strategy: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Unit] = {
    rootPtr.readOpt().map(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData()))
        .map(state=>state.copy(strategy = strategy))
        .get
    }).flatMap(newRootData => rootPtr.write((newRootData)).map(_=>Unit))
  }

  def getClusterStrategy()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[ClassificationStrategy] = {
    rootPtr.readOpt().map(_.map(_.strategy).getOrElse(new DefaultClassificationStrategy))
  }

  def getClusterId(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[STMPtr[ClassificationTreeNode]] = {
    rootPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData())).map(state=>
        state.find(value).map(state -> _)).get
    }).flatMap(newRootData => rootPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def getClusterPath(value: PointerType)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    val ptr: STMPtr[ClassificationTreeNode] = new STMPtr[ClassificationTreeNode](value)
    ptr.read().flatMap(_.getInfo(ptr))
  }

  def getClusterCount(value: PointerType)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    val ptr: STMPtr[ClassificationTreeNode] = new STMPtr[ClassificationTreeNode](value)
    ptr.read().map(_.iterateClusterMembers(max=100,cursor=0,ptr)).map(r=>{
      require(-1 == r._1)
      r._2.groupBy(_.label).mapValues(_.size)
    })
  }

  def iterateCluster(node : STMPtr[ClassificationTreeNode], max: Int = 100, cursor : Int = 0)
                    (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[(Int, List[ClassificationTreeItem])]= {
    node.read().map(_.iterateClusterMembers(max, cursor, node)).map(x=>x._1->x._2.map(_.value))
  }

  def iterateTree(max: Int = 100, cursor : Int = 0)
                           (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[(Int, List[ClassificationTreeItem])] = {
    rootPtr.readOpt().map(_.orElse(Option(ClassificationTree.newClassificationTreeData()))).flatMap(innerData => {
      iterateCluster(innerData.get.root, max = max, cursor = cursor)
    })
  }

  def splitCluster(node : STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy)
                    (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    node.read().flatMap(_.splitTask(node, strategy))
  }

  def splitTree(strategy: ClassificationStrategy)
                 (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.orElse(Option(ClassificationTree.newClassificationTreeData()))).flatMap(innerData => {
      splitCluster(innerData.get.root, strategy)
    })
  }
}

