package stm.collection

import stm.collection.ClassificationTree.{ClassificationTreeNode, _}
import stm.task.Task.{TaskResult, TaskSuccess}
import stm.task.{StmExecutionQueue, Task}
import stm.{STMPtr, _}
import storage.Restm
import storage.Restm.PointerType
import storage.types.KryoValue
import util.Util

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
      root.read().flatMap(_.find(value).map(_.getOrElse(root)))

    def add(value: List[LabeledItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
      root.read().flatMap(_.add(value, root, strategy, 1).map(_=>Unit))
    }

  }
  private def newClassificationTreeNode(parent : Option[STMPtr[ClassificationTreeNode]] = None)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new ClassificationTreeNode(parent, itemBuffer = Option(BatchedTreeCollection[LabeledItem]()))


  def splitFn(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy) : (Restm, ExecutionContext) => TaskResult[Int] = (cluster, executionContext) => {
    val tasks: List[Task[Int]] = {
      //implicit val _cluster = cluster
      //implicit val _executionContext = executionContext

      //println(s"Running split on ${self.id}")
      val newState: ClassificationTreeNode = Await.result({
        implicit val _executionContext = executionContext
        self.atomic(cluster, executionContext).sync.read.atomic()(cluster, executionContext).split(self, strategy, 0).flatMap(_ => self.atomic(cluster, executionContext).readOpt)
      }, 90.seconds).get

      Await.result(new STMTxn[List[(Restm, ExecutionContext) => TaskResult[Int]]] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[List[(Restm, ExecutionContext) => TaskResult[Int]]] = Future {
          Map(
            "pass"->newState.pass, "fail"->newState.fail, "exception"->newState.exception
          ).filter(_._2.isDefined).mapValues(_.get).map(t=>{
            val (childType, child: STMPtr[ClassificationTreeNode]) = t
            val map: Option[(Restm, ExecutionContext) => TaskResult[Int]] = child.sync.readOpt
              .filter((childState: ClassificationTreeNode) => {
                childState.itemBuffer.map(itemBuffer => {
                  lazy val size = itemBuffer.sync.size()
                  val recurse = strategy.split(childState.itemBuffer.get)
                  //println(s"Split result for child $childType of ${self.id}: $size => $recurse (child id=${child.id})")
                  recurse
                }).getOrElse(true)
              })
              .map((childState: ClassificationTreeNode) => splitFn(child, strategy))
            map
          }).filter(_.isDefined).map(_.get).toList
        }
      }.txnRun(cluster)(executionContext), 90.seconds)
        .map((x: (Restm, ExecutionContext) => TaskResult[Int]) =>StmExecutionQueue.atomic(cluster, executionContext).sync.add(x))
    }
    new Task.TaskContinue[Int](newFunction = (cluster,executionContext) =>{
      implicit val _cluster = cluster
      implicit val _executionContext = executionContext
      new TaskSuccess(tasks.map(_.atomic().sync.result()).sum)
    }, queue = StmExecutionQueue, newTriggers = tasks)
  }


  case class ClassificationTreeNode
  (
    parent : Option[STMPtr[ClassificationTreeNode]],
    pass : Option[STMPtr[ClassificationTreeNode]] = None,
    fail : Option[STMPtr[ClassificationTreeNode]] = None,
    exception : Option[STMPtr[ClassificationTreeNode]] = None,
    itemBuffer : Option[BatchedTreeCollection[LabeledItem]],
    rule : Option[KryoValue[(ClassificationTreeItem)=>Boolean]] = None
  ) {



    private def this() = this(None, itemBuffer = None)
    def this(parent : Option[STMPtr[ClassificationTreeNode]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
      this(parent, itemBuffer = Option(BatchedTreeCollection[LabeledItem]()))

    def apply = rule.get.deserialize().get

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

    def getByTreeId(cursor: Int, self : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[STMPtr[ClassificationTreeNode]] = {
      require(0 <= cursor)
      if(itemBuffer.isDefined) {
        require(0 == cursor)
        Future.successful(self)
      } else {
        val childCursor: Int = Math.floorDiv(cursor, 3)
        val cursorBit: Int = cursor % 3
        cursorBit match {
          case 0 =>
            pass.get.read().flatMap(_.getByTreeId(childCursor, pass.get))
          case 1 =>
            fail.get.read().flatMap(_.getByTreeId(childCursor, fail.get))
          case 2 =>
            exception.get.read().flatMap(_.getByTreeId(childCursor, exception.get))
        }
      }
    }

    def getTreeBit(self:STMPtr[ClassificationTreeNode], node:STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Int = {
      if(pass.filter(_==node).isDefined) 0
      else if(fail.filter(_==node).isDefined) 1
      else if(exception.filter(_==node).isDefined) 2
      else throw new RuntimeException()
    }

    def getTreeId(self:STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[(Int,Int)] = {
      parent.map(_.read().flatMap(parentNode => {
        parentNode.getTreeId(parent.get).map(x=>{
          val (depth:Int,parentId:Int) = x
          val bit: Int = parentNode.getTreeBit(parent.get, self)
          val tuple: (Int, Int) = (depth + 1) -> (parentId+Math.pow(3,depth).toInt*bit)
          tuple
        })
      })).getOrElse(Future.successful(0->0))
    }

    def getInfo(self:STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[NodeInfo] = {
      val id: Future[Int] = getTreeId(self).map(_._2)
      id.flatMap(id=>{
        val nodeInfo = new NodeInfo(self, id)
        val map: Option[Future[NodeInfo]] = parent.map(parent => parent.read().flatMap(_.getInfo(parent)).map(parent => nodeInfo.copy(parent = Option(parent))))
        map.getOrElse(Future.successful(nodeInfo))
      })
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
        def split(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 0) = sync { NodeAtomicApi.this.split(self, strategy, maxSplitDepth) }
      }
      def sync(duration: Duration) = new SyncApi(duration)
      def sync = new SyncApi(10.seconds)

      def split(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 0) = atomic { ClassificationTreeNode.this.split(self, strategy, maxSplitDepth)(_,executionContext) }
    }
    def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new NodeAtomicApi(priority,maxRetries)


    def split(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 0)
             (implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Int] = {
      itemBuffer.map(_.stream().flatMap((bufferedValues: Stream[LabeledItem]) => {
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
            Future.sequence(bufferedValues.grouped(64).map(_.toList).map(item => {
              nextValue.add(item, self, strategy, maxSplitDepth - 1)
            })).map(_.sum)
          })
        } else Future.successful(0)
        result
          //.map(x=>{println(s"Finished spliting node ${self.id} -> (${nextValue.pass.get.id}, ${nextValue.fail.get.id}, ${nextValue.exception.get.id})");x})
      })).getOrElse({
        //println(s"Already split: ${self.id} -> (${pass.get.id}, ${fail.get.id}, ${exception.get.id})")
        Future.successful(0)
      })
    }

    def add(value: List[LabeledItem], self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 1)
           (implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Int] = {
      if(itemBuffer.isDefined) {
        //println(s"Adding an item ${value} - current size ${itemBuffer.get.sync.stream().size} id=${self.id}")
        itemBuffer.get.add(value.toArray.toList).flatMap(_=>{
          if (maxSplitDepth > 0 && strategy.split(itemBuffer.get.asInstanceOf[BatchedTreeCollection[ClassificationTree.LabeledItem]])) {
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
          val results: Map[Boolean, List[LabeledItem]] = value.groupBy(x=>apply(x.value))
          Future.sequence(List(
            if(results.get(true).isDefined) {
              pass.get.read().flatMap(_.add(results(true), pass.get, strategy, maxSplitDepth))
            } else {
              Future.successful(0)
            },
            if(results.get(false).isDefined) {
              fail.get.read().flatMap(_.add(results(false), fail.get, strategy, maxSplitDepth))
            } else {
              Future.successful(0)
            }
          )).map(_.reduceOption(_+_).getOrElse(0))
        } catch {
          case e : Throwable =>
            exception.get.read().flatMap(_.add(value, exception.get, strategy, maxSplitDepth))
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
    StmExecutionQueue.add(splitFn(node, strategy))
  }

  def splitTree(strategy: ClassificationStrategy)
                 (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.orElse(Option(ClassificationTree.newClassificationTreeData()))).flatMap(innerData => Util.monitorFuture("ClassificationTree.splitTree") {
      splitCluster(innerData.get.root, strategy)
    })
  }
}

