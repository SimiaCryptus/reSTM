package stm.collection

import stm.collection.ClassificationTree.{ClassificationTreeNode, _}
import stm.{STMPtr, _}
import storage.Restm
import storage.Restm._
import storage.types.KryoValue
import util.LevenshteinDistance

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

trait ClassificationStrategy {

  def getRule(values:List[ClassificationTree.LabeledItem]): (ClassificationTreeItem) => Boolean

  def split(buffer : TreeCollection[ClassificationTree.LabeledItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Boolean

}

case class TextClassificationStrategy(
                                          branchThreshold : Int = 4
                                        ) extends ClassificationStrategy
{
  def fitness(left: Map[String, Int], right: Map[String, Int], exceptions: Map[String, Int]): Double = {
    Random.nextDouble()
  }
  def getRule(values:List[ClassificationTree.LabeledItem]): (ClassificationTreeItem) => Boolean = {
    val fields = values.flatMap(_.value.attributes.keys).toSet
    fields.flatMap(field=>{
      val fileredItems: Seq[LabeledItem] = values.filter(_.value.attributes.isDefinedAt(field))
      fileredItems.flatMap(center=>{
        val exceptionCounts = values.filter(!_.value.attributes.isDefinedAt(field)).groupBy(_.label).mapValues(_.size)
        val valueSortMap = fileredItems.map(item => {
          item.label -> LevenshteinDistance.getDefaultInstance.apply(
            center.value.attributes(field).toString,
            item.value.attributes(field).toString
          ).doubleValue()
        }).sortBy(_._2)

        val labelCounters = valueSortMap.groupBy(_._1).mapValues(_.size)
        val valueCounters = new mutable.HashMap[String,Int]()
        valueSortMap.map(item=>{
          val (label, value) = item
          valueCounters.put(label, valueCounters.getOrElse(label, 0)+1)
          val compliment: Map[String, Int] = valueCounters.toMap.map(e=>e._1->(labelCounters(e._1) - e._2))
          val rule : (ClassificationTreeItem) => Boolean = item => {
            LevenshteinDistance.getDefaultInstance.apply(
              center.value.attributes(field).toString,
              item.attributes(field).toString
            ) < value.asInstanceOf[Number].doubleValue()
          }
          rule -> fitness(valueCounters.toMap, compliment, exceptionCounts)
        })
      })
    }).maxBy(_._2)._1
  }

  def split(buffer : TreeCollection[ClassificationTree.LabeledItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Boolean = {
    buffer.sync.apxSize() > branchThreshold
  }
}

case class DefaultClassificationStrategy(
                                          branchThreshold : Int = 4
                                        ) extends ClassificationStrategy
{
  def fitness(left: Map[String, Int], right: Map[String, Int], exceptions: Map[String, Int]): Double = {
    Random.nextDouble()
  }
  def getRule(values:List[ClassificationTree.LabeledItem]): (ClassificationTreeItem) => Boolean = {
    val fields = values.flatMap(_.value.attributes.keys).toSet
    fields.flatMap(field=>{
      val valueOptMap = values.map(item=>item.label->item.value.attributes.get(field))
      val exceptionCounts = valueOptMap.filter(_._2.isEmpty).groupBy(_._1).mapValues(_.size)
      val valueSortMap = valueOptMap.filter(_._2.isDefined).map(x=>x._1->x._2.get).sortBy(_._2.asInstanceOf[Number].doubleValue().toDouble)
      val labelCounters = valueSortMap.groupBy(_._1).mapValues(_.size)
      val valueCounters = new mutable.HashMap[String,Int]()
      valueSortMap.map(item=>{
        val (label, value) = item
        valueCounters.put(label, valueCounters.getOrElse(label, 0)+1)
        val compliment: Map[String, Int] = valueCounters.toMap.map(e=>e._1->(labelCounters(e._1) - e._2))
        val rule : (ClassificationTreeItem) => Boolean = item => {
          item.attributes(field).asInstanceOf[Number].doubleValue() < value.asInstanceOf[Number].doubleValue()
        }
        rule -> fitness(valueCounters.toMap, compliment, exceptionCounts)
      })
    }).maxBy(_._2)._1
  }
  def split(buffer : TreeCollection[ClassificationTree.LabeledItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Boolean = {
    buffer.sync.apxSize() > branchThreshold
  }
}

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

    def add(label: String, value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[ClassificationTreeNode]] = {
      val read: Future[ClassificationTreeNode] = root.read()
      read.flatMap(_.add(label, value, root, strategy).map(_.getOrElse(root)))
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

    def getNextClusterMembers(cursor: Int, max: Int, self : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[(Int, List[LabeledItem])] = {
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
            pass.get.read().flatMap(_.getNextClusterMembers(childCursor, max, pass.get))
          case 1 =>
            fail.get.read().flatMap(_.getNextClusterMembers(childCursor, max, fail.get))
          case 2 =>
            exception.get.read().flatMap(_.getNextClusterMembers(childCursor, max, exception.get))
        }
        childResult.map(childResult=> {
          val nextChildCursor: Int = childResult._1
          val nextCursor: Int = if(nextChildCursor < 0) {
            if(cursorBit == 2) -1 else cursorBit + 1
          } else {
            nextChildCursor * 3 + cursorBit
          }
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

    def add(label: String, value: ClassificationTreeItem, self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[STMPtr[ClassificationTreeNode]]] = {
      if(itemBuffer.isDefined) {
        //println(s"Adding an item ${value} - current size ${itemBuffer.get.sync.stream().size} id=${self.id}")
        itemBuffer.get.add(new LabeledItem(label,value)).flatMap(_=>{
          if (strategy.split(itemBuffer.get.asInstanceOf[TreeCollection[ClassificationTree.LabeledItem]])) {
            val bufferedValues: List[LabeledItem] = itemBuffer.get.sync.stream().toList
            //println(s"Begin split node with ${bufferedValues.size} items id=${self.id}")
            val nextValue: ClassificationTreeNode = copy(
              itemBuffer = None,
              rule = Option(KryoValue(strategy.getRule(bufferedValues.asInstanceOf[List[ClassificationTree.LabeledItem]]))),
              pass = Option(STMPtr.dynamicSync(newClassificationTreeNode(Option(self)))),
              fail = Option(STMPtr.dynamicSync(newClassificationTreeNode(Option(self)))),
              exception = Option(STMPtr.dynamicSync(newClassificationTreeNode(Option(self))))
            )
            self.write(nextValue).flatMap(_=>{
              Future.sequence(bufferedValues.map(item=>{
                nextValue.add(item.label, item.value, self, strategy)
              }))
            }).map(_=>{
              //println(s"Finished spliting node ${self.id} -> (${nextValue.pass.get.id}, ${nextValue.fail.get.id}, ${nextValue.exception.get.id})")
              None
            })
          } else {
            Future.successful(None)
          }
        })
      } else {
        try {
          if(apply(value)) {
            pass.get.read().flatMap(_.add(label, value, pass.get, strategy)).map(_.orElse(pass))
          } else {
            fail.get.read().flatMap(_.add(label, value, fail.get, strategy)).map(_.orElse(fail))
          }
        } catch {
          case e : Throwable =>
            exception.get.read().flatMap(_.add(label, value, exception.get, strategy)).map(_.orElse(exception))
        }
      }
    }

    def iterateClusterMembers(max: Int = 100, cursor : Int = 0, self : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
      def result(cursor : Int): (Int, List[LabeledItem]) = Await.result(getNextClusterMembers(cursor, max, self), 10.seconds)
      val stream: Seq[(Int, List[LabeledItem])] = Stream.iterate((cursor, List.empty[LabeledItem]))(t => {
        val (prevCursor, prevList) = t
        if(prevCursor < 0) t else {
          val (nextCursor, thisList) = result(prevCursor)
          //println(s"Fetch cursor $prevCursor - ${thisList.size} items")
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
      def predictLabel(value: ClassificationTreeItem) = sync { AtomicApi.this.predictLabel(value) }
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
    def predictLabel(value: ClassificationTreeItem) = atomic { ClassificationTree.this.predictLabel(value)(_,executionContext) }
  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority,maxRetries)

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def setClusterStrategy(value: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.setClusterStrategy(value) }
    def getClusterStrategy()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterStrategy() }
    def add(label:String, value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.add(label, value) }
    def getClusterId(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterId(value) }
    def getClusterPath(value: PointerType)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterPath(value) }
    def getClusterCount(value: PointerType)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterCount(value) }
    def iterateCluster(node : STMPtr[ClassificationTreeNode], value: PointerType, max: Int = 100, cursor : Int = 0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.iterateCluster(node, max, cursor) }
    def iterateTree(value: PointerType, max: Int = 100, cursor : Int = 0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.iterateTree(max, cursor) }
    def predictLabel(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
      sync { ClassificationTree.this.predictLabel(value) }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def add(label:String, value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData()))
        .map(state => state.add(label, value).map(state -> _)).get
    }).flatMap(newRootData => rootPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def setClusterStrategy(value: ClassificationStrategy)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Unit] = {
    rootPtr.readOpt().map(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData()))
        .map(state=>state.copy(strategy = value))
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

  def predictLabel(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Map[String,Double]] = {
    throw new RuntimeException
  }

}

