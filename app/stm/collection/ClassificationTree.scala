package stm.collection

import stm.collection.ClassificationTree.ClassificationTreeNode
import stm.{STMPtr, _}
import storage.Restm
import storage.Restm._
import storage.data.KryoValue

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

trait ClassificationStrategy {

  def getRule(values:List[ClassificationTree.TreeItem]): (ClassificationTreeItem) => Boolean

  def split(buffer : TreeCollection[ClassificationTree.TreeItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Boolean

}

object ClassificationStrategy extends ClassificationStrategy {

  def fitness(left: Map[String, Int], right: Map[String, Int], exceptions: Map[String, Int]): Double = {
    Random.nextDouble()
  }

  def getRule(values:List[ClassificationTree.TreeItem]): (ClassificationTreeItem) => Boolean = {
    val fields = values.flatMap(_.value.attributes.keys).toSet
    fields.flatMap(field=>{
      val valueOptMap = values.map(item=>item.label->item.value.attributes.get(field))
      val exceptionCounts = valueOptMap.filter(_._2.isEmpty).groupBy(_._1).mapValues(_.size)
      val valueSortMap = valueOptMap.filter(_._2.isDefined).map(x=>x._1->x._2.get).sortBy(_._2)
      val labelCounters = valueSortMap.groupBy(_._1).mapValues(_.size)
      val valueCounters = new mutable.HashMap[String,Int]()
      valueSortMap.map(item=>{
        val (label, value) = item
        valueCounters.put(label, valueCounters.getOrElse(label, 0)+1)
        val compliment: Map[String, Int] = valueCounters.toMap.map(e=>e._1->(labelCounters(e._1) - e._2))
        val rule : (ClassificationTreeItem) => Boolean = item => {
          item.attributes(field) < value
        }
        rule -> fitness(valueCounters.toMap, compliment, exceptionCounts)
      })
    }).maxBy(_._2)._1
  }

  def split(buffer : TreeCollection[ClassificationTree.TreeItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Boolean = {
    //itemBuffer.get.sync.apxSize() > 1
    buffer.sync.stream().size > 3
  }

}

case class ClassificationTreeItem(attributes:Map[String,Double])


object ClassificationTree {


  def newClassificationTreeData()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    {
      new ClassificationTreeData(STMPtr.dynamicSync(new ClassificationTreeNode(None)))
    }

  case class ClassificationTreeData
  (
    root: STMPtr[ClassificationTreeNode]
  ) {

    private def this() = this(new STMPtr[ClassificationTreeNode](new PointerType))

    def find(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[ClassificationTreeNode]] =
      root.read().flatMap(_.find(value).map(_.get))

    def add(label: String, value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[ClassificationTreeNode]] = {
      val read: Future[ClassificationTreeNode] = root.read()
      read.flatMap(_.add(label, value, root).map(_.getOrElse(root)))
    }

    def iterateClusterMembers(max: Int = 100, cursor : Int = 0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
      def result(cursor : Int): (Int, List[ClassificationTreeItem]) = Await.result(root.readOpt().flatMap(rootNode => rootNode.get.iterateClusterMembers(cursor, max, root)), 10.seconds)
      val stream: Seq[(Int, List[ClassificationTreeItem])] = Stream.iterate((cursor, List.empty[ClassificationTreeItem]))(t => {
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
  private def newClassificationTreeNode()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new ClassificationTreeNode(None, itemBuffer = Option(TreeCollection[TreeItem]()))

  case class TreeItem(label : String, value:ClassificationTreeItem)

  case class ClassificationTreeNode
  (
    parent : Option[STMPtr[ClassificationTreeNode]],
    pass : Option[STMPtr[ClassificationTreeNode]] = None,
    fail : Option[STMPtr[ClassificationTreeNode]] = None,
    exception : Option[STMPtr[ClassificationTreeNode]] = None,
    itemBuffer : Option[TreeCollection[TreeItem]],
    rule : Option[KryoValue] = None
  ) {

    private def this() = this(None, itemBuffer = None)
    def this(parent : Option[STMPtr[ClassificationTreeNode]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
      this(parent, itemBuffer = Option(TreeCollection[TreeItem]()))

    def apply = rule.get.deserialize[(ClassificationTreeItem)=>Boolean]().get

    def iterateClusterMembers(cursor: Int, max: Int, self : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[(Int, List[ClassificationTreeItem])] = {
      require(0 <= cursor)
      if(itemBuffer.isDefined) {
        require(0 == cursor)
        itemBuffer.get.stream().map(_.map(_.value)).map(stream=>{
          val list = stream.toList
          //println(s"Returning ${list.size} items from ${self.id}")
          -1 -> list
        })
      } else {
        val childCursor: Int = Math.floorDiv(cursor, 3)
        val cursorBit: Int = cursor % 3
        val childResult: Future[(Int, List[ClassificationTreeItem])] = cursorBit match {
          case 0 =>
            pass.get.read().flatMap(_.iterateClusterMembers(childCursor, max, pass.get))
          case 1 =>
            fail.get.read().flatMap(_.iterateClusterMembers(childCursor, max, fail.get))
          case 2 =>
            exception.get.read().flatMap(_.iterateClusterMembers(childCursor, max, exception.get))
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

    def add(label: String, value: ClassificationTreeItem, self : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[STMPtr[ClassificationTreeNode]]] = {
      if(itemBuffer.isDefined) {
        //println(s"Adding an item ${value} - current size ${itemBuffer.get.sync.stream().size} id=${self.id}")
        itemBuffer.get.add(new TreeItem(label,value)).flatMap(_=>{
          if (ClassificationStrategy.split(itemBuffer.get.asInstanceOf[TreeCollection[ClassificationTree.TreeItem]])) {
            val bufferedValues: List[TreeItem] = itemBuffer.get.sync.stream().toList
            //println(s"Begin split node with ${bufferedValues.size} items id=${self.id}")
            val nextValue: ClassificationTreeNode = copy(
              itemBuffer = None,
              rule = Option(KryoValue(ClassificationStrategy.getRule(bufferedValues.asInstanceOf[List[ClassificationTree.TreeItem]]))),
              pass = Option(STMPtr.dynamicSync(newClassificationTreeNode())),
              fail = Option(STMPtr.dynamicSync(newClassificationTreeNode())),
              exception = Option(STMPtr.dynamicSync(newClassificationTreeNode()))
            )
            self.write(nextValue).flatMap(_=>{
              Future.sequence(bufferedValues.map(item=>{
                nextValue.add(item.label, item.value, self)
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
            pass.get.read().flatMap(_.add(label, value, pass.get)).map(_.orElse(pass))
          } else {
            fail.get.read().flatMap(_.add(label, value, fail.get)).map(_.orElse(fail))
          }
        } catch {
          case e : Throwable =>
            exception.get.read().flatMap(_.add(label, value, exception.get)).map(_.orElse(exception))
        }
      }
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
      def add(label:String, value: ClassificationTreeItem) = sync { AtomicApi.this.add(label, value) }
      def getClusterId(value: ClassificationTreeItem) = sync { AtomicApi.this.getClusterId(value) }
      def getClusterInfo(value: PointerType) = sync { AtomicApi.this.getClusterInfo(value) }
      def iterateClusterMembers(max: Int = 100, cursor : Int = 0) = sync { AtomicApi.this.iterateClusterMembers(max, cursor) }
      def predictLabel(value: ClassificationTreeItem) = sync { AtomicApi.this.predictLabel(value) }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)

    def add(label:String, value: ClassificationTreeItem) = atomic { ClassificationTree.this.add(label, value)(_,executionContext).map(_ => Unit) }
    def getClusterId(value: ClassificationTreeItem) = atomic { ClassificationTree.this.getClusterId(value)(_,executionContext).map(_ => Unit) }
    def getClusterInfo(value: PointerType) = atomic { ClassificationTree.this.getClusterInfo(value)(_,executionContext) }
    def iterateClusterMembers(max: Int = 100, cursor : Int = 0) =
      atomic { ClassificationTree.this.iterateClusterMembers(max, cursor)(_,executionContext) }
    def predictLabel(value: ClassificationTreeItem) = atomic { ClassificationTree.this.predictLabel(value)(_,executionContext).map(_ => Unit) }
  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority,maxRetries)

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def add(label:String, value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.add(label, value) }
    def getClusterId(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterId(value) }
    def getClusterInfo(value: PointerType)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { ClassificationTree.this.getClusterInfo(value) }
    def iterateClusterMembers(value: PointerType, max: Int = 100, cursor : Int = 0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
      sync { ClassificationTree.this.iterateClusterMembers(max, cursor) }
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

  def getClusterId(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData())).map(state=>
        state.find(value).map(state -> _)).get
    }).flatMap(newRootData => rootPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def getClusterInfo(value: PointerType)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData())).map(state=>{
        val ptr: STMPtr[ClassificationTreeNode] = new STMPtr[ClassificationTreeNode](value)
        ptr.read().flatMap(_.getInfo(ptr)).map(state -> _)
      }).get
    }).flatMap(newRootData => rootPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def iterateClusterMembers(max: Int = 100, cursor : Int = 0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.orElse(Option(ClassificationTree.newClassificationTreeData()))).map(innerData => {
      innerData.get.iterateClusterMembers(max, cursor)
    })
  }

  def predictLabel(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Map[String,Double]] = {
    throw new RuntimeException
  }

}

