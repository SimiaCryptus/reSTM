package stm.collection

import stm.collection.ClassificationTree.ClassificationTreeNode
import stm.{STMPtr, _}
import storage.Restm
import storage.Restm._
import storage.data.KryoValue

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Random

trait ClassificationStrategy[T] {

  def getRule(values:List[ClassificationTree.TreeItem[T]]): (T) => Boolean

  def split(buffer : TreeCollection[ClassificationTree.TreeItem[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) : Boolean

}

object ClassificationStrategy extends ClassificationStrategy[AnyRef] {

  def getRule(values:List[ClassificationTree.TreeItem[AnyRef]]): (AnyRef) => Boolean = {
    item => {
      Random.nextBoolean()
    }
  }

  def split(buffer : TreeCollection[ClassificationTree.TreeItem[AnyRef]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[AnyRef]) : Boolean = {
    //itemBuffer.get.sync.apxSize() > 1
    buffer.sync.stream().size > 3
  }

}

object ClassificationTree {

  def newClassificationTreeData[T]()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    {
      new ClassificationTreeData[T](STMPtr.dynamicSync(new ClassificationTreeNode[T](None)))
    }

  case class ClassificationTreeData[T]
  (
    root: STMPtr[ClassificationTreeNode[T]]
  ) {

    private def this() = this(new STMPtr[ClassificationTreeNode[T]](new PointerType))

    def find(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[ClassificationTreeNode[T]]] =
      root.read().flatMap(_.find(value).map(_.get))

    def add(label: String, value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[STMPtr[ClassificationTreeNode[T]]] = {
      val read: Future[ClassificationTreeNode[T]] = root.read()
      read.flatMap(_.add(label, value, root).map(_.getOrElse(root)))
    }

    def iterateClusterMembers(max: Int = 100, cursor : Int = 0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = {
      def result(cursor : Int): (Int, List[T]) = Await.result(root.readOpt().flatMap(rootNode => rootNode.get.iterateClusterMembers(cursor, max, root)), 10.seconds)
      val stream: Seq[(Int, List[T])] = Stream.iterate((cursor, List.empty[T]))(t => {
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
  private def newClassificationTreeNode[T]()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new ClassificationTreeNode(None, itemBuffer = Option(TreeCollection[TreeItem[T]]()))

  case class TreeItem[T](label : String, value:T)

  case class ClassificationTreeNode[T]
  (
    parent : Option[STMPtr[ClassificationTreeNode[T]]],
    pass : Option[STMPtr[ClassificationTreeNode[T]]] = None,
    fail : Option[STMPtr[ClassificationTreeNode[T]]] = None,
    exception : Option[STMPtr[ClassificationTreeNode[T]]] = None,
    itemBuffer : Option[TreeCollection[TreeItem[T]]],
    rule : Option[KryoValue] = None
  ) {

    private def this() = this(None, itemBuffer = None)
    def this(parent : Option[STMPtr[ClassificationTreeNode[T]]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
      this(parent, itemBuffer = Option(TreeCollection[TreeItem[T]]()))

    def apply = rule.get.deserialize[(T)=>Boolean]().get

    def iterateClusterMembers(cursor: Int, max: Int, self : STMPtr[ClassificationTreeNode[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) : Future[(Int, List[T])] = {
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
        val childResult: Future[(Int, List[T])] = cursorBit match {
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

    def getInfo(self:STMPtr[ClassificationTreeNode[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[NodeInfo[T]] = {
      val x = new NodeInfo(self)
      val map: Option[Future[NodeInfo[T]]] = parent.map(parent => parent.read().flatMap(_.getInfo(parent)).map(parent => x.copy(parent = Option(parent))))
      map.getOrElse(Future.successful(x))
    }

    def find(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[STMPtr[ClassificationTreeNode[T]]]] = {
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

    def add(label: String, value: T, self : STMPtr[ClassificationTreeNode[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Option[STMPtr[ClassificationTreeNode[T]]]] = {
      if(itemBuffer.isDefined) {
        //println(s"Adding an item ${value} - current size ${itemBuffer.get.sync.stream().size} id=${self.id}")
        itemBuffer.get.add(new TreeItem[T](label,value)).flatMap(_=>{
          if (ClassificationStrategy.split(itemBuffer.get.asInstanceOf[TreeCollection[ClassificationTree.TreeItem[AnyRef]]])) {
            val bufferedValues: List[TreeItem[T]] = itemBuffer.get.sync.stream().toList
            //println(s"Begin split node with ${bufferedValues.size} items id=${self.id}")
            val nextValue: ClassificationTreeNode[T] = copy(
              itemBuffer = None,
              rule = Option(KryoValue(ClassificationStrategy.getRule(bufferedValues.asInstanceOf[List[ClassificationTree.TreeItem[AnyRef]]]))),
              pass = Option(STMPtr.dynamicSync(newClassificationTreeNode[T]())),
              fail = Option(STMPtr.dynamicSync(newClassificationTreeNode[T]())),
              exception = Option(STMPtr.dynamicSync(newClassificationTreeNode[T]()))
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

  case class NodeInfo[T]
  (
    node : STMPtr[ClassificationTreeNode[T]],
    parent : Option[NodeInfo[T]] = None
  )

  def apply[T]()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new ClassificationTree[T](STMPtr.dynamicSync(ClassificationTree.newClassificationTreeData[T]()))
}

class ClassificationTree[T](rootPtr: STMPtr[ClassificationTree.ClassificationTreeData[T]]) {

  def this(id : PointerType) = this(new STMPtr[ClassificationTree.ClassificationTreeData[T]](id))
  private def this() = this(new PointerType)


  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(label:String, value: T)(implicit classTag: ClassTag[T]) = sync { AtomicApi.this.add(label, value) }
      def getClusterId(value: T)(implicit classTag: ClassTag[T]) = sync { AtomicApi.this.getClusterId(value) }
      def getClusterInfo(value: PointerType)(implicit classTag: ClassTag[T]) = sync { AtomicApi.this.getClusterInfo(value) }
      def iterateClusterMembers(max: Int = 100, cursor : Int = 0)(implicit classTag: ClassTag[T]) = sync { AtomicApi.this.iterateClusterMembers(max, cursor) }
      def predictLabel(value: T)(implicit classTag: ClassTag[T]) = sync { AtomicApi.this.predictLabel(value) }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)

    def add(label:String, value: T)(implicit classTag: ClassTag[T]) = atomic { ClassificationTree.this.add(label, value)(_,executionContext, classTag).map(_ => Unit) }
    def getClusterId(value: T)(implicit classTag: ClassTag[T]) = atomic { ClassificationTree.this.getClusterId(value)(_,executionContext,classTag).map(_ => Unit) }
    def getClusterInfo(value: PointerType)(implicit classTag: ClassTag[T]) = atomic { ClassificationTree.this.getClusterInfo(value)(_,executionContext,classTag) }
    def iterateClusterMembers(max: Int = 100, cursor : Int = 0)(implicit classTag: ClassTag[T]) =
      atomic { ClassificationTree.this.iterateClusterMembers(max, cursor)(_,executionContext, classTag) }
    def predictLabel(value: T) = atomic { ClassificationTree.this.predictLabel(value)(_,executionContext).map(_ => Unit) }
  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority,maxRetries)

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def add(label:String, value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = sync { ClassificationTree.this.add(label, value) }
    def getClusterId(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = sync { ClassificationTree.this.getClusterId(value) }
    def getClusterInfo(value: PointerType)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = sync { ClassificationTree.this.getClusterInfo(value) }
    def iterateClusterMembers(value: PointerType, max: Int = 100, cursor : Int = 0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) =
      sync { ClassificationTree.this.iterateClusterMembers(max, cursor) }
    def predictLabel(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) =
      sync { ClassificationTree.this.predictLabel(value) }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def add(label:String, value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = {
    rootPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData[T]()))
        .map(state => state.add(label, value).map(state -> _)).get
    }).flatMap(newRootData => rootPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def getClusterId(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = {
    rootPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData[T]())).map(state=>
        state.find(value).map(state -> _)).get
    }).flatMap(newRootData => rootPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def getClusterInfo(value: PointerType)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = {
    rootPtr.readOpt().flatMap(prev => {
      prev.orElse(Option(ClassificationTree.newClassificationTreeData[T]())).map(state=>{
        val ptr: STMPtr[ClassificationTreeNode[T]] = new STMPtr[ClassificationTreeNode[T]](value)
        ptr.read().flatMap(_.getInfo(ptr)).map(state -> _)
      }).get
    }).flatMap(newRootData => rootPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def iterateClusterMembers(max: Int = 100, cursor : Int = 0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = {
    rootPtr.readOpt().map(_.orElse(Option(ClassificationTree.newClassificationTreeData[T]()))).map(innerData => {
      innerData.get.iterateClusterMembers(max, cursor)
    })
  }

  def predictLabel(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Map[String,Double]] = {
    throw new RuntimeException
  }

}

