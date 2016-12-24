package stm.collection

import stm.collection.ClassificationTree.ClassificationTreeNode
import stm.{STMPtr, _}
import storage.Restm
import storage.Restm._
import storage.data.KryoValue

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Random


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

    def iterateClusterMembers(cursor: Int, max: Int)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) : Future[(List[T],Int)] = {
      require(0 <= cursor)
      if(itemBuffer.isDefined) {
        require(0 == cursor)
        itemBuffer.get.stream().map(_.map(_.value)).map(stream=>{
          val list = stream.toList
          list -> -1
        })
      } else {
        val childCursor: Int = Math.floorDiv(cursor, 3)
        val cursorBit: Int = cursor % 3
        val childResult: Future[(List[T], Int)] = cursorBit match {
          case 0 =>
            pass.get.read().flatMap(_.iterateClusterMembers(childCursor, max))
          case 1 =>
            fail.get.read().flatMap(_.iterateClusterMembers(childCursor, max))
          case 2 =>
            exception.get.read().flatMap(_.iterateClusterMembers(childCursor, max))
        }
        childResult.map(childResult=> {
          val nextChildCursor: Int = childResult._2
          val nextCursor: Int = if(nextChildCursor < 0) {
            if(cursorBit == 2) -1 else cursorBit + 1
          } else {
            nextChildCursor * 3 + cursorBit
          }
          childResult._1 -> nextCursor
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

    def getRule(values:List[TreeItem[T]]): (T) => Boolean = {
      item => {
        Random.nextBoolean()
      }
    }

    def add(label: String, value: T, self : STMPtr[ClassificationTreeNode[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Option[STMPtr[ClassificationTreeNode[T]]]] = {
      if(itemBuffer.isDefined) {
        //println(s"Adding an item ${value} - current size ${itemBuffer.get.sync.stream().size} id=${itemBuffer.get.rootPtr.id}")
        itemBuffer.get.add(new TreeItem[T](label,value)).map(_=>{
          val size: Long = itemBuffer.get.sync.apxSize()
          if(size > 5) {
            val bufferedValues: List[TreeItem[T]] = itemBuffer.get.sync.stream().toList
            val nextValue: ClassificationTreeNode[T] = copy(
              itemBuffer = None,
              rule = Option(KryoValue(getRule(bufferedValues))),
              pass = Option(STMPtr.dynamicSync(newClassificationTreeNode[T]())),
              fail = Option(STMPtr.dynamicSync(newClassificationTreeNode[T]())),
              exception = Option(STMPtr.dynamicSync(newClassificationTreeNode[T]()))
            )
            self.write(nextValue).flatMap(_=>{
              Future.sequence(bufferedValues.map(item=>{
                nextValue.add(item.label, item.value, self)
              }))
            })

          }
          None
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
    rootPtr.readOpt().map(_.orElse(Option(ClassificationTree.newClassificationTreeData[T]()))).flatMap(innerData => {
      innerData.map(_.root.readOpt().flatMap(rootNode => rootNode.get.iterateClusterMembers(cursor, max).map(t=> innerData.get -> t))).get
    }).flatMap(newRootData => rootPtr.write((newRootData._1)).map(_=>newRootData._2))
  }

  def predictLabel(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Map[String,Double]] = {
    throw new RuntimeException
  }

}

