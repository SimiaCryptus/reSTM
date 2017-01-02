package stm.collection.clustering


import stm.collection.BatchedTreeCollection
import stm.collection.clustering.ClassificationTree.{ClassificationTreeItem, LabeledItem, NodeInfo}
import stm.task.Task.TaskSuccess
import stm.task.{StmExecutionQueue, Task}
import stm.{STMPtr, _}
import storage.Restm
import storage.types.KryoValue
import util.Util

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

case class ClassificationTreeNode
(
  parent : Option[STMPtr[ClassificationTreeNode]],
  pass : Option[STMPtr[ClassificationTreeNode]] = None,
  fail : Option[STMPtr[ClassificationTreeNode]] = None,
  exception : Option[STMPtr[ClassificationTreeNode]] = None,
  itemBuffer : Option[BatchedTreeCollection[LabeledItem]],
  splitBuffer : Option[BatchedTreeCollection[LabeledItem]] = None,
  splitTask : Option[Task[String]] = None,
  rule : Option[KryoValue[(ClassificationTreeItem)=>Boolean]] = None
) {


  private def this() = this(None, itemBuffer = None)


  class NodeAtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def split(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 0): Int = sync { ClassificationTreeNode.split(self, strategy, maxSplitDepth) }
      def firstNode(self : STMPtr[ClassificationTreeNode]): STMPtr[ClassificationTreeNode] = sync { NodeAtomicApi.this.firstNode(self) }
      def nextNode(self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode]): Option[STMPtr[ClassificationTreeNode]] = sync { NodeAtomicApi.this.nextNode(self, root) }
      def getTreeId(self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode]): Long = sync { NodeAtomicApi.this.getTreeId(self, root) }
      def nextBlock(value: Long, self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode]): (Long, Stream[LabeledItem]) = sync { NodeAtomicApi.this.nextBlock(value, self, root) }
      def stream(self : STMPtr[ClassificationTreeNode]): Stream[LabeledItem] = sync { NodeAtomicApi.this.stream(self) }
      def getMembers(self : STMPtr[ClassificationTreeNode]): Stream[LabeledItem] = sync { NodeAtomicApi.this.getMembers(self) }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)


    def add(value: List[LabeledItem], self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 1): Future[Int] = atomic { ClassificationTreeNode.this.add(value, self, strategy, maxSplitDepth)(_,executionContext) }
    def route(value: List[LabeledItem], self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 1): Future[Int] = atomic { ClassificationTreeNode.this.route(value, self, strategy, maxSplitDepth)(_,executionContext) }
    def firstNode(self : STMPtr[ClassificationTreeNode]): Future[STMPtr[ClassificationTreeNode]] = atomic { ClassificationTreeNode.this.firstNode(self)(_,executionContext) }
    def nextNode(self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode]): Future[Option[STMPtr[ClassificationTreeNode]]] = atomic { ClassificationTreeNode.this.nextNode(self, root)(_,executionContext) }
    def getTreeId(self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode]): Future[Long] = atomic { ClassificationTreeNode.this.getTreeId(self, root)(_,executionContext) }
    def getByTreeId(cursor: Long, self : STMPtr[ClassificationTreeNode]): Future[STMPtr[ClassificationTreeNode]] = atomic { ClassificationTreeNode.this.getByTreeId(cursor, self)(_,executionContext) }

    def nextBlock(cursor:Long, self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode]): Future[(Long, Stream[LabeledItem])] = {
      if(cursor < 0) {
        Future.successful( (cursor-1) -> Stream.empty )
      } else {
        val cursorPtr: Future[STMPtr[ClassificationTreeNode]] = if(cursor == 0) {
          firstNode(self)
        } else {
          getByTreeId(cursor, self)
        }
        cursorPtr.flatMap(nodePtr => {
          nodePtr.atomic.read.flatMap((node: ClassificationTreeNode) => {
            val members: Future[Stream[LabeledItem]] = node.atomic().getMembers(nodePtr)
            val nextId: Future[Long] = node.atomic().nextNode(nodePtr, root).flatMap(_.map((y: STMPtr[ClassificationTreeNode]) =>
              y.atomic.read.flatMap(_.atomic().getTreeId(y, root))).getOrElse(Future.successful(-1)))
            members.flatMap(members=>nextId.map(nextId=>{
              nextId -> members
            }))
          })
        })
      }
    }


    def getMembers(self : STMPtr[ClassificationTreeNode]): Future[Stream[LabeledItem]] = Future.successful {
        itemBuffer.map((x: BatchedTreeCollection[LabeledItem]) =>x.atomic().stream()).getOrElse(Stream.empty)
    }

    def stream(self : STMPtr[ClassificationTreeNode]): Future[Stream[LabeledItem]] = Future.successful {
      val cursorStream: Stream[(Long, Stream[LabeledItem])] = Stream.iterate((0l, Stream.empty[LabeledItem]))(t => sync.nextBlock(t._1, self, self))
      val itemStream: Stream[LabeledItem] = cursorStream.takeWhile(_._1 > -2).flatMap(_._2)
      itemStream
    }

  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new NodeAtomicApi(priority,maxRetries)

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def nextBlock(value: Long, self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode])
                 (implicit ctx: STMTxnCtx, executionContext: ExecutionContext): (Long, Stream[LabeledItem]) =
      sync { ClassificationTreeNode.this.nextBlock(value, self, root) }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)



  def getInfo(self:STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[NodeInfo] = {
    getTreeId(self, root).flatMap(id=>{
      val nodeInfo = NodeInfo(self, id)
      parent.map(parent => parent.read().flatMap(_.getInfo(parent, root)).map(parent => nodeInfo.copy(parent = Option(parent))))
        .getOrElse(Future.successful(nodeInfo))
    })
  }

  def find(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[STMPtr[ClassificationTreeNode]]] = {
    if(rule.isDefined) {
      try {
        if(rule.get.deserialize().get(value)) {
          pass.get.read().flatMap(_.find(value)).map(_.orElse(pass))
        } else {
          fail.get.read().flatMap(_.find(value)).map(_.orElse(fail))
        }
      } catch {
        case _: Throwable =>
          exception.get.read().flatMap(_.find(value)).map(_.orElse(exception))
      }
    } else {
      Future.successful(None)
    }
  }



  def add(value: List[LabeledItem], self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 1)
         (implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Int] = {
    if(itemBuffer.isDefined) {
      itemBuffer.get.add(value.toArray.toList)
        .flatMap(_=>autosplit(self, strategy, maxSplitDepth))
    } else {
      route(value, self, strategy, maxSplitDepth)
    }
  }


  private def autosplit(self: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int)
                       (implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Int] = {
    if (splitTask.isEmpty && maxSplitDepth > 0 && strategy.split(itemBuffer.get)) {
      self.lock().flatMap(locked => {
        if (locked) {
          Option(StmExecutionQueue.get()).map(_.add(ClassificationTreeNode.splitTaskFn(self, strategy, maxSplitDepth)).flatMap(
            (task: Task[String]) => {
              self.write(ClassificationTreeNode.this.copy(splitTask = Option(task))).map(_ => 0)
            }
          )).getOrElse({
            System.err.println("StmExecutionQueue not initialized - cannot queue split")
            Future.successful(0)
          })
        } else {
          Future.successful(0)
        }
      })
    } else {
      Future.successful(0)
    }
  }

  private def route(value: List[LabeledItem], self : STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int)
                   (implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Int] = {
    require(pass.isDefined)
    require(fail.isDefined)
    require(exception.isDefined)
    try {
      val deserializedRule = rule.get.deserialize().get
      val results: Map[Boolean, List[LabeledItem]] = value.groupBy(x => deserializedRule(x.value))
      Future.sequence(List(
        if (results.get(true).isDefined) {
          pass.get.read().flatMap(_.add(results(true), pass.get, strategy, maxSplitDepth))
        } else {
          Future.successful(0)
        },
        if (results.get(false).isDefined) {
          fail.get.read().flatMap(_.add(results(false), fail.get, strategy, maxSplitDepth))
        } else {
          Future.successful(0)
        }
      )).map(_.reduceOption(_ + _).getOrElse(0))
    } catch {
      case _: Throwable =>
        exception.get.read().flatMap(_.add(value, exception.get, strategy, maxSplitDepth))
    }
  }

  def firstNode(self : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[STMPtr[ClassificationTreeNode]] = {
    pass.orElse(fail).orElse(exception)
      .map(ptr => ptr.read().flatMap(_.firstNode(ptr)))
      .getOrElse(Future.successful(self))
  }

  def nextNode(self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode])
              (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Option[STMPtr[ClassificationTreeNode]]] = {
    parent.filterNot(_=>self==root).map(parentPtr => parentPtr.read().flatMap(parentNode => {
      (Option(self) match {
        case parentNode.pass => parentNode.fail.orElse(parentNode.exception)
          .map(nodePtr => nodePtr.read().flatMap(_.firstNode(nodePtr)))
          .getOrElse(Future.successful(parentPtr))
        case parentNode.fail => parentNode.exception
          .map(nodePtr => nodePtr.read().flatMap(_.firstNode(nodePtr)))
          .getOrElse(Future.successful(parentPtr))
        case parentNode.exception => Future.successful(parentPtr)
        case _ => Future.failed(new IllegalStateException(s"$self not found in $parentPtr"))
      }).map((ptr: STMPtr[ClassificationTreeNode]) =>{
        require(self != ptr)
        Option(ptr)
      })
    })).getOrElse(Future.successful(None))
  }

  def recursePath(self : STMPtr[ClassificationTreeNode], path: List[Int])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[STMPtr[ClassificationTreeNode]] = {
    if(path.isEmpty) Future.successful(self)
    else {
      path.head match {
        case 0 =>
          pass.get.readOpt().map(_.getOrElse({
            throw new NoSuchElementException
          })).flatMap(_.recursePath(pass.get, path.tail))
        case 1 =>
          fail.get.readOpt().map(_.getOrElse({
            throw new NoSuchElementException
          })).flatMap(_.recursePath(fail.get, path.tail))
        case 2 =>
          exception.get.readOpt().map(_.getOrElse({
            throw new NoSuchElementException
          })).flatMap(_.recursePath(exception.get, path.tail))
      }
    }
  }

  def getByTreeId(cursor: Long, self : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[STMPtr[ClassificationTreeNode]] = {
    require(0 <= cursor)
    if(1 == cursor) {
      Future.successful(self)
    } else {
      var depth = 0
      var lastCounter = 1l
      var counter = 2l
      while(counter <= cursor) {
        depth = depth + 1
        val levelSize = Math.pow(3, depth).toLong
        lastCounter = counter
        counter = counter + levelSize
      }
      var path = Util.toDigits(cursor - lastCounter, 3)
      while(path.size < depth) path = List(0) ++ path
      recursePath(self, path)
    }
  }

  private def getTreeBit(node:STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Int = {
    if(pass.exists(_ == node)) 0
    else if(fail.exists(_ == node)) 1
    else if(exception.exists(_ == node)) 2
    else throw new RuntimeException()
  }

  def getTreeId(self:STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Long] = {
    getTreeId_Minus1(self, root).map(_+1)
  }

  def getTreeId_Minus1(self:STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Long] = {
    parent.filterNot(_=>self==root)
      .map(parentPtr=>parentPtr.read().flatMap(parentNode => {
        parentNode.getTreeId_Minus1(parent.get, root).map(parentId=>{
          val bit: Int = parentNode.getTreeBit(self)
          parentId * 3 + bit + 1
        })
      })).getOrElse(Future.successful(0l))
      .map(id=>{
        if(id < 0) throw new RuntimeException("Node is too deep to calculate id")
        id
      })
  }

  def stream(self : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Stream[LabeledItem] = {
    Stream.iterate((0l, Stream.empty[LabeledItem]))(t => sync.nextBlock(t._1, self, self)).takeWhile(_._1 > -2).flatMap(_._2)
  }

  def nextBlock(cursor:Long, self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[(Long, Stream[LabeledItem])] = {
    if(cursor < 0) {
      Future.successful( (cursor-1) -> Stream.empty )
    } else {
      val cursorPtr: Future[STMPtr[ClassificationTreeNode]] = if(cursor == 0) {
        firstNode(self)
      } else {
        getByTreeId(cursor, self)
      }
      cursorPtr.flatMap(nodePtr => {
        nodePtr.read().flatMap(node => {
          val members: Stream[LabeledItem] = node.itemBuffer.map(_.stream()).getOrElse(Stream.empty)
          val nextId: Future[Long] = node.nextNode(nodePtr, root).flatMap(_.map((y: STMPtr[ClassificationTreeNode]) =>
            y.read().flatMap(_.getTreeId(y, root))).getOrElse(Future.successful(-1)))
          nextId.map(nextId=>{
            nextId -> members
          })
        })
      })
    }
  }

}

object ClassificationTreeNode {

  def apply(parent : Option[STMPtr[ClassificationTreeNode]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    new ClassificationTreeNode(parent, itemBuffer = Option(BatchedTreeCollection[LabeledItem]()))

  def splitTaskFn(self: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int): (Restm, ExecutionContext) => TaskSuccess[String] =
    (c : Restm, e : ExecutionContext) => {
      println(s"Starting split task for $self")
      val future = ClassificationTreeNode.split(self, strategy, maxSplitDepth)(c,e).map(_=>TaskSuccess("OK"))(e)
      val result = Await.result(future, 10.minutes)
      println(s"Completed split task for $self - $result")
      result
    }


  def split(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 0)
           (implicit cluster: Restm, executionContext: ExecutionContext): Future[Int] = {
    def currentData = self.atomic.sync.read

    val obtainLock: Future[Option[BatchedTreeCollection[LabeledItem]]] = new STMTxn[Option[BatchedTreeCollection[LabeledItem]]] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[BatchedTreeCollection[LabeledItem]]] = {
        //println(s"Obtaining split lock for $self")
        self.read().flatMap(node => {
          if (node.splitBuffer.isDefined) {
            //println(s"Split lock failed for $self")
            Future.successful(None)
          } else if (node.itemBuffer.isDefined) {
            val prevRecieveBuffer = node.itemBuffer.get
            BatchedTreeCollection.create[LabeledItem]().flatMap(newBuffer=>{
              self.write(node.copy(
                itemBuffer = Option(newBuffer),
                splitBuffer = Option(prevRecieveBuffer))
              ).map(_ => prevRecieveBuffer).map(Option(_))
            })
          } else {
            //println(s"Node already split - $self")
            Future.successful(None)
          }
        })
      }
    }.txnRun(cluster)

    def transferBuffers() = new STMTxn[BatchedTreeCollection[LabeledItem]] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[BatchedTreeCollection[LabeledItem]] = {
        //println(s"Swapping queues for $self")
        self.read().flatMap(node => {
          val collection = node.itemBuffer.get
          self.write(node.copy(
            itemBuffer = Option(BatchedTreeCollection[LabeledItem]()),
            splitBuffer = Option(collection))
          ).map(_ => collection)
        })
      }
    }.txnRun(cluster)

    val makeRule: Future[Option[Int]] = obtainLock.flatMap(_.map((itemBuffer: BatchedTreeCollection[LabeledItem]) => {
      println(s"Deriving rule for $self")
      val stream: Stream[LabeledItem] = itemBuffer.atomic().stream()
      val newRule = strategy.getRule(stream)
      println(s"Deriving rule complete for $self")
      if (null != newRule) {
        createChildren(self).flatMap(t => {
          require(!t.contains(null))
          self.atomic.update(_.copy(
            rule = Option(KryoValue(newRule)),
            pass = Option(t(0)),
            fail = Option(t(1)),
            exception = Option(t(2))
          )).map(_=>Option(0))
        })
      } else {
        Future.successful(Option(0))
      }
    }).getOrElse(Future.successful(None)))

    def transferAsync(obtainLock: Future[Option[BatchedTreeCollection[LabeledItem]]] = obtainLock): Future[Option[Int]] = obtainLock.flatMap(lockOpt=>
      makeRule.flatMap(_=>{
        lockOpt.map((itemBuffer: BatchedTreeCollection[LabeledItem]) => {
          val node = currentData
          val stream: Stream[LabeledItem] = itemBuffer.atomic().stream()
          val routeTasks: Iterator[Future[Int]] = stream.grouped(512).map(_.toList).map((block: List[LabeledItem]) => {
            node.atomic().route(block, self, strategy, maxSplitDepth - 1).map(_ => block.size)
          })
          Future.sequence(routeTasks).map(_.sum).map(sum=>{
            println(s"Routed $sum items for $self")
            Option(sum)
          })
        }).getOrElse(Future.successful(None))
      })
    )

    def transferRecursive(obtainLock: Future[Option[BatchedTreeCollection[LabeledItem]]] = obtainLock): Future[Option[Int]] = {
      transferAsync(obtainLock).flatMap(optRows=>{
        val rows = optRows.getOrElse(0)
        if(rows > 10) {
          transferRecursive(transferBuffers().map(Option(_))).map(_.map(_+rows))
        } else {
          Future.successful(optRows)
        }
      })
    }

    transferRecursive().flatMap(_.map(rowsTransfered=>{
      new STMTxn[Int] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Int] = {
          //println(s"Splitting (sync) on $self")
          self.read().flatMap((node: ClassificationTreeNode) => {
            val stream: Stream[LabeledItem] = node.itemBuffer.get.stream()
            Future.sequence(stream.grouped(512).map(_.toList).map(block => {
              node.route(block, self, strategy, maxSplitDepth - 1).map(_=>block.size)
            })).map(_.sum).flatMap(phase2Transfered => {
              println(s"Finalizing $self after transferring $phase2Transfered items")
              self.write(node.copy(itemBuffer = None,splitBuffer = None)).map(_ => rowsTransfered + phase2Transfered)
            })
          })
        }
      }.txnRun(cluster)
    }).getOrElse(Future.successful(0)))


  }

  private def createChildren(self: STMPtr[ClassificationTreeNode])
                            (implicit cluster: Restm, executionContext: ExecutionContext): Future[List[STMPtr[ClassificationTreeNode]]] =
    new STMTxn[List[STMPtr[ClassificationTreeNode]]] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[List[STMPtr[ClassificationTreeNode]]] = {
        Future.sequence(List(
          STMPtr.dynamic(ClassificationTree.newClassificationTreeNode(Option(self))),
          STMPtr.dynamic(ClassificationTree.newClassificationTreeNode(Option(self))),
          STMPtr.dynamic(ClassificationTree.newClassificationTreeNode(Option(self)))
        ))
      }
    }.txnRun(cluster)

}
