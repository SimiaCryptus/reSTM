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
  def this(parent : Option[STMPtr[ClassificationTreeNode]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    this(parent, itemBuffer = Option(BatchedTreeCollection[LabeledItem]()))


  class NodeAtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def split(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 0) = sync { NodeAtomicApi.this.split(self, strategy, maxSplitDepth) }
      def firstNode(self : STMPtr[ClassificationTreeNode]) = sync { NodeAtomicApi.this.firstNode(self) }
      def nextNode(self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode]) = sync { NodeAtomicApi.this.nextNode(self, root) }
      def getTreeId(self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode]) = sync { NodeAtomicApi.this.getTreeId(self, root) }
      def nextBlock(value: Long, self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode]) = sync { NodeAtomicApi.this.nextBlock(value, self, root) }
      def stream(self : STMPtr[ClassificationTreeNode]) = sync { NodeAtomicApi.this.stream(self) }
      def getMembers(self : STMPtr[ClassificationTreeNode]) = sync { NodeAtomicApi.this.getMembers(self) }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)

    def split(self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 0): Future[Int] = {

      val obtainLock = new STMTxn[Option[BatchedTreeCollection[LabeledItem]]] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          //println(s"Obtaining split lock for $self")
          self.read().flatMap(prev => {
            if (prev.splitBuffer.isDefined) {
              //println(s"Split lock failed for $self")
              Future.successful(None)
            } else if (prev.itemBuffer.isDefined) {
              val collection = prev.itemBuffer.get
              self.write(prev.copy(
                itemBuffer = Option(BatchedTreeCollection[LabeledItem]()),
                splitBuffer = Option(collection))
              ).map(_ => collection).map(Option(_))
            } else {
              //println(s"Node already split - $self")
              Future.successful(None)
            }
          })
        }
      }.txnRun(cluster)

      val transferAsync: Future[Option[Int]] = obtainLock.flatMap(_.map((itemBuffer: BatchedTreeCollection[LabeledItem]) => {
        println(s"Deriving rule for $self")
        val stream: Stream[LabeledItem] = itemBuffer.atomic().stream()
        createChildren(self).flatMap(t => {
          val nextValue: ClassificationTreeNode = copy(
            rule = Option(KryoValue(strategy.getRule(stream))),
            pass = Option(t(0)),
            fail = Option(t(1)),
            exception = Option(t(2))
          )
          if (null != nextValue.rule) {
            println(s"Splitting (async) on $self")
            self.atomic.write(nextValue).flatMap(_ => {
              Future.sequence(stream.grouped(512).map(_.toList).map((block: List[LabeledItem]) => {
                nextValue.atomic().route(block, self, strategy, maxSplitDepth - 1).map(_=>block.size)
              })).map(_.sum).map(sum=>{
                println(s"Routed $sum items for $self")
                sum
              })
            })
          } else {
            Future.successful(0)
          }
        }).map(Option(_))
      }).getOrElse(Future.successful(None)))

      transferAsync.flatMap(_.map(rowsTransfered=>{
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

    private def createChildren(self: STMPtr[ClassificationTreeNode]): Future[List[STMPtr[ClassificationTreeNode]]] = atomic { txn => {
      implicit val _txn = txn
      Future.sequence(List(
        STMPtr.dynamic(ClassificationTree.newClassificationTreeNode(Option(self))),
        STMPtr.dynamic(ClassificationTree.newClassificationTreeNode(Option(self))),
        STMPtr.dynamic(ClassificationTree.newClassificationTreeNode(Option(self)))
      ))
    }}

    def add(value: List[LabeledItem], self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 1) = atomic { ClassificationTreeNode.this.add(value, self, strategy, maxSplitDepth)(_,executionContext) }
    def route(value: List[LabeledItem], self : STMPtr[ClassificationTreeNode], strategy:ClassificationStrategy, maxSplitDepth:Int = 1) = atomic { ClassificationTreeNode.this.route(value, self, strategy, maxSplitDepth)(_,executionContext) }
    def firstNode(self : STMPtr[ClassificationTreeNode]) = atomic { ClassificationTreeNode.this.firstNode(self)(_,executionContext) }
    def nextNode(self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode]) = atomic { ClassificationTreeNode.this.nextNode(self, root)(_,executionContext) }
    def getTreeId(self : STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode]) = atomic { ClassificationTreeNode.this.getTreeId(self, root)(_,executionContext) }
    def getByTreeId(cursor: Long, self : STMPtr[ClassificationTreeNode]) = atomic { ClassificationTreeNode.this.getByTreeId(cursor, self)(_,executionContext) }

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


    def getMembers(self : STMPtr[ClassificationTreeNode]) = Future.successful {
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
                 (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
      sync { ClassificationTreeNode.this.nextBlock(value, self, root) }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)



  def getInfo(self:STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[NodeInfo] = {
    getTreeId(self, root).flatMap(id=>{
      val nodeInfo = new NodeInfo(self, id)
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
        case e : Throwable =>
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
      case e: Throwable =>
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
    if(pass.filter(_==node).isDefined) 0
    else if(fail.filter(_==node).isDefined) 1
    else if(exception.filter(_==node).isDefined) 2
    else throw new RuntimeException()
  }

  def getTreeId(self:STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Long] = {
    getTreeId_Minus1(self, root).map(_+1)
  }

  def getTreeId_Minus1(self:STMPtr[ClassificationTreeNode], root : STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Long] = {
    parent.filterNot(_=>ClassificationTreeNode.this==root)
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
    val cursorStream: Stream[(Long, Stream[LabeledItem])] = Stream.iterate((0l, Stream.empty[LabeledItem]))(t => sync.nextBlock(t._1, self, self))
    cursorStream.takeWhile(_._1 > -2).flatMap(_._2)
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

  def splitTaskFn(self: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int): (Restm, ExecutionContext) => TaskSuccess[String] =
    (c : Restm, e : ExecutionContext) => {
      println(s"Starting split task for $self")
      val future = self.atomic(c,e).sync.read.atomic()(c, e).split(self, strategy, maxSplitDepth).map(_=>new TaskSuccess("OK"))(e)
      val result = Await.result(future, 10.minutes)
      println(s"Completed split task for $self - $result")
      result
    }

}
