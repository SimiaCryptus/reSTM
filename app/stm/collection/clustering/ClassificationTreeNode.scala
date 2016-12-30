package stm.collection.clustering


import stm.collection.BatchedTreeCollection
import stm.collection.clustering.ClassificationTree.{ClassificationTreeItem, LabeledItem, NodeInfo}
import stm.{STMPtr, _}
import storage.Restm
import storage.types.KryoValue

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

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
        pass = Option(STMPtr.dynamicSync(ClassificationTree.newClassificationTreeNode(Option(self)))),
        fail = Option(STMPtr.dynamicSync(ClassificationTree.newClassificationTreeNode(Option(self)))),
        exception = Option(STMPtr.dynamicSync(ClassificationTree.newClassificationTreeNode(Option(self))))
      )
      val result = if(null != nextValue.rule) {
        self.write(nextValue).flatMap(_ => {
          Future.sequence(bufferedValues.grouped(128).map(_.toList).map(item => {
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
        val deserializedRule = rule.get.deserialize().get
        val results: Map[Boolean, List[LabeledItem]] = value.groupBy(x=>deserializedRule(x.value))
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
