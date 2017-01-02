package stm.collection.clustering

import stm.STMTxnCtx
import stm.collection.BatchedTreeCollection
import stm.collection.clustering.ClassificationTree.{ClassificationTreeItem, LabeledItem}

import scala.concurrent.ExecutionContext

class NoBranchStrategy extends ClassificationStrategy {
  override def getRule(values: Stream[LabeledItem]): (ClassificationTreeItem) => Boolean = _ => true

  override def split(buffer: BatchedTreeCollection[LabeledItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Boolean = false
}
