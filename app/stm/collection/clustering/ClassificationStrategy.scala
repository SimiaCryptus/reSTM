package stm.collection.clustering

import stm.STMTxnCtx
import stm.collection.BatchedTreeCollection
import stm.collection.clustering.ClassificationTree.ClassificationTreeItem

import scala.concurrent.ExecutionContext


trait ClassificationStrategy {

  def getRule(values: Stream[ClassificationTree.LabeledItem]): (ClassificationTreeItem) => Boolean

  def split(buffer: BatchedTreeCollection[ClassificationTree.LabeledItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Boolean

}


