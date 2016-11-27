package stm

import scala.concurrent.ExecutionContext

case class BinaryTreeNode
(
  value: String,
  left: Option[STMPtr[BinaryTreeNode]] = None,
  right: Option[STMPtr[BinaryTreeNode]] = None
) {

  def +=(newValue: String)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): BinaryTreeNode = {
    if (value.compareTo(newValue) < 0) {
      left.map(leftPtr => {
        leftPtr <<= (leftPtr.get += newValue)
        BinaryTreeNode.this
      }).getOrElse({
        this.copy(left = Option(STMPtr.dynamicSync(BinaryTreeNode(newValue))))
      })
    } else {
      right.map(rightPtr => {
        rightPtr <<= (rightPtr.get += newValue)
        BinaryTreeNode.this
      }).getOrElse({
        this.copy(right = Option(STMPtr.dynamicSync(BinaryTreeNode(newValue))))
      })
    }
  }

  def contains(newValue: String)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Boolean = {
    if (value.compareTo(newValue) == 0) {
      true
    } else if (value.compareTo(newValue) < 0) {
      left.exists(_.get.contains(newValue))
    } else {
      right.exists(_.get.contains(newValue))
    }
  }

  private def equalityFields = List(value, left, right)

  override def hashCode(): Int = equalityFields.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: BinaryTreeNode => x.equalityFields == equalityFields
    case _ => false
  }
}
