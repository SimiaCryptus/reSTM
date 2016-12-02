package stm

import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}




object LinkedList {
  def empty[T <: AnyRef] = new STMTxn[LinkedList[T]] {
    override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[LinkedList[T]] = create[T]
  }
  def create[T <: AnyRef](implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = STMPtr.dynamic[Option[LinkedListHead[T]]](None).map(new LinkedList(_))

  def static[T <: AnyRef](id:PointerType) = new LinkedList(STMPtr.static[Option[LinkedListHead[T]]](id, None))
}

class LinkedList[T <: AnyRef](rootPtr : STMPtr[Option[LinkedListHead[T]]]) {
  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) {

    class SyncApi(duration:Duration) {
      def add(value: T) = Await.result(AtomicApi.this.add(value), duration)
      def remove() = Await.result(AtomicApi.this.remove(), duration)
    }
    def sync(duration:Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)

    def add(value: T) = new STMTxn[Unit] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] =
        LinkedList.this.add(value).map(_=>Unit)
    }.txnRun(cluster)(executionContext)

    def remove() = new STMTxn[Option[T]] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] =
        LinkedList.this.remove()
    }.txnRun(cluster)(executionContext)

  }
  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi

  def add(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.flatMap(x=>x)).map(_.getOrElse(new LinkedListHead)).map(prev => {
      prev.add(value)
    }).flatMap(newRootData=>rootPtr.write(Option(newRootData)))
  }
  def remove()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Option[T]] = {
    rootPtr.readOpt()
      .map(_.flatMap(x => x))
      .map(_.map(r => r.remove()))
      .flatMap(_.map(newRootTuple=>{
        val (newRoot, removedItem) = newRootTuple
        rootPtr.write(Option(newRoot)).map(_ => removedItem)
      }).getOrElse(Future.successful(None)))
  }
}

private case class LinkedListHead[T <: AnyRef]
(
  head: Option[STMPtr[LinkedListNode[T]]] = None,
  tail: Option[STMPtr[LinkedListNode[T]]] = None
) {
  def add(newValue: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): LinkedListHead[T] = {
    if(head.isDefined) {
      val newNodeAddr = STMPtr.dynamicSync(LinkedListNode(newValue, right = head))
      head.get <= head.get.sync.get.copy(left = Option(newNodeAddr))
      copy(head = Option(newNodeAddr))
    } else {
      val newRoot: (STMPtr[LinkedListNode[_$1]]) forSome {type _$1 >: T <: T} = STMPtr.dynamicSync(LinkedListNode(newValue))
      copy(head = Option(newRoot), tail = Option(newRoot))
    }
  }
  def remove()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): (LinkedListHead[T],Option[T]) = {
    if(head.isDefined) {
      val tailNode: LinkedListNode[T] = tail.get.sync.get
      val newTailAddr = tailNode.left
      if(newTailAddr.isDefined) {
        newTailAddr.get <= newTailAddr.get.sync.get.copy(right = None)
        (copy(tail = newTailAddr), Option(tailNode.value))
      } else {
        (copy(head = None, tail = None), Option(tailNode.value))
      }
    } else {
      (this, None)
    }
  }
}

private case class LinkedListNode[T <: AnyRef]
(
  value: T,
  left: Option[STMPtr[LinkedListNode[T]]] = None,
  right: Option[STMPtr[LinkedListNode[T]]] = None
) {

  private def equalityFields = List(value, left, right)

  override def hashCode(): Int = equalityFields.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: LinkedListNode[_] => x.equalityFields == equalityFields
    case _ => false
  }
}
