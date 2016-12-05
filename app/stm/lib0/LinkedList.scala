package stm.lib0

import stm._
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object LinkedList {
  def create[T <: AnyRef](implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    STMPtr.dynamic[Option[LinkedListHead[T]]](None).map(new LinkedList(_))

  def static[T <: AnyRef](id: PointerType) = new LinkedList(STMPtr.static[Option[LinkedListHead[T]]](id, None))
}

class LinkedList[T <: AnyRef](rootPtr: STMPtr[Option[LinkedListHead[T]]]) {

  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase{
    def add(value: T) = atomic { (ctx: STMTxnCtx) => LinkedList.this.add(value)(ctx, executionContext) }
    def remove() = atomic { (ctx: STMTxnCtx) => LinkedList.this.remove()(ctx, executionContext) }
    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(value: T) = sync { AtomicApi.this.add(value) }
      def remove() = sync { AtomicApi.this.remove() }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)
  }
  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi
  class SyncApi(duration: Duration)(implicit executionContext: ExecutionContext) extends SyncApiBase(duration) {
    def add(value: T)(implicit ctx: STMTxnCtx) = sync { LinkedList.this.add(value) }
    def remove()(implicit ctx: STMTxnCtx) = sync { LinkedList.this.remove() }
  }
  def sync(duration: Duration)(implicit executionContext: ExecutionContext) = new SyncApi(duration)
  def sync(implicit executionContext: ExecutionContext) = new SyncApi(10.seconds)

  def stream()(implicit cluster: Restm, executionContext: ExecutionContext) : Stream[T] = {
    rootPtr.atomic.sync.readOpt.flatten.flatMap(_.tail).map(
      _.atomic.sync.readOpt.map(node=>node.value -> node.next)
    ).map(Stream.iterate(_)(_.get._2.flatMap(
      _.atomic.sync.readOpt.map(node=>node.value -> node.next)
    )).takeWhile(_.isDefined).map(_.get._1)).getOrElse(Stream.empty)
  }

  def add(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Unit] = {
    val read: Future[LinkedListHead[T]] = rootPtr.readOpt().map(_.flatten).map(_.getOrElse(new LinkedListHead))
    val update: Future[LinkedListHead[T]] = read.map(_.add(value))
    update.flatMap(newRootData => rootPtr.write(Option(newRootData)))
  }

  def remove()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
    rootPtr.readOpt()
      .map(_.flatten)
      .map(_.map(r => r.remove()))
      .flatMap(_.map(newRootTuple => {
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
    if (head.isDefined) {
      val newNodeAddr = STMPtr.dynamicSync(LinkedListNode(newValue, prev = head))
      val headPtr: STMPtr[LinkedListNode[T]] = head.get
      headPtr <= headPtr.sync.read.copy(next = Option(newNodeAddr))
      copy(head = Option(newNodeAddr))
    } else {
      val newRoot: (STMPtr[LinkedListNode[T]]) = STMPtr.dynamicSync(LinkedListNode(newValue))
      copy(head = Option(newRoot), tail = Option(newRoot))
    }
  }

  def remove()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): (LinkedListHead[T], Option[T]) = {
    if (head.isDefined) {
      val tailNode: LinkedListNode[T] = tail.get.sync.read
      val newTailAddr = tailNode.next
      if (newTailAddr.isDefined) {
        newTailAddr.get <= newTailAddr.get.sync.read.copy(prev = None)
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
  next: Option[STMPtr[LinkedListNode[T]]] = None,
  prev: Option[STMPtr[LinkedListNode[T]]] = None
) {

  private def equalityFields = List(value, next, prev)

  override def hashCode(): Int = equalityFields.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: LinkedListNode[_] => x.equalityFields == equalityFields
    case _ => false
  }
}
