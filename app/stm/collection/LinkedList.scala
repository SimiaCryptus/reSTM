package stm.collection

import stm.{SyncApiBase, _}
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object LinkedList {
  def create[T](implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    STMPtr.dynamic[Option[LinkedListHead[T]]](None).map(new LinkedList(_))

  class NonePtr[T](id:PointerType) extends STMPtr[Option[T]](id) {
    override def default(): Future[Option[Option[T]]] = Future.successful(None)
  }
  def static[T](id: PointerType) = new LinkedList(new NonePtr[LinkedListHead[T]](id))
}

class LinkedList[T](rootPtr: STMPtr[Option[LinkedListHead[T]]]) {
  def id = rootPtr.id.toString
  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase{
    def add(value: T, strictness:Double = 1.0) = atomic { (ctx: STMTxnCtx) => LinkedList.this.add(value)(ctx, executionContext) }
    def remove(strictness:Double = 1.0) = atomic { (ctx: STMTxnCtx) => LinkedList.this.remove()(ctx, executionContext) }
    def size() = atomic { (ctx: STMTxnCtx) => LinkedList.this.size()(ctx, executionContext) }
    def stream()(implicit cluster: Restm, executionContext: ExecutionContext) = {
      rootPtr.atomic.readOpt.map(_.flatten.flatMap(_.tail)
        .map(tail=>tail.atomic.sync.readOpt.map(node => node.value -> node.next))
        .map(seed=>Stream.iterate(seed)(prev=>{
          val next: Option[(T, Option[STMPtr[LinkedListNode[T]]])] =
            prev.get._2.flatMap(_.atomic.sync.readOpt.map(node => node.value -> node.next))
          next
        }).takeWhile(_.isDefined).map(_.get._1))
        .getOrElse(Stream.empty))
    }
    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(value: T, strictness:Double = 1.0) = sync { AtomicApi.this.add(value) }
      def remove(strictness:Double = 1.0) = sync { AtomicApi.this.remove() }
      def stream() = sync { AtomicApi.this.stream() }
      def size() = sync { AtomicApi.this.size() }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)
  }
  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi
  class SyncApi(duration: Duration)(implicit executionContext: ExecutionContext) extends SyncApiBase(duration) {
    def add(value: T, strictness:Double = 1.0)(implicit ctx: STMTxnCtx) = sync { LinkedList.this.add(value) }
    def remove(strictness:Double = 1.0)(implicit ctx: STMTxnCtx) = sync { LinkedList.this.remove() }
    def stream()(implicit ctx: STMTxnCtx) = sync { LinkedList.this.stream() }
    def size()(implicit ctx: STMTxnCtx) = sync { LinkedList.this.size() }
  }
  def sync(duration: Duration)(implicit executionContext: ExecutionContext) = new SyncApi(duration)
  def sync(implicit executionContext: ExecutionContext) = new SyncApi(10.seconds)

  def size()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    stream().map(_.size)
  }

  def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.read.map(_.flatMap(_.tail)
      .map(_.sync.readOpt.map(node => node.value -> node.next))
      .map(seed=>Stream.iterate(seed)(_.get._2.flatMap(_.sync.readOpt.map(node => node.value -> node.next)))
        .takeWhile(_.isDefined).map(_.get._1))
      .getOrElse(Stream.empty))
  }

  def add(value: T, strictness:Double = 1.0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Unit] = {
    val read: Future[LinkedListHead[T]] = rootPtr.readOpt().map(_.flatten).map(_.getOrElse(new LinkedListHead))
    val update: Future[LinkedListHead[T]] = read.map(_.add(value, strictness))
    update.flatMap(newRootData => rootPtr.write(Option(newRootData)))
  }

  def remove(strictness:Double = 1.0)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
    rootPtr.readOpt()
      .map(_.flatten)
      .map(_.map(r => r.remove(strictness)))
      .flatMap(_.map(newRootTuple => {
        val (newRoot, removedItem) = newRootTuple
        rootPtr.write(Option(newRoot)).map(_ => removedItem)
      }).getOrElse(Future.successful(None)))
  }
}

private case class LinkedListHead[T]
(
  head: Option[STMPtr[LinkedListNode[T]]] = None,
  tail: Option[STMPtr[LinkedListNode[T]]] = None
) {
  def add(newValue: T, strictness:Double)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): LinkedListHead[T] = {
    if (head.isDefined) {
      def insertAt(nodePtr: STMPtr[LinkedListNode[T]] = head.get): LinkedListHead[T] = {
        val currentValue: LinkedListNode[T] = nodePtr.sync.read
        if(currentValue.prev.isDefined && Random.nextDouble() > strictness) {
          insertAt(currentValue.prev.get)
        } else {
          val newPtr = STMPtr.dynamicSync(LinkedListNode(newValue, prev = Option(nodePtr), next = currentValue.next))
          nodePtr.sync <= currentValue.copy(next = Option(newPtr))
          if (currentValue.next.isDefined) {
            currentValue.next.foreach(ptr=>ptr.sync <= ptr.sync.read.copy(prev = Option(newPtr)))
            LinkedListHead.this
          } else {
            LinkedListHead.this.copy(head = Option(newPtr))
          }
        }
      }
      insertAt()
    } else {
      val newRoot = STMPtr.dynamicSync(LinkedListNode(newValue))
      copy(head = Option(newRoot), tail = Option(newRoot))
    }
  }

  def remove(strictness:Double)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): (LinkedListHead[T], Option[T]) = {
    if (tail.isDefined) {
      def removeAt(nodePtr: STMPtr[LinkedListNode[T]] = tail.get): (LinkedListHead[T],Option[T]) = {
        val currentValue: LinkedListNode[T] = nodePtr.sync.read
        if(currentValue.next.isDefined && Random.nextDouble() > strictness) {
          removeAt(currentValue.next.get)
        } else {
          currentValue.next.foreach(ptr => ptr.sync <= ptr.sync.read.copy(prev = currentValue.prev))
          currentValue.prev.foreach(ptr => ptr.sync <= ptr.sync.read.copy(next = currentValue.next))
          if(currentValue.prev.isDefined && currentValue.next.isDefined) {
            (LinkedListHead.this, Option(currentValue.value))
          } else if(currentValue.next.isDefined) {
            (LinkedListHead.this.copy(tail = currentValue.next), Option(currentValue.value))
          } else if(currentValue.prev.isDefined) {
            (LinkedListHead.this.copy(head = currentValue.prev), Option(currentValue.value))
          } else {
            (LinkedListHead.this.copy(head = None, tail = None), Option(currentValue.value))
          }
        }
      }
      removeAt()
    } else {
      (this, None)
    }
  }

}

private case class LinkedListNode[T]
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
