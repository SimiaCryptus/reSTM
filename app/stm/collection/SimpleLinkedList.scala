package stm.collection

import stm.{SyncApiBase, _}
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object SimpleLinkedList {
  def create[T](implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    STMPtr.dynamic[SimpleLinkedListHead[T]](new SimpleLinkedListHead[T]()).map(new SimpleLinkedList(_))

  def static[T](id: PointerType) = new SimpleLinkedList(new STMPtr[SimpleLinkedListHead[T]](id))
}

class SimpleLinkedList[T](rootPtr: STMPtr[SimpleLinkedListHead[T]]) {

  def id = rootPtr.id.toString
  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority, maxRetries){
    def add(value: T) = atomic { (ctx: STMTxnCtx) => SimpleLinkedList.this.add(value)(ctx, executionContext) }
    def remove() = atomic { (ctx: STMTxnCtx) => SimpleLinkedList.this.remove()(ctx, executionContext) }
    def size() = atomic { (ctx: STMTxnCtx) => SimpleLinkedList.this.size()(ctx, executionContext) }
    def stream()(implicit cluster: Restm, executionContext: ExecutionContext) = {
      rootPtr.atomic.readOpt.map(_
        .flatMap(_.tail)
        .map(tail=>tail.atomic.sync.readOpt.map(node => node.value -> node.next))
        .map(seed=>{
          Stream.iterate(seed)((prev: Option[(T, Option[STMPtr[SimpleLinkedListNode[T]]])]) => {
            prev.get._2.flatMap((node: STMPtr[SimpleLinkedListNode[T]]) =>
              node.atomic.sync.readOpt.map(node => node.value -> node.next))
          }).takeWhile(_.isDefined).map(_.get._1)
        }).getOrElse(Stream.empty))
    }
    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(value: T) = sync { AtomicApi.this.add(value) }
      def remove() = sync { AtomicApi.this.remove() }
      def stream() = sync { AtomicApi.this.stream() }
      def size() = sync { AtomicApi.this.size() }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)
  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority, maxRetries)
  class SyncApi(duration: Duration)(implicit executionContext: ExecutionContext) extends SyncApiBase(duration) {
    def add(value: T)(implicit ctx: STMTxnCtx) = sync { SimpleLinkedList.this.add(value) }
    def remove()(implicit ctx: STMTxnCtx) = sync { SimpleLinkedList.this.remove() }
    def stream()(implicit ctx: STMTxnCtx) = sync { SimpleLinkedList.this.stream() }
    def size()(implicit ctx: STMTxnCtx) = sync { SimpleLinkedList.this.size() }
  }
  def sync(duration: Duration)(implicit executionContext: ExecutionContext) = new SyncApi(duration)
  def sync(implicit executionContext: ExecutionContext) = new SyncApi(10.seconds)

  def size()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    stream().map(_.size)
  }

  def stream()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt.map(_
      .flatMap(_.tail)
      .map(_.sync.readOpt.map(node => node.value -> node.next))
      .map(seed=>
        Stream.iterate(seed)((prev: Option[(T, Option[STMPtr[SimpleLinkedListNode[T]]])]) =>
          prev.get._2.flatMap((node: STMPtr[SimpleLinkedListNode[T]]) =>
            node.sync.readOpt.map(node => node.value -> node.next)))
        .takeWhile(_.isDefined).map(_.get._1))
      .getOrElse(Stream.empty))
  }

  def add(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Unit] = {
    rootPtr.readOpt().map(_.getOrElse(new SimpleLinkedListHead)).flatMap(_.add(value, rootPtr))
  }

  def remove()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
    rootPtr.readOpt().flatMap(_.map(head=>head.remove(rootPtr)).getOrElse(Future.successful(None)))
  }

  def lock()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Boolean] = {
    rootPtr.lock()
  }
}

private case class SimpleLinkedListHead[T]
(
  head: Option[STMPtr[SimpleLinkedListNode[T]]] = None,
  tail: Option[STMPtr[SimpleLinkedListNode[T]]] = None
) {
  def add(newValue: T, self: STMPtr[SimpleLinkedListHead[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
    val ifDefinedFuture: Option[Future[Unit]] = head.map(nodePtr => {
      nodePtr.read.flatMap(currentValue => {
        require(currentValue.next == None)
        val node = SimpleLinkedListNode(newValue, prev = Option(nodePtr), next = None)
        STMPtr.dynamic(node).flatMap(newPtr => {
          nodePtr.write(currentValue.copy(next = Option(newPtr)))
            .flatMap(_ => {
              self.write(SimpleLinkedListHead.this.copy(head = Option(newPtr)))
          })
        })
      })
    })
    ifDefinedFuture.getOrElse({
      require(tail == None)
      val ptrFuture = STMPtr.dynamic(SimpleLinkedListNode(newValue))
      ptrFuture.flatMap(newNode => self.write(copy(head = Option(newNode), tail = Option(newNode))))
    })
  }

  def remove(self: STMPtr[SimpleLinkedListHead[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
    if (tail.isDefined) {
      val tailPtr: STMPtr[SimpleLinkedListNode[T]] = tail.get
      tailPtr.read.flatMap(tailValue=>{
        require(tailValue.prev == None)
        if (tailValue.next.isDefined) {
          val nextPtr = tailValue.next.get
          val writeFuture: Future[Unit] = nextPtr.read.map(nextValue=>{
            require(nextValue.prev == tail)
            nextValue.copy(prev = None)
          }).flatMap(nextPtr.write(_))
            .flatMap(_ => self.write(copy(tail = Option(nextPtr))))
          writeFuture.map(_=>Option(tailValue.value))
        } else {
          require(tail == head)
          self.write(copy(tail = None, head = None)).map(_=>Option(tailValue.value))
        }
      })
    } else {
      require(head == None)
      Future.successful(None)
    }
  }
}

private case class SimpleLinkedListNode[T]
(
  value: T,
  next: Option[STMPtr[SimpleLinkedListNode[T]]] = None,
  prev: Option[STMPtr[SimpleLinkedListNode[T]]] = None
) {

  private def equalityFields = List(value, next, prev)

  override def hashCode(): Int = equalityFields.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: SimpleLinkedListNode[_] => x.equalityFields == equalityFields
    case _ => false
  }
}
