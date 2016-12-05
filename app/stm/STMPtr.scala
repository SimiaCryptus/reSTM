package stm

import stm.lib0.{AtomicApiBase, SyncApiBase}
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect._

object STMPtr {

  def dynamic[T <: AnyRef](value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[STMPtr[T]] = {
    ctx.newPtr(value).map(new STMPtr[T](_))
  }

  def dynamicSync[T <: AnyRef](value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): STMPtr[T] = {
    Await.result(dynamic[T](value), ctx.defaultTimeout)
  }

  def static[T <: AnyRef](id: PointerType)(implicit classTag: ClassTag[T]): STMPtr[T] = new STMPtr[T](id)

  def static[T <: AnyRef](id: PointerType, default: => T)(implicit classTag: ClassTag[T]): STMPtr[T] = new STMPtr[T](id) {
    override def readOpt()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Option[T]] = super.readOpt().map(_.orElse(Option(default)))
  }

  def static[T <: AnyRef](id: PointerType, default: STMTxn[T])(implicit classTag: ClassTag[T]): STMPtr[T] = new STMPtr[T](id) {
    override def readOpt()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Option[T]] = {
      super.readOpt().flatMap(_.map(Future.successful).getOrElse(default.txnLogic())).map(Option(_))
    }
  }

}

class STMPtr[T <: AnyRef](val id: PointerType) {

  def init(default: => T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[STMPtr[T]] = readOpt().flatMap(optValue => {
    optValue.map(_ => Future.successful(this)).getOrElse(write(default).map(_ => this))
  })

  def initOrUpdate(default: => T, upadater: T => T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[STMPtr[T]] = readOpt().flatMap(optValue => {
    val x = optValue.map(upadater).getOrElse(default)
    write(x).map(_ => this)
  })

  def update(upadater: T => T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[STMPtr[T]] = readOpt().flatMap(optValue => {
    write(upadater(optValue.get)).map(_ => this)
  })

  def lock()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Boolean] = ctx.lock(id)
    .recover({ case e => throw new RuntimeException(s"failed lock to $id", e) })

  def readOpt()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Option[T]] = ctx.readOpt[T](id)
    .recover({ case e => throw new RuntimeException(s"failed readOpt to $id", e) })

  def read()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[T] = ctx.readOpt[T](id).map(_.get)

  def read(default: => T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[T] = ctx.readOpt[T](id).map(_.getOrElse(default))

  def write(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Unit] = ctx.write(id, value)
    .recover({ case e => throw new RuntimeException(s"failed write to $id", e) })

  def <=(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = {
    STMPtr.this.write(value)
  }

  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase {

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def readOpt(implicit classTag: ClassTag[T]) = sync(AtomicApi.this.readOpt)
      def init(default: => T)(implicit classTag: ClassTag[T]) = sync(AtomicApi.this.init(default))
      def write(value: T)(implicit classTag: ClassTag[T]) = sync(AtomicApi.this.write(value))
    }

    def sync(defaultTimeout: Duration): SyncApi = new SyncApi(defaultTimeout)
    def sync: SyncApi = sync(1.minutes)

    def readOpt(implicit executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Option[T]] =
      atomic { STMPtr.this.readOpt()(_,executionContext,classTag) }

    def init(default: => T)(implicit executionContext: ExecutionContext, classTag: ClassTag[T]) =
      atomic { STMPtr.this.init(default)(_,executionContext,classTag) }

    def write(value: T)(implicit executionContext: ExecutionContext, classTag: ClassTag[T]) =
      atomic { STMPtr.this.write(value)(_,executionContext,classTag) }
  }
  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def read(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = sync(STMPtr.this.read())
    def init(default: => T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = sync(STMPtr.this.init(default))
    def write(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = sync(STMPtr.this.write(value))
    def <=(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]) = sync(STMPtr.this <= value)
  }
  def sync(defaultTimeout: Duration) = new SyncApi(defaultTimeout)
  def sync(implicit ctx: STMTxnCtx) = new SyncApi(ctx.defaultTimeout)

  private def equalityFields = List(id)
  override def hashCode(): Int = equalityFields.hashCode()
  override def equals(obj: scala.Any): Boolean = obj match {
    case x: STMPtr[_] => x.equalityFields == equalityFields
    case _ => false
  }


}


