package stm

import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag

object STMPtr {

  def dynamic[T <: AnyRef](value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, tag: ClassTag[T]): Future[STMPtr[T]] = {
    ctx.newPtr(value).map(new STMPtr[T](_, tag.runtimeClass.getCanonicalName))
  }

  def dynamicSync[T <: AnyRef](value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, tag: ClassTag[T]): STMPtr[T] = {
    Await.result(dynamic(value), ctx.defaultTimeout)
  }

  def static[T <: AnyRef](id: PointerType)(implicit tag: ClassTag[T]): STMPtr[T] = new STMPtr[T](id, tag.runtimeClass.getCanonicalName)

  def static[T <: AnyRef](id: PointerType, default: => T)(implicit tag: ClassTag[T]): STMPtr[T] = new STMPtr[T](id, tag.runtimeClass.getCanonicalName) {
    override def readOpt()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = super.readOpt().map(_.orElse(Option(default)))
  }

  def static[T <: AnyRef](id: PointerType, default: STMTxn[T])(implicit tag: ClassTag[T]): STMPtr[T] = new STMPtr[T](id, tag.runtimeClass.getCanonicalName) {
    override def readOpt()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
      super.readOpt().flatMap(_.map(Future.successful).getOrElse(default.txnLogic())).map(Option(_))
    }
  }



}

class STMPtr[T <: AnyRef](val id: PointerType, val typeName: String) {
  @transient private[this] lazy val clazz = try {
    require(null != typeName)
    getClass.getClassLoader.loadClass(typeName).asInstanceOf[Class[T]]
  } catch {
    case e : Throwable => throw new RuntimeException("No class: " + typeName, e)
  }

  def init(default: => T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[T]] = readOpt().flatMap(optValue => {
    optValue.map(_=>Future.successful(this)).getOrElse(write(default).map(_=>this))
  })

  def initOrUpdate(default: => T, upadater: T=>T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[T]] = readOpt().flatMap(optValue => {
    val x = optValue.map(upadater).getOrElse(default)
    write(x).map(_=>this)
  })

  def update(upadater: T=>T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[T]] = readOpt().flatMap(optValue => {
    write(upadater(optValue.get)).map(_=>this)
  })

  def initOrUpdate(cluster: Restm, default: => T, upadater: T=>T)(implicit executionContext: ExecutionContext): Future[STMPtr[T]] = new STMTxn[STMPtr[T]] {
    override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
      initOrUpdate(default, upadater)
    }
  }.txnRun(cluster)(executionContext)

  def init(cluster: Restm, default: => T)(implicit executionContext: ExecutionContext): Future[STMPtr[T]] = new STMTxn[STMPtr[T]] {
    override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
      init(default)
    }
  }.txnRun(cluster)(executionContext)

  def lock()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Boolean] = ctx.lock(id)
    .recover({ case e => throw new RuntimeException(s"failed lock to $id", e) })

  def readOpt()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = ctx.readOpt[T](id, clazz)
    .recover({ case e => throw new RuntimeException(s"failed readOpt to $id", e) })

  def read()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[T] = ctx.readOpt[T](id, clazz).map(_.get)
    .recover({ case e => throw new RuntimeException(s"failed read to $id", e) })

  def read(default: => T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[T] = ctx.readOpt[T](id, clazz).map(_.getOrElse(default))
    .recover({ case e => throw new RuntimeException(s"failed read to $id", e) })

  def write(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = ctx.write(id, value, clazz)
    .recover({ case e => throw new RuntimeException(s"failed write to $id", e) })

  def <=(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    STMPtr.this.write(value)
  }

  class SyncApi(defaultTimeout: Duration) {
    def <=(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
      Await.result(STMPtr.this <= value, defaultTimeout)
    }

    implicit def get(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): T = {
      Await.result(STMPtr.this.read(), defaultTimeout)
    }
  }
  def sync(defaultTimeout: Duration) = new SyncApi(defaultTimeout)
  def sync(implicit ctx: STMTxnCtx) = new SyncApi(ctx.defaultTimeout)

  private def equalityFields = List(id, typeName)

  override def hashCode(): Int = equalityFields.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: STMPtr[_] => x.equalityFields == equalityFields
    case _ => false
  }
}


