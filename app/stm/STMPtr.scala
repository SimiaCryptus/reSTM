/*
 * Copyright (c) 2017 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package stm

import storage.Restm.PointerType
import storage.{Restm, TransactionConflict}
import util.Util

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect._

object STMPtr {

  def dynamicSync[T <: AnyRef](value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): STMPtr[T] = {
    require(null != ctx)
    Await.result(dynamic[T](value), ctx.defaultTimeout)
  }

  def dynamic[T <: AnyRef](value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[STMPtr[T]] = {
    require(null != ctx)
    val future: Future[PointerType] = ctx.newPtr(value)
    require(null != future)
    future.map(new STMPtr[T](_))
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

  def readOpt()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Option[T]] = Util.chainEx(s"failed readOpt to $id") {
    ctx.readOpt[T](id).flatMap(_.map(value => Future.successful(Option(value))).getOrElse(default))
  }

  def default: Future[Option[T]] = Future.successful(None)

  def update(upadater: T => T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[STMPtr[T]] = readOpt().flatMap(optValue => {
    write(upadater(optValue.get)).map(_ => this)
  })

  def lock()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Boolean] = Util.chainEx(s"failed lock on $id") {
    ctx.lockOptional(id)
  }

  def read()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[T] = ctx.readOpt[T](id).map(_.get)

  def read(default: => T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[T] = ctx.readOpt[T](id).map(_.getOrElse(default))

  def <=(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Unit] = {
    STMPtr.this.write(value)
  }

  def write(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Unit] = ctx.write(id, value)
    .recover({ case e => throw new TransactionConflict(s"failed write to $id", e) })

  def <=(value: Option[T])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Unit] = {
    value.map(STMPtr.this.write).getOrElse(STMPtr.this.delete())
  }

  def delete()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Unit] = ctx.delete(id)
    .recover({ case e => throw new TransactionConflict(s"failed write to $id", e) })

  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi

  def sync(defaultTimeout: Duration) = new SyncApi(defaultTimeout)

  def sync(implicit ctx: STMTxnCtx) = new SyncApi(ctx.defaultTimeout)

  override def hashCode(): Int = equalityFields.hashCode()

  private def equalityFields = List(id)

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: STMPtr[_] => x.equalityFields == equalityFields
    case _ => false
  }

  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase {

    def sync: SyncApi = sync(1.minutes)

    def sync(defaultTimeout: Duration): SyncApi = new SyncApi(defaultTimeout)

    def readOpt(implicit executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Option[T]] =
      atomic {
        STMPtr.this.readOpt()(_, executionContext, classTag)
      }

    def read(implicit executionContext: ExecutionContext, classTag: ClassTag[T]): Future[T] =
      atomic {
        STMPtr.this.read()(_, executionContext, classTag)
      }

    def init(default: => T)(implicit executionContext: ExecutionContext, classTag: ClassTag[T]): Future[STMPtr[T]] =
      atomic {
        STMPtr.this.init(default)(_, executionContext, classTag)
      }

    def write(value: T)(implicit executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Unit] =
      atomic {
        STMPtr.this.write(value)(_, executionContext, classTag)
      }

    def update(fn: T => T)(implicit executionContext: ExecutionContext, classTag: ClassTag[T]): Future[Unit] = atomic {
      txn => {
        STMPtr.this.read()(txn, executionContext, classTag)
          .map(fn)
          .flatMap(value => STMPtr.this.write(value)(txn, executionContext, classTag))
      }
    }

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def readOpt(implicit classTag: ClassTag[T]): Option[T] = sync(AtomicApi.this.readOpt)

      def read(implicit classTag: ClassTag[T]): T = sync(AtomicApi.this.read)

      def init(default: => T)(implicit classTag: ClassTag[T]): STMPtr[T] = sync(AtomicApi.this.init(default))

      def write(value: T)(implicit classTag: ClassTag[T]): Unit = sync(AtomicApi.this.write(value))
    }

  }

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def read(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): T = sync(STMPtr.this.read())

    def readOpt(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Option[T] = sync(STMPtr.this.readOpt())

    def init(default: => T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): STMPtr[T] = sync(STMPtr.this.init(default))

    def write(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Unit = sync(STMPtr.this.write(value))

    def <=(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Unit = sync(STMPtr.this <= value)

    def <=(value: Option[T])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext, classTag: ClassTag[T]): Unit = sync(STMPtr.this <= value)
  }


}


