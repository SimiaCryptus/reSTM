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

package stm.collection

import stm._
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

object Skeleton {
  private implicit def executionContext = StmPool.executionContext

  def createSync[T]()(implicit cluster: Restm, classTag: ClassTag[T]): Skeleton[T] =
    Await.result(new STMTxn[Skeleton[T]] {
      override def txnLogic()(implicit ctx: STMTxnCtx): Future[Skeleton[T]] = {
        create[T]()
      }
    }.txnRun(cluster), 60.seconds)

  def create[T]()(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Skeleton[T]] =
    STMPtr.dynamic(new Skeleton.SkeletonData[T]()).map(new Skeleton(_))

  case class SkeletonData[T]
  (
    // Primary mutable internal data here
  ) {
    // In-txn operations
    def sampleOperation(self: STMPtr[Skeleton.SkeletonData[T]])(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Unit] = {
      self.write(this)
    }
  }

}

class Skeleton[T](rootPtr: STMPtr[Skeleton.SkeletonData[T]]) {
  private implicit def executionContext = StmPool.executionContext
  def id: String = rootPtr.id.toString

  def this(ptr: PointerType) = this(new STMPtr[Skeleton.SkeletonData[T]](ptr))

  def atomic(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm) = new AtomicApi(priority, maxRetries)

  def sync(duration: Duration) = new SyncApi(duration)

  def sync = new SyncApi(10.seconds)

  def sampleOperation()(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Future[Unit]] = {
    getInner().map(inner => {
      inner.sampleOperation(rootPtr)
    })
  }

  private def getInner()(implicit ctx: STMTxnCtx) = {
    rootPtr.readOpt().map(_.orElse(Option(new Skeleton.SkeletonData[T]()))).map(_.get)
  }

  class AtomicApi(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm) extends AtomicApiBase(priority, maxRetries) {

    def sync(duration: Duration) = new SyncApi(duration)

    def sync = new SyncApi(10.seconds)

    def sampleOperation()(implicit classTag: ClassTag[T]): Future[Unit.type] = atomic {
      Skeleton.this.sampleOperation()(_, classTag).map(_ => Unit)
    }

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def sampleOperation()(implicit classTag: ClassTag[T]): Unit.type = sync {
        AtomicApi.this.sampleOperation()
      }
    }

  }

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def sampleOperation()(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Unit] = sync {
      Skeleton.this.sampleOperation()
    }
  }

}

