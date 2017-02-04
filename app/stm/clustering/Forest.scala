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

package stm.clustering

import stm._
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

object Forest {
  private implicit def executionContext = StmPool.executionContext

  def createSync[T]()(implicit cluster: Restm, classTag: ClassTag[T]): Forest[T] =
    Await.result(new STMTxn[Forest[T]] {
      override def txnLogic()(implicit ctx: STMTxnCtx): Future[Forest[T]] = {
        create[T]()
      }
    }.txnRun(cluster), 60.seconds)

  def create[T]()(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Forest[T]] =
    STMPtr.dynamic(new Forest.ForestData[T]()).map(new Forest(_))

  case class ForestData[T]
  (
    trees : List[ClassificationTree] = List.empty
  ) {
    // In-txn operations
    def add(self: STMPtr[Forest.ForestData[T]])(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Unit] = {
      self.write(this)
    }

    def find(self: STMPtr[Forest.ForestData[T]])(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Unit] = {
      self.write(this)
    }

    def sampleOperation(self: STMPtr[Forest.ForestData[T]])(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Unit] = {
      self.write(this)
    }

    def getStrategy(self: STMPtr[Forest.ForestData[T]])(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Unit] = {
      self.write(this)
    }
    def setStrategy(self: STMPtr[Forest.ForestData[T]])(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Unit] = {
      self.write(this)
    }
    def split(self: STMPtr[Forest.ForestData[T]])(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Unit] = {
      self.write(this)
    }
  }

}

class Forest[T](rootPtr: STMPtr[Forest.ForestData[T]]) {
  private implicit def executionContext = StmPool.executionContext
  def id: String = rootPtr.id.toString

  def this(ptr: PointerType) = this(new STMPtr[Forest.ForestData[T]](ptr))

  def atomic(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm) = new AtomicApi(priority, maxRetries)

  def sync(duration: Duration) = new SyncApi(duration)

  def sync = new SyncApi(10.seconds)

  def sampleOperation()(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Future[Unit]] = {
    getInner().map(inner => {
      inner.sampleOperation(rootPtr)
    })
  }

  private def getInner()(implicit ctx: STMTxnCtx) = {
    rootPtr.readOpt().map(_.orElse(Option(new Forest.ForestData[T]()))).map(_.get)
  }

  class AtomicApi(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm) extends AtomicApiBase(priority, maxRetries) {

    def sync(duration: Duration) = new SyncApi(duration)

    def sync = new SyncApi(10.seconds)

    def sampleOperation()(implicit classTag: ClassTag[T]): Future[Unit.type] = atomic {
      Forest.this.sampleOperation()(_, classTag).map(_ => Unit)
    }

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def sampleOperation()(implicit classTag: ClassTag[T]): Unit.type = sync {
        AtomicApi.this.sampleOperation()
      }
    }

  }

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def sampleOperation()(implicit ctx: STMTxnCtx, classTag: ClassTag[T]): Future[Unit] = sync {
      Forest.this.sampleOperation()
    }
  }

}

