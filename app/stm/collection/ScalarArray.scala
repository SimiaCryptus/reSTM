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

import java.lang.Double

import stm._
import storage.Restm.PointerType
import storage.{Restm, TransactionConflict}

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random

object ScalarArray {
  private implicit def executionContext = StmPool.executionContext

  def createSync(size: Int = 8)(implicit cluster: Restm): ScalarArray =
    Await.result(new STMTxn[ScalarArray] {
      override def txnLogic()(implicit ctx: STMTxnCtx): Future[ScalarArray] = {
        create(size)
      }
    }.txnRun(cluster), 60.seconds)

  def create(size: Int = 8)(implicit ctx: STMTxnCtx): Future[ScalarArray] =
    Future.sequence((1 to size).map(_ => STMPtr.dynamic(Double.valueOf(0.0)))).flatMap(ptrs => {
      STMPtr.dynamic(ScalarArray.ScalarData(ptrs.toList)).map(new ScalarArray(_))
    })

  case class ScalarData(values: List[STMPtr[java.lang.Double]] = List.empty) {

    def add(value: Double, rootPtr: STMPtr[ScalarData])(implicit ctx: STMTxnCtx): Future[Unit] = {
      val shuffledLists = values.map(_ -> Random.nextDouble()).sortBy(_._2).map(_._1)

      def add(list: Seq[STMPtr[java.lang.Double]] = shuffledLists): Future[Unit] = {
        if (list.isEmpty) throw new TransactionConflict("Could not lock any queue") else {
          val head = list.head
          val tail: Seq[STMPtr[java.lang.Double]] = list.tail
          head.lock().flatMap(locked => {
            if (locked) {
              val read: Future[Double] = head.readOpt().map(x ⇒ x.getOrElse(0.0))
              read.map(_ + value).flatMap(head.write(_))
            } else {
              add(tail)
            }
          })
        }
      }

      add()
    }

    def get(rootPtr: STMPtr[ScalarData])(implicit ctx: STMTxnCtx): Future[scala.Double] = {
      Future.sequence(values.map({
        _.readOpt().map(_.getOrElse(0.0)).asInstanceOf[Future[Double]]
      })).map((sequence: List[java.lang.Double]) => {
        sequence.map(_.toDouble).reduceOption(_ + _).getOrElse(0.0)
      })
    }

  }

}

class ScalarArray(rootPtr: STMPtr[ScalarArray.ScalarData]) {
  private implicit def executionContext = StmPool.executionContext

  def id: String = rootPtr.id.toString

  def this(ptr: PointerType) = this(new STMPtr[ScalarArray.ScalarData](ptr))

  def atomic(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm) = new AtomicApi(priority, maxRetries)

  def sync(duration: Duration) = new SyncApi(duration)

  def sync = new SyncApi(10.seconds)

  def add(value: Double)(implicit ctx: STMTxnCtx): Future[Unit] = {
    getInner().flatMap(inner => {
      inner.add(value, rootPtr)
    })
  }

  private def getInner()(implicit ctx: STMTxnCtx) = {
    rootPtr.readOpt().map(_.orElse(Option(ScalarArray.ScalarData()))).map(_.get)
  }

  def get()(implicit ctx: STMTxnCtx): Future[scala.Double] = {
    getInner().flatMap(inner => {
      inner.get(rootPtr)
    })
  }

  private def this() = this(null: STMPtr[ScalarArray.ScalarData])

  class AtomicApi(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm) extends AtomicApiBase(priority, maxRetries) {

    def sync(duration: Duration) = new SyncApi(duration)

    def sync = new SyncApi(10.seconds)

    def add(value: Double): Future[Unit.type] = atomic {
      ScalarArray.this.add(value)(_).map(_ => Unit)
    }

    def get(): Future[scala.Double] = atomic {
      ScalarArray.this.get()(_)
    }

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(value: Double): Unit.type = sync {
        AtomicApi.this.add(value)
      }

      def get(): scala.Double = sync {
        AtomicApi.this.get()
      }
    }

  }

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def add(value: Double)(implicit ctx: STMTxnCtx): Unit = sync {
      ScalarArray.this.add(value)
    }

    def get()(implicit ctx: STMTxnCtx): scala.Double = sync {
      ScalarArray.this.get()
    }
  }

}

