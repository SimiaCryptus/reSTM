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

import scala.concurrent.Future
import scala.concurrent.duration._

object TreeMap {
  private implicit def executionContext = StmPool.executionContext

  def empty[T <: Comparable[T], V] = new STMTxn[TreeMap[T, V]] {
    override def txnLogic()(implicit ctx: STMTxnCtx): Future[TreeMap[T, V]] = create[T, V]
  }

  def create[T <: Comparable[T], V](implicit ctx: STMTxnCtx): Future[TreeMap[T, V]] = STMPtr.dynamic[Option[BinaryTreeMapNode[T, V]]](None).map(new TreeMap(_))

}

class TreeMap[T <: Comparable[T], V](rootPtr: STMPtr[Option[BinaryTreeMapNode[T, V]]]) {
  implicit def executionContext = StmPool.executionContext

  def this(ptr: PointerType) = this(new STMPtr[Option[BinaryTreeMapNode[T, V]]](ptr))

  def atomic(implicit cluster: Restm) = new AtomicApi

  def add(key: T, value: V)(implicit ctx: STMTxnCtx): Future[Unit] = {
    rootPtr.readOpt().map(_.flatten).map(prev => {
      prev.map(r => r += key -> value).getOrElse(new BinaryTreeMapNode[T, V](key, value))
    }).flatMap(newRootData => rootPtr.write(Option(newRootData)))
  }

  def remove(value: T)(implicit ctx: STMTxnCtx): Future[Unit] = {
    rootPtr.readOpt().map(_.flatten).map(_.flatMap(r => r -= value))
      .flatMap(newRootData => rootPtr.write(newRootData))
  }

  def contains(value: T)(implicit ctx: STMTxnCtx): Future[Boolean] = {
    rootPtr.readOpt().map(_.flatten).map(_.exists(_.contains(value)))
  }

  def get(value: T)(implicit ctx: STMTxnCtx): Future[Option[V]] = {
    rootPtr.readOpt().map(_.flatten).flatMap(_.map(_.get(value)).getOrElse(Future.successful(None)))
  }

  def min(value: T)(implicit ctx: STMTxnCtx): Future[Product with Serializable] = {
    rootPtr.readOpt().map(_.flatten).map(_.map(_.min()).getOrElse(None))
  }

  def max(value: T)(implicit ctx: STMTxnCtx): Future[Product with Serializable] = {
    rootPtr.readOpt().map(_.flatten).map(_.map(_.max()).getOrElse(None))
  }

  private def this() = this(new PointerType)

  class AtomicApi()(implicit cluster: Restm) extends AtomicApiBase {

    def sync(duration: Duration) = new SyncApi(duration)

    def sync = new SyncApi(10.seconds)

    def add(key: T, value: V): Future[Unit.type] = atomic {
      TreeMap.this.add(key, value)(_).map(_ => Unit)
    }

    def remove(key: T): Future[Unit] = atomic {
      TreeMap.this.remove(key)(_)
    }

    def contains(key: T): Future[Boolean] = atomic {
      TreeMap.this.contains(key)(_)
    }

    def get(key: T): Future[Option[V]] = atomic {
      TreeMap.this.get(key)(_)
    }

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(key: T, value: V): Unit.type = sync {
        AtomicApi.this.add(key, value)
      }

      def remove(value: T): Unit = sync {
        AtomicApi.this.remove(value)
      }

      def contains(value: T): Boolean = sync {
        AtomicApi.this.contains(value)
      }

      def get(value: T): Option[V] = sync {
        AtomicApi.this.get(value)
      }
    }

  }

}

private case class BinaryTreeMapNode[T <: Comparable[T], V]
(
  key: T,
  value: V,
  left: Option[STMPtr[BinaryTreeMapNode[T, V]]] = None,
  right: Option[STMPtr[BinaryTreeMapNode[T, V]]] = None
) {
  private implicit def executionContext = StmPool.executionContext

  def min()(implicit ctx: STMTxnCtx): BinaryTreeMapNode[T, V] = {
    right.map(_.sync.read.min).getOrElse(this)
  }

  def max()(implicit ctx: STMTxnCtx): BinaryTreeMapNode[T, V] = {
    left.map(_.sync.read.min).getOrElse(this)
  }

  def -=(toRemove: T)(implicit ctx: STMTxnCtx): Option[BinaryTreeMapNode[T, V]] = {
    val compare: Int = key.compareTo(toRemove)
    if (compare == 0) {
      if (left.isEmpty && right.isEmpty) {
        None
      } else if (left.isDefined) {
        left.map(leftPtr => {
          val prevNode: BinaryTreeMapNode[T, V] = leftPtr.sync.read
          val promotedNode = prevNode.min()
          val maybeNode: Option[BinaryTreeMapNode[T, V]] = prevNode -= promotedNode.key
          maybeNode.map(newNode => {
            leftPtr.sync <= newNode
            BinaryTreeMapNode.this.copy(key = promotedNode.key, value = promotedNode.value)
          }).getOrElse(BinaryTreeMapNode.this.copy(left = None, key = promotedNode.key, value = promotedNode.value))
        })
      } else {
        right.map(rightPtr => {
          val prevNode: BinaryTreeMapNode[T, V] = rightPtr.sync.read
          val promotedNode = prevNode.max()
          val maybeNode: Option[BinaryTreeMapNode[T, V]] = prevNode -= promotedNode.key
          maybeNode.map(newNode => {
            rightPtr.sync <= newNode
            BinaryTreeMapNode.this.copy(key = promotedNode.key, value = promotedNode.value)
          }).getOrElse(BinaryTreeMapNode.this.copy(right = None, key = promotedNode.key, value = promotedNode.value))
        })
      }
    } else if (compare < 0) {
      left.map(leftPtr => {
        Option((leftPtr.sync.read -= toRemove).map(newLeft => {
          leftPtr.sync <= newLeft
          BinaryTreeMapNode.this
        }).getOrElse(BinaryTreeMapNode.this.copy(left = None)))
      }).getOrElse({
        throw new NoSuchElementException
      })
    } else {
      right.map(rightPtr => {
        Option((rightPtr.sync.read -= toRemove).map(newRight => {
          rightPtr.sync <= newRight
          BinaryTreeMapNode.this
        }).getOrElse(BinaryTreeMapNode.this.copy(right = None)))
      }).getOrElse({
        throw new NoSuchElementException
      })
    }
  }

  def +=(newValue: (T, V))(implicit ctx: STMTxnCtx): BinaryTreeMapNode[T, V] = {
    if (key.compareTo(newValue._1) < 0) {
      left.map(leftPtr => {
        leftPtr.sync <= (leftPtr.sync.read += newValue)
        BinaryTreeMapNode.this
      }).getOrElse({
        this.copy(left = Option(STMPtr.dynamicSync(BinaryTreeMapNode(newValue._1, newValue._2))))
      })
    } else {
      right.map(rightPtr => {
        rightPtr.sync <= (rightPtr.sync.read += newValue)
        BinaryTreeMapNode.this
      }).getOrElse({
        this.copy(right = Option(STMPtr.dynamicSync(BinaryTreeMapNode(newValue._1, newValue._2))))
      })
    }
  }

  def contains(newValue: T)(implicit ctx: STMTxnCtx): Boolean = {
    if (key.compareTo(newValue) == 0) {
      true
    } else if (key.compareTo(newValue) < 0) {
      left.exists(_.sync.read.contains(newValue))
    } else {
      right.exists(_.sync.read.contains(newValue))
    }
  }

  def get(newValue: T)(implicit ctx: STMTxnCtx): Future[Option[V]] = {
    if (key.compareTo(newValue) == 0) {
      Future.successful(Option(value))
    } else if (key.compareTo(newValue) < 0) {
      left.map(_.read().flatMap(_.get(newValue))).getOrElse(Future.successful(None))
    } else {
      right.map(_.read().flatMap(_.get(newValue))).getOrElse(Future.successful(None))
    }
  }

  override def hashCode(): Int = equalityFields.hashCode()

  private def equalityFields = List(key, left, right)

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: BinaryTreeMapNode[_, _] => x.equalityFields == equalityFields
    case _ => false
  }
}
