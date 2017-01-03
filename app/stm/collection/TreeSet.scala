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
import stm.collection.TreeSet.TreeSetNode
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


object TreeSet {

  def create[T <: Comparable[T]]()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[TreeSet[T]] = {
    STMPtr.dynamic(null: TreeSetNode[T]).map(new TreeSet[T](_))
  }

  private case class TreeSetNode[T <: Comparable[T]]
  (
    value: T,
    left: Option[STMPtr[TreeSetNode[T]]] = None,
    right: Option[STMPtr[TreeSetNode[T]]] = None
  ) {

    def min()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[T] = {
      right.map(_.read.flatMap(_.min)).getOrElse(Future.successful(value))
    }

    def max()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[T] = {
      left.map(_.read.flatMap(_.max)).getOrElse(Future.successful(value))
    }

    def remove(newValue: T, self: STMPtr[TreeSetNode[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Boolean] = {
      val compare: Int = value.compareTo(newValue)
      if (compare == 0) {
        if (left.isEmpty && right.isEmpty) {
          self.delete().map(_ => true)
        } else if (left.isDefined) {
          left.map(leftPtr => {
            leftPtr.read.flatMap(leftNode => {
              leftNode.min().flatMap(minValue => {
                leftNode.remove(minValue, leftPtr).flatMap(result => {
                  if (result) self.write(TreeSetNode.this.copy(left = None, value = minValue)).map(_ => false)
                  else self.write(TreeSetNode.this.copy(value = minValue)).map(_ => false)
                })
              })
            })
          }).get
        } else {
          right.map(rightPtr => {
            rightPtr.read.flatMap(rightNode => {
              rightNode.max().flatMap(maxValue => {
                rightNode.remove(maxValue, rightPtr).flatMap(result => {
                  if (result) self.write(TreeSetNode.this.copy(right = None, value = maxValue)).map(_ => false)
                  else self.write(TreeSetNode.this.copy(value = maxValue)).map(_ => false)
                })
              })
            })
          }).get
        }
      } else if (compare < 0) {
        left.map(leftPtr => {
          leftPtr.read.flatMap(_.remove(newValue, leftPtr).flatMap(result => {
            if (result) self.write(TreeSetNode.this.copy(left = None)).map(_ => false)
            else Future.successful(result)
          }))
        }).getOrElse({
          throw new NoSuchElementException
        })
      } else {
        right.map(rightPtr => {
          rightPtr.read().flatMap(_.remove(newValue, rightPtr).flatMap(result => {
            if (result) self.write(TreeSetNode.this.copy(right = None)).map(_ => false)
            else Future.successful(result)
          }))
        }).getOrElse({
          throw new NoSuchElementException
        })
      }
    }

    def add(newValue: T, self: STMPtr[TreeSetNode[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
      if (value.compareTo(newValue) < 0) {
        left.map(leftPtr => {
          leftPtr.read.flatMap(_.add(newValue, leftPtr))
        }).getOrElse({
          val newNode = STMPtr.dynamic(TreeSetNode(newValue))
          newNode.flatMap(newNode⇒self.write(this.copy(left = Option(newNode))))

        })
      } else {
        right.map(rightPtr => {
          rightPtr.read.flatMap(_.add(newValue, rightPtr))
        }).getOrElse({
          val newNode = STMPtr.dynamic(TreeSetNode(newValue))
          newNode.flatMap(newNode⇒self.write(this.copy(right = Option(newNode))))
        })
      }
    }

    def contains(newValue: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Boolean] = {
      if (value.compareTo(newValue) == 0) {
        Future.successful(true)
      } else if (value.compareTo(newValue) < 0) {
        left.map(_.read.flatMap(_.contains(newValue))).getOrElse(Future.successful(false))
      } else {
        right.map(_.read.flatMap(_.contains(newValue))).getOrElse(Future.successful(false))
      }
    }

    override def hashCode(): Int = equalityFields.hashCode()

    private def equalityFields = List(value, left, right)

    override def equals(obj: scala.Any): Boolean = obj match {
      case x: TreeSetNode[_] => x.equalityFields == equalityFields
      case _ => false
    }
  }

}

class TreeSet[T <: Comparable[T]](rootPtr: STMPtr[TreeSetNode[T]]) {

  def this(ptr: PointerType) = this(new STMPtr[TreeSetNode[T]](ptr))

  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi

  def sync(duration: Duration) = new SyncApi(duration)

  def sync = new SyncApi(10.seconds)

  def add(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
    ctx.log(s"Add $value").flatMap(_⇒{
      rootPtr.readOpt().flatMap(
        _.map(_.add(value, rootPtr))
          .getOrElse(rootPtr.write(TreeSetNode(value))))
    })
  }

  def remove(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Boolean] = {
    ctx.log(s"Remove $value").flatMap(_⇒{
      rootPtr.readOpt().flatMap(
        _.map(r => {
          try {
            r.remove(value, rootPtr).map(_ => true).recover({ case _: NoSuchElementException => false })
          } catch {
            case _: NoSuchElementException => Future.successful(false)
          }
        }).getOrElse(Future.successful(false)
        ))
    })
  }

  def contains(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Boolean] = {
    ctx.log(s"Contains $value").flatMap(_⇒{
      rootPtr.readOpt().flatMap(_.map(_.contains(value)).getOrElse(Future.successful(false)))
    })
  }

  def min(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
    rootPtr.readOpt().flatMap(prev => {
      prev.map(_.min().map(Option(_))).getOrElse(Future.successful(None))
    })
  }

  def max(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[T]] = {
    rootPtr.readOpt().flatMap(prev => {
      prev.map(_.max().map(Option(_))).getOrElse(Future.successful(None))
    })
  }

  private def this() = this(new PointerType)

  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase {

    def sync(duration: Duration) = new SyncApi(duration)

    def sync = new SyncApi(10.seconds)

    def add(key: T): Future[Unit.type] = atomic {
      TreeSet.this.add(key)(_, executionContext).map(_ => Unit)
    }

    def remove(key: T): Future[Boolean] = atomic {
      TreeSet.this.remove(key)(_, executionContext)
    }

    def contains(key: T): Future[Boolean] = atomic {
      TreeSet.this.contains(key)(_, executionContext)
    }

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(key: T): Unit.type = sync {
        AtomicApi.this.add(key)
      }

      def remove(value: T): Boolean = sync {
        AtomicApi.this.remove(value)
      }

      def contains(value: T): Boolean = sync {
        AtomicApi.this.contains(value)
      }
    }

  }

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def add(key: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Unit = sync {
      TreeSet.this.add(key)
    }

    def remove(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Boolean = sync {
      TreeSet.this.remove(value)
    }

    def contains(value: T)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Boolean = sync {
      TreeSet.this.contains(value)
    }
  }

}
