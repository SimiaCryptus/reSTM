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

import stm.clustering.ClassificationTree.NodeInfo
import stm.clustering.strategy.{ClassificationStrategy, RuleData}
import stm.task.Task.TaskSuccess
import stm.task.{StmExecutionQueue, Task}
import stm.{STMPtr, _}
import storage.Restm
import storage.types.KryoValue
import util.Util

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect._

case class ClassificationTreeNode
(
  parent: Option[STMPtr[ClassificationTreeNode]],
  pass: Option[STMPtr[ClassificationTreeNode]] = None,
  fail: Option[STMPtr[ClassificationTreeNode]] = None,
  exception: Option[STMPtr[ClassificationTreeNode]] = None,
  itemBuffer: Option[PageTree],
  splitBuffer: Option[PageTree] = None,
  splitTask: Option[Task[String]] = None,
  rule: Option[KryoValue[RuleData]] = None
) {

  private implicit def executionContext = StmPool.executionContext

  def atomic(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm) = new NodeAtomicApi(priority, maxRetries)

  def sync(duration: Duration) = new SyncApi(duration)

  def getInfo(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx): Future[NodeInfo] = {
    getTreeId(self, root).flatMap(id => {
      val nodeInfo = NodeInfo(self, id, rule.flatMap(_.deserialize()).map(_.name).orNull)
      parent.map(parent => parent.read().flatMap(_.getInfo(parent, root)).map(parent => nodeInfo.copy(parent = Option(parent))))
        .getOrElse(Future.successful(nodeInfo))
    })
  }

  def find(value: ClassificationTreeItem)(implicit ctx: STMTxnCtx): Future[Option[STMPtr[ClassificationTreeNode]]] = {
    if (rule.isDefined) {
      try {
        if (rule.get.deserialize().get.fn(value)) {
          pass.get.read().flatMap(_.find(value)).map(_.orElse(pass))
        } else {
          fail.get.read().flatMap(_.find(value)).map(_.orElse(fail))
        }
      } catch {
        case _: Throwable =>
          exception.get.read().flatMap(_.find(value)).map(_.orElse(exception))
      }
    } else {
      Future.successful(None)
    }
  }

  def add(value: Page, self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int = 1)
         (implicit ctx: STMTxnCtx): Future[Int] = {
    if (itemBuffer.isDefined) {
      itemBuffer.get.add(value)
        .flatMap(_ => autosplit(self, root, strategy, maxSplitDepth))
    } else {
      route(value, self, root, strategy, maxSplitDepth)
    }
  }

  def firstNode(self: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx): Future[STMPtr[ClassificationTreeNode]] = {
    pass.orElse(fail).orElse(exception)
      .map(ptr => ptr.read().flatMap(_.firstNode(ptr)))
      .getOrElse(Future.successful(self))
  }

  def nextNode(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode])
              (implicit ctx: STMTxnCtx): Future[Option[STMPtr[ClassificationTreeNode]]] = {
    parent.filterNot(_ => self == root).map(parentPtr => parentPtr.read().flatMap(parentNode => {
      (Option(self) match {
        case parentNode.pass => parentNode.fail.orElse(parentNode.exception)
          .map(nodePtr => nodePtr.read().flatMap(_.firstNode(nodePtr)))
          .getOrElse(Future.successful(parentPtr))
        case parentNode.fail => parentNode.exception
          .map(nodePtr => nodePtr.read().flatMap(_.firstNode(nodePtr)))
          .getOrElse(Future.successful(parentPtr))
        case parentNode.exception => Future.successful(parentPtr)
        case _ => Future.failed(new IllegalStateException(s"$self not found in $parentPtr"))
      }).map((ptr: STMPtr[ClassificationTreeNode]) => {
        require(self != ptr)
        Option(ptr)
      })
    })).getOrElse(Future.successful(None))
  }

  def recursePath(self: STMPtr[ClassificationTreeNode], path: List[Int])(implicit ctx: STMTxnCtx): Future[STMPtr[ClassificationTreeNode]] = {
    if (path.isEmpty) Future.successful(self)
    else {
      path.head match {
        case 0 =>
          pass.get.readOpt().map(_.getOrElse({
            throw new NoSuchElementException
          })).flatMap(_.recursePath(pass.get, path.tail))
        case 1 =>
          fail.get.readOpt().map(_.getOrElse({
            throw new NoSuchElementException
          })).flatMap(_.recursePath(fail.get, path.tail))
        case 2 =>
          exception.get.readOpt().map(_.getOrElse({
            throw new NoSuchElementException
          })).flatMap(_.recursePath(exception.get, path.tail))
      }
    }
  }

  def getByTreeId(cursor: BigInt, self: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx): Future[STMPtr[ClassificationTreeNode]] = {
    require(0 <= cursor)
    if (1 == cursor) {
      Future.successful(self)
    } else {
      var depth = 0
      var lastCounter = BigInt(1l)
      var counter = BigInt(2l)
      while (counter <= cursor) {
        depth = depth + 1
        val levelSize = BigInt(3).pow(depth)
        lastCounter = counter
        counter = counter + levelSize
      }
      var path = Util.toDigits(cursor - lastCounter, 3)
      while (path.size < depth) path = List(0) ++ path
      recursePath(self, path)
    }
  }

  def getTreeId(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx): Future[BigInt] = {
    getTreeId_Minus1(self, root).map(_ + 1).flatMap(id⇒{
      root.read().flatMap(rootValue⇒{rootValue.getByTreeId(id, root)})
        .map((verifyNode: STMPtr[ClassificationTreeNode]) ⇒{
          require(verifyNode == self)
          id
        })
    })
  }

  def getTreeId_Minus1(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx): Future[BigInt] = {
    parent.filterNot(_ => self == root)
      .map(parentPtr => parentPtr.read().flatMap(parentNode => {
        parentNode.getTreeId_Minus1(parent.get, root).map(parentId => {
          val bit: Int = parentNode.getTreeBit(self)
          parentId * 3 + bit + 1
        })
      })).getOrElse(Future.successful(BigInt(0l)))
      .map(id => {
        if (id < 0) throw new RuntimeException("Node is too deep to calculate id")
        id
      })
  }

  def getTreeDepth(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx): Future[Int] = {
    parent.filterNot(_ => self == root)
      .map(parentPtr => parentPtr.read().flatMap(parentNode => {
        parentNode.getTreeDepth(parent.get, root).map(parentDepth => {
          val bit: Int = parentNode.getTreeBit(self)
          parentDepth + 1
        })
      })).getOrElse(Future.successful(0))
      .map(id => {
        if (id < 0) throw new RuntimeException("Node is too deep to calculate id")
        id
      })
  }

  def stream(self: STMPtr[ClassificationTreeNode], duration: Duration = 30.seconds)(implicit ctx: STMTxnCtx): Stream[Page#PageRow] = {
    pageStream(self,duration).flatMap(_.rows)
  }

  def pageStream(self: STMPtr[ClassificationTreeNode], duration: Duration = 30.seconds)(implicit ctx: STMTxnCtx): Stream[Page] = {
    Stream.iterate((BigInt(0l), Stream.empty[Page]))(t => sync(duration = duration).nextBlock(t._1, self, self)).takeWhile(_._1 > -2).flatMap(_._2)
  }

  def sync = new SyncApi(10.seconds)

  def nextBlock(cursor: BigInt, self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx): Future[(BigInt, Stream[Page])] = {
    if (cursor < 0) {
      Future.successful((cursor - 1) -> Stream.empty)
    } else {
      val cursorPtr: Future[STMPtr[ClassificationTreeNode]] = if (cursor == 0) {
        firstNode(self)
      } else {
        getByTreeId(cursor, self)
      }
      cursorPtr.flatMap(nodePtr => {
        nodePtr.read().flatMap(node => {
          val members: Stream[Page] = node.itemBuffer.map(_.pageStream()).getOrElse(Stream.empty)
          val nextId: Future[BigInt] = node.nextNode(nodePtr, root).flatMap(_.map((y: STMPtr[ClassificationTreeNode]) =>
            y.read().flatMap(_.getTreeId(y, root))).getOrElse(Future.successful(-1)))
          nextId.map(nextId => {
            nextId -> members
          })
        })
      })
    }
  }

  private def this() = this(None, itemBuffer = None)

  private def autosplit(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int)
                       (implicit ctx: STMTxnCtx): Future[Int] = {
    if (splitTask.isEmpty && maxSplitDepth > 0 && strategy.split(itemBuffer.get)) {
      self.lock().flatMap(locked => {
        if (locked) {
          Option(StmExecutionQueue.get()).map(_.add(ClassificationTreeNode.splitTaskFn(self, root, strategy, maxSplitDepth)).flatMap(
            (task: Task[String]) => {
              self.write(ClassificationTreeNode.this.copy(splitTask = Option(task))).map(_ => 0)
            }
          )).getOrElse({
            System.err.println("StmExecutionQueue not initialized - cannot queue split")
            Future.successful(0)
          })
        } else {
          Future.successful(0)
        }
      })
    } else {
      Future.successful(0)
    }
  }

  def route(value: Page, self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int)
                   (implicit ctx: STMTxnCtx): Future[Int] = {
    val results: Map[Int, Page] = split(value)
    insert(self, root, strategy, maxSplitDepth, results)
  }

  def insert(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int, results: Map[Int, Page])
                    (implicit ctx: STMTxnCtx) = {
    require(pass.isDefined)
    require(fail.isDefined)
    require(exception.isDefined)
    Future.sequence(List(
      if (results.get(0).isDefined) {
        pass.get.read().flatMap(_.add(results(0), pass.get, root, strategy, maxSplitDepth))
      } else {
        Future.successful(0)
      },
      if (results.get(1).isDefined) {
        fail.get.read().flatMap(_.add(results(1), fail.get, root, strategy, maxSplitDepth))
      } else {
        Future.successful(0)
      },
      if (results.get(2).isDefined) {
        exception.get.read().flatMap(_.add(results(2), exception.get, root, strategy, maxSplitDepth))
      } else {
        Future.successful(0)
      }
    )).map(_.reduceOption(_ + _).getOrElse(0))
  }

  def split(value: Page, deserializedRule: (KeyValue[String,Any]) ⇒ Boolean = getRule): Map[Int, Page] = {
    def eval(item: KeyValue[String,Any]): Int = {
      try {
        if (deserializedRule(item)) {
          0
        } else {
          1
        }
      } catch {
        case _: Throwable =>
          2
      }
    }

    val splits = value.rows
      .toParArray // Make parallel
      .groupBy(x => eval(x)).mapValues(_.toList)
      .toList.toMap // Make sequential
    splits.mapValues(value.getAll(_))
  }

  private def getRule: (KeyValue[String,Any]) ⇒ Boolean = {
    rule.flatMap(_.deserialize()).map(_.fn).get
  }

  private def getTreeBit(node: STMPtr[ClassificationTreeNode])(implicit ctx: STMTxnCtx): Int = {
    if (pass.exists(_ == node)) 0
    else if (fail.exists(_ == node)) 1
    else if (exception.exists(_ == node)) 2
    else throw new RuntimeException()
  }

  class NodeAtomicApi(priority: Duration = 0.seconds, maxRetries: Int = 20)(implicit cluster: Restm) extends AtomicApiBase(priority, maxRetries) {

    def sync(duration: Duration) = new SyncApi(duration)

    def add(value: Page, self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int = 1): Future[Int] = atomic {
      ClassificationTreeNode.this.add(value, self, root, strategy, maxSplitDepth)(_)
    }

    def route(value: Page, self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int = 1): Future[Int] = {
      val results: Map[Int, Page] = split(value)
      insert(self, root, strategy, maxSplitDepth, results)
    }

    def insert(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int, results: Map[Int, Page]) = {
      require(pass.isDefined)
      require(fail.isDefined)
      require(exception.isDefined)
      Future.sequence(List(
        if (results.get(0).isDefined) atomic { ctx ⇒ {
          pass.get.read()(ctx, classTag[ClassificationTreeNode]).flatMap(_.add(results(0), pass.get, root, strategy, maxSplitDepth)(ctx))
        }} else {
          Future.successful(0)
        },
        if (results.get(1).isDefined) atomic { ctx ⇒ {
          fail.get.read()(ctx, classTag[ClassificationTreeNode]).flatMap(_.add(results(1), fail.get, root, strategy, maxSplitDepth)(ctx))
        }} else {
          Future.successful(0)
        },
        if (results.get(2).isDefined) atomic { ctx ⇒ {
          exception.get.read()(ctx, classTag[ClassificationTreeNode]).flatMap(_.add(results(2), exception.get, root, strategy, maxSplitDepth)(ctx))
        }} else {
          Future.successful(0)
        }
      )).map(_.reduceOption(_ + _).getOrElse(0))
    }

    def nextBlock(cursor: BigInt, self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode]): Future[(BigInt, Stream[LabeledItem])] = {
      if (cursor < 0) {
        Future.successful((cursor - 1) -> Stream.empty)
      } else {
        val cursorPtr: Future[STMPtr[ClassificationTreeNode]] = if (cursor == 0) {
          firstNode(self)
        } else {
          getByTreeId(cursor, self)
        }
        cursorPtr.flatMap(nodePtr => {
          nodePtr.atomic.read.flatMap((node: ClassificationTreeNode) => {
            val members: Future[Stream[LabeledItem]] = node.atomic().getMembers(nodePtr)
            val nextId: Future[BigInt] = node.atomic().nextNode(nodePtr, root).flatMap(_.map((y: STMPtr[ClassificationTreeNode]) =>
              y.atomic.read.flatMap(_.atomic().getTreeId(y, root))).getOrElse(Future.successful(-1)))
            members.flatMap(members => nextId.map(nextId => {
              nextId -> members
            }))
          })
        })
      }
    }

    def firstNode(self: STMPtr[ClassificationTreeNode]): Future[STMPtr[ClassificationTreeNode]] = atomic {
      ClassificationTreeNode.this.firstNode(self)(_)
    }

    def nextNode(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode]): Future[Option[STMPtr[ClassificationTreeNode]]] = atomic {
      ClassificationTreeNode.this.nextNode(self, root)(_)
    }

    def getTreeId(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode]): Future[BigInt] = atomic {
      ClassificationTreeNode.this.getTreeId(self, root)(_)
    }

    def getByTreeId(cursor: BigInt, self: STMPtr[ClassificationTreeNode]): Future[STMPtr[ClassificationTreeNode]] = atomic {
      ClassificationTreeNode.this.getByTreeId(cursor, self)(_)
    }

    def getMembers(self: STMPtr[ClassificationTreeNode]): Future[Stream[LabeledItem]] = Future.successful {
      itemBuffer.map((x: PageTree) => x.atomic().stream()).getOrElse(Stream.empty)
    }

    def stream(self: STMPtr[ClassificationTreeNode]): Future[Stream[LabeledItem]] = Future.successful {
      val cursorStream: Stream[(BigInt, Stream[LabeledItem])] = Stream.iterate((BigInt(0l), Stream.empty[LabeledItem]))(t => sync.nextBlock(t._1, self, self))
      val itemStream: Stream[LabeledItem] = cursorStream.takeWhile(_._1 > -2).flatMap(_._2)
      itemStream
    }

    def sync = new SyncApi(10.seconds)

    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def split(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int = 0): Int = sync {
        ClassificationTreeNode.split(self, root, strategy, maxSplitDepth)
      }

      def firstNode(self: STMPtr[ClassificationTreeNode]): STMPtr[ClassificationTreeNode] = sync {
        NodeAtomicApi.this.firstNode(self)
      }

      def nextNode(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode]): Option[STMPtr[ClassificationTreeNode]] = sync {
        NodeAtomicApi.this.nextNode(self, root)
      }

      def getTreeId(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode]): BigInt = sync {
        NodeAtomicApi.this.getTreeId(self, root)
      }

      def nextBlock(value: BigInt, self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode]): (BigInt, Stream[LabeledItem]) = sync {
        NodeAtomicApi.this.nextBlock(value, self, root)
      }

      def stream(self: STMPtr[ClassificationTreeNode]): Stream[LabeledItem] = sync {
        NodeAtomicApi.this.stream(self)
      }

      def getMembers(self: STMPtr[ClassificationTreeNode]): Stream[LabeledItem] = sync {
        NodeAtomicApi.this.getMembers(self)
      }
    }

  }

  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def nextBlock(value: BigInt, self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode])
                 (implicit ctx: STMTxnCtx): (BigInt, Stream[Page]) =
      sync {
        ClassificationTreeNode.this.nextBlock(value, self, root)
      }
  }

}

object ClassificationTreeNode {
  private implicit def executionContext = StmPool.executionContext

  def apply(parent: Option[STMPtr[ClassificationTreeNode]])(implicit ctx: STMTxnCtx) =
    new ClassificationTreeNode(parent, itemBuffer = Option(PageTree()))

  def splitTaskFn(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int): (Restm, ExecutionContext) => TaskSuccess[String] =
    (c: Restm, e: ExecutionContext) => {
      //println(s"Starting split task for $self")
      try {
        val future = ClassificationTreeNode.split(self, root, strategy, maxSplitDepth)(c).map(_ => TaskSuccess("OK"))(e)
        val result = Await.result(future, 10.minutes)
        //println(s"Completed split task for $self - $result")
        result
      } catch {
        case e ⇒ e.printStackTrace();throw e
      }
    }


  def split(self: STMPtr[ClassificationTreeNode], root: STMPtr[ClassificationTreeNode], strategy: ClassificationStrategy, maxSplitDepth: Int = 0)
           (implicit cluster: Restm): Future[Int] = {
    def currentData = self.atomic.sync.read
    val pageSize = 128
    //new RuntimeException().printStackTrace()
    //println(s"Spliting $self")

    val depth: Future[Int] = new STMTxn[Int] {
      override def txnLogic()(implicit ctx: STMTxnCtx): Future[Int] = {
        self.read().flatMap(_.getTreeDepth(self, root))
      }
    }.txnRun(cluster)

    depth.flatMap(depth⇒{
      val firstBuffer: Future[Option[PageTree]] = new STMTxn[Option[PageTree]] {
        override def txnLogic()(implicit ctx: STMTxnCtx): Future[Option[PageTree]] = {
          //println(s"Obtaining split lock for $self")
          self.read().flatMap(node => {
            if (node.splitBuffer.isDefined) {
              println(s"Split lock failed for $self")
              Future.successful(None)
            } else if (node.itemBuffer.isDefined) {
              val prevRecieveBuffer = node.itemBuffer.get
              PageTree.create().flatMap(newBuffer => {
                self.write(node.copy(
                  itemBuffer = Option(newBuffer),
                  splitBuffer = Option(prevRecieveBuffer))
                ).map(_ => prevRecieveBuffer).map(Option(_))
              })
            } else {
              //println(s"Node already split - $self")
              Future.successful(None)
            }
          })
        }
      }.txnRun(cluster)

      firstBuffer.flatMap(_.map((firstBuffer: PageTree) ⇒ {
        require(firstBuffer.atomic().sync.size() > 0)
        def transferBuffers() = new STMTxn[PageTree] {
          override def txnLogic()(implicit ctx: STMTxnCtx): Future[PageTree] = {
            //println(s"Swapping queues for $self")
            self.read().flatMap(node => {
              val collection = node.itemBuffer.get
              self.write(node.copy(
                itemBuffer = Option(PageTree()),
                splitBuffer = Option(collection))
              ).map(_ => collection)
            })
          }
        }.txnRun(cluster)

        val makeRule: Future[Boolean] = {
          //println(s"Deriving rule for $self")
          val newRule = strategy.getRule(firstBuffer.atomic().pageStream(), depth)
          //ClassificationTreeNode.this.getTreeId(self, root)
          //println(s"Deriving rule complete for ${self}")
          if (null != newRule) {
            createChildren(self).flatMap(t => {
              require(!t.contains(null))
              self.atomic.update(_.copy(
                rule = Option(KryoValue(newRule)),
                pass = Option(t(0)),
                fail = Option(t(1)),
                exception = Option(t(2))
              )).map(_ => true)
            })
          } else {
            Future.successful(false)
          }
        }

        makeRule.flatMap(ruleDefined => {
          if(ruleDefined) {
            def transferAsync(tree: Future[PageTree]): Future[Int] =
              tree.flatMap((pageTree: PageTree) => {
                val node = currentData
                try {
                  //println(s"Routing items")
                  val pageStream: Stream[Page] = Stream.continually(pageTree.atomic().sync.get()).takeWhile(_.isDefined).map(_.get)
                  val pageAccumulator = pageStream.scanLeft((List.empty[Page],List.empty[Page]))((prev: (List[Page], List[Page]), page: Page)⇒{
                    if(prev._2.map(_.size).sum < pageSize) {
                      (List.empty[Page], prev._2 ++ List(page))
                    } else {
                      (prev._2, List(page))
                    }
                  })
                  val pages = (pageAccumulator.map(_._1) ++ Stream(pageAccumulator.last._2)).filterNot(_.isEmpty).map(Page.union(_:_*))
                  //println(s"Routing pages")
                  def routePage(block: Page): Future[Int] = {
                    node.atomic().route(block, self, root, strategy, maxSplitDepth - 1).map(_ => block.size)
                  }
                  Future.sequence(pages.map(routePage).toList).map(_.sum)
                } catch {
                  case e ⇒ e.printStackTrace(); throw e
                }
              })

            def transferRecursive(buffer: Future[PageTree]): Future[Int] = {
              transferAsync(buffer).flatMap(rows => {
                if (rows > 10) {
                  transferRecursive(transferBuffers()).map(_ + rows)
                } else {
                  Future.successful(rows)
                }
              })
            }

            transferRecursive(Future.successful(firstBuffer)).flatMap(rowsTransfered => {
              new STMTxn[Int] {
                override def txnLogic()(implicit ctx: STMTxnCtx): Future[Int] = {
                  self.read().flatMap((node: ClassificationTreeNode) => {
                    try {
                      //println(s"Splitting (sync) on $self")
                      val pages: Stream[Page] = node.itemBuffer.map(pageTree⇒{
                        val pageStream: Stream[Page] = Stream.continually(pageTree.sync.get()).takeWhile(_.isDefined).map(_.get)
                        val pageAccumulator = pageStream.scanLeft((List.empty[Page],List.empty[Page]))((prev: (List[Page], List[Page]), page: Page)⇒{
                          if(prev._2.map(_.size).sum < pageSize) {
                            (List.empty[Page], prev._2 ++ List(page))
                          } else {
                            (prev._2, List(page))
                          }
                        })
                        (pageAccumulator.map(_._1) ++ Stream(pageAccumulator.last._2)).filterNot(_.isEmpty).map(Page.union(_:_*))
                      }).getOrElse(Stream.empty)
                      //println(s"Final transfer for $self = ${pages.size} pages")
                      val routeTasks = pages.map((block: Page) ⇒ {
                        node.route(block, self, root, strategy, maxSplitDepth - 1).map(_ => block.size)
                      }).toList
                      //println(s"Waiting for insert for $self of ${pages.size} pages")
                      Future.sequence(routeTasks).map(_.sum).flatMap(phase2Transfered => {
                        //println(s"Finalizing $self after transferring $phase2Transfered items")
                        self.write(node.copy(itemBuffer = None, splitBuffer = None)).map(_ => rowsTransfered + phase2Transfered)
                      })
                    } catch {
                      case e ⇒ e.printStackTrace(); throw e
                    }
                  })
                }
              }.txnRun(cluster)
            })
          } else {
            self.atomic.read.flatMap(node => {
              require(firstBuffer.atomic().sync.size() > 0)
              Future.sequence(firstBuffer.atomic().pageStream().map(page⇒node.itemBuffer.get.atomic().add(page)))
                .map(_⇒{
                  require(node.itemBuffer.isDefined)
                  require(node.itemBuffer.get.atomic().sync.size() > 0)
                  0
                })
            })
          }
        })
      }).getOrElse(Future.successful(0)).flatMap(result⇒{
        val node = self.atomic.sync.read
        if(node.rule.isDefined) {
          require(!node.itemBuffer.isDefined)
          require(node.pass.get.atomic.sync.read.itemBuffer.get.atomic().sync.size() > 0)
          require(node.fail.get.atomic.sync.read.itemBuffer.get.atomic().sync.size() > 0)
          Future.successful(result)
        } else {
          require(node.itemBuffer.isDefined)
          node.splitBuffer.map(splitBuffer⇒{
            Future.sequence(splitBuffer.atomic().pageStream().map(page⇒node.itemBuffer.get.atomic().add(page)))
          }).getOrElse(Future.successful(Stream.empty)).map(_⇒{
            val size = node.itemBuffer.get.atomic().sync.size()
            require(size > 0)
            result
          })
        }
      })
      )
    })




  }

  private def createChildren(self: STMPtr[ClassificationTreeNode])
                            (implicit cluster: Restm): Future[List[STMPtr[ClassificationTreeNode]]] =
    new STMTxn[List[STMPtr[ClassificationTreeNode]]] {
      override def txnLogic()(implicit ctx: STMTxnCtx): Future[List[STMPtr[ClassificationTreeNode]]] = {
        Future.sequence(List(
          STMPtr.dynamic(ClassificationTree.newClassificationTreeNode(Option(self))),
          STMPtr.dynamic(ClassificationTree.newClassificationTreeNode(Option(self))),
          STMPtr.dynamic(ClassificationTree.newClassificationTreeNode(Option(self)))
        ))
      }
    }.txnRun(cluster)

}
