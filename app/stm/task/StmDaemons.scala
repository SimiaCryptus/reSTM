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

package stm.task

import java.util.concurrent._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import stm.collection.LinkedList
import storage.Restm
import storage.Restm.PointerType
import storage.types.KryoValue

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object DaemonConfig {
  def apply(name: String, f: (Restm, ExecutionContext) => Unit): DaemonConfig = {
    new DaemonConfig(name, KryoValue[(Restm, ExecutionContext) => Unit](f))
  }
}

case class DaemonConfig(name: String, impl: KryoValue[(Restm, ExecutionContext) => Unit]) {
  def deserialize(): Option[(Restm, ExecutionContext) => Unit] = impl.deserialize()
}

object StmDaemons {

  val config: LinkedList[DaemonConfig] = LinkedList.static[DaemonConfig](new PointerType("StmDaemons/config"))
  private[this] val daemonThreads = new scala.collection.concurrent.TrieMap[String, Thread]
  private[this] var mainThread: Option[Thread] = None
  private[this] lazy val pool = new ThreadPoolExecutor(8, 32, 5L, TimeUnit.SECONDS,
    new LinkedBlockingQueue[Runnable], //new SynchronousQueue[Runnable],
    new ThreadFactoryBuilder().setNameFormat("daemon-pool-%d").build())
  private[task] implicit lazy val executionContext: ExecutionContext = ExecutionContext.fromExecutor(pool)

  def start()(implicit cluster: Restm): Unit = {
    if (mainThread.filter(_.isAlive).isEmpty) mainThread = Option({
      Await.result(StmExecutionQueue.init()(cluster), 30.seconds)
      val thread: Thread = new Thread(new Runnable {
        override def run(): Unit = {
          try {
            while (!Thread.interrupted()) {
              try {
                startAll()(cluster)
              } catch {
                case e : Throwable ⇒ e.printStackTrace()
              }
              Thread.sleep(1000)
            }
          } finally {
            println("Daemon Thread Stopping")
            daemonThreads.values.foreach(_.interrupt())
            StmExecutionQueue.reset()
          }
        }
      })
      thread.setName("StmDaemons-poller")
      thread.start()
      thread
    })
  }

  private[this] def startAll()(implicit cluster: Restm) = {
    daemonThreads.filter(!_._2.isAlive).forall(t => daemonThreads.remove(t._1, t._2))
    config.atomic().sync.stream().foreach(item => {
      daemonThreads.getOrElseUpdate(item.name, {
        val task: (Restm, ExecutionContext) => Unit = item.deserialize().get
        val thread: Thread = new Thread(new Runnable {
          override def run(): Unit = task(cluster, executionContext)
        })
        thread.start()
        thread.setName("StmDaemons-" + item.name)
        println(s"Starting $thread")
        thread
      })
    })
  }

  def stop(): Future[Unit] = {
    mainThread.foreach(_.interrupt())
    join()
  }

  def join(): Future[Unit] = {
    val promise: Promise[Unit] = Promise[Unit]

    def isMainAlive: Boolean = mainThread.exists(_.isAlive)

    def allDaemonsComplete: Boolean = !daemonThreads.exists(_._2.isAlive)

    val scheduledFuture = Task.scheduledThreadPool.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = if (!isMainAlive && allDaemonsComplete) promise.success(Unit)
    }, 100, 100, TimeUnit.MILLISECONDS)
    val future: Future[Unit] = promise.future
    future.onComplete(_ => scheduledFuture.cancel(false))
    future
  }
}
