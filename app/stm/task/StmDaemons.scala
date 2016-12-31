package stm.task

import java.util.concurrent._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import stm.collection.SimpleLinkedList
import storage.Restm
import storage.Restm.PointerType
import storage.types.KryoValue

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object DaemonConfig {
  def apply(name:String, f:(Restm, ExecutionContext)=>Unit) = {
    new DaemonConfig(name, KryoValue[(Restm, ExecutionContext)=>Unit](f))
  }
}
case class DaemonConfig(name: String, impl: KryoValue[(Restm, ExecutionContext)=>Unit]) {
  def deserialize() = impl.deserialize()
}

object StmDaemons {

  val config = SimpleLinkedList.static[DaemonConfig](new PointerType("StmDaemons/config"))
  private[this] val daemonThreads = new scala.collection.concurrent.TrieMap[String,Thread]
  private[this] var mainThread: Option[Thread] = None

  def start()(implicit cluster: Restm) : Unit = {
    if(!mainThread.filter(_.isAlive).isDefined) mainThread = Option({



      val pool = new ThreadPoolExecutor(32, 128, 5L, TimeUnit.SECONDS,
        new LinkedBlockingQueue[Runnable],//new SynchronousQueue[Runnable],
        new ThreadFactoryBuilder().setNameFormat("daemon-pool-%d").build())
      val executionContext: ExecutionContext = ExecutionContext.fromExecutor(pool)
      Await.result(StmExecutionQueue.init()(cluster, executionContext), 30.seconds)
      val thread: Thread = new Thread(new Runnable {
        override def run(): Unit = {
          implicit def _exe: ExecutionContext = executionContext
          try {
            while(!Thread.interrupted()) {
              startAll()
              Thread.sleep(1000)
            }
          } finally {
            daemonThreads.values.foreach(_.interrupt())
            pool.shutdown()
            StmExecutionQueue.reset()
          }
        }
      })
      thread.setName("StmDaemons-poller")
      thread.start()
      thread
    })
  }

  def stop()(implicit executionContext: ExecutionContext) = {
    mainThread.foreach(_.interrupt())
    join()
  }

  def join()(implicit executionContext: ExecutionContext): Future[Unit] = {
    val promise: Promise[Unit] = Promise[Unit]
    def isMainAlive: Boolean = mainThread.filter(_.isAlive).isDefined
    def allDaemonsComplete: Boolean = daemonThreads.filter(_._2.isAlive).isEmpty
    val scheduledFuture = Task.scheduledThreadPool.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = if (!isMainAlive && allDaemonsComplete) promise.success(Unit)
    }, 100, 100, TimeUnit.MILLISECONDS)
    val future: Future[Unit] = promise.future
    future.onComplete(_ => scheduledFuture.cancel(false))
    future
  }

  private[this] def startAll()(implicit cluster: Restm, executionContext: ExecutionContext) = {
    daemonThreads.filter(!_._2.isAlive).forall(t=>daemonThreads.remove(t._1, t._2))
    config.atomic().sync.stream().foreach(item=>{
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
}
