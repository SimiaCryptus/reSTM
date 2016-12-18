package stm.concurrent

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.annotation.JsonIgnore
import stm.collection.LinkedList
import storage.Restm
import storage.Restm.PointerType
import storage.data.KryoValue

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}


object DaemonConfig {
  def apply(name:String, f:(Restm, ExecutionContext)=>Unit) = {
    new DaemonConfig(name, KryoValue(f))
  }
}
case class DaemonConfig(name: String, impl: KryoValue) {
  def deserialize() = impl.deserialize[(Restm, ExecutionContext)=>Unit]()
}

object StmDaemons {

  @JsonIgnore val localName: String = InetAddress.getLocalHost.getHostAddress
  @JsonIgnore val currentStatus = new TrieMap[String,Task[_]]()

  val config = LinkedList.static[DaemonConfig](new PointerType("StmDaemons/config"))
  private[this] val daemonThreads = new scala.collection.concurrent.TrieMap[String,Thread]
  private[this] var mainThread: Option[Thread] = None

  def start()(implicit cluster: Restm, executionContext: ExecutionContext) : Unit = {
    if(!mainThread.filter(_.isAlive).isDefined) mainThread = Option({
      val thread: Thread = new Thread(new Runnable {
        override def run(): Unit = try {
          while(!Thread.interrupted()) {
            startAll()
            Thread.sleep(1000)
          }
        } finally {
          daemonThreads.values.foreach(_.interrupt())
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
    config.atomic.stream().foreach(item=>{
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
