package stm.lib0

import storage.Restm
import storage.Restm.PointerType
import storage.data.KryoValue

import scala.concurrent.ExecutionContext


object StmDaemons {

  case class DaemonConfig(name: String, impl: KryoValue) {
    def deserialize() = impl.deserialize[(Restm, ExecutionContext)=>Unit]()
  }

  val config = LinkedList.static[DaemonConfig](new PointerType("StmDaemons/config"))
  val threads = new scala.collection.concurrent.TrieMap[String,Thread]

  def init()(implicit cluster: Restm, executionContext: ExecutionContext) = {
    val thread: Thread = new Thread(new Runnable {
      override def run(): Unit = {
        while(!Thread.interrupted()) {
          startAll()
          Thread.sleep(1000)
        }
      }
    })
    thread.setName("StmDaemons-poller")
    thread.start()
    thread
  }

  def startAll()(implicit cluster: Restm, executionContext: ExecutionContext) = {
    config.stream().foreach(item=>{
      threads.getOrElseUpdate(item.name, {
        val thread: Thread = new Thread(new Runnable {
          override def run(): Unit = {
            item.deserialize().get(cluster, executionContext)
          }
        })
        thread.start()
        thread.setName("StmDaemons-" + item.name)
        println(s"Starting $thread")
        thread
      })
    })
  }
}
