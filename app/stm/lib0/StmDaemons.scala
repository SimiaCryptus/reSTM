package stm.lib0

import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.ExecutionContext


object StmDaemons {
  val config = LinkedList.static[(String, (Restm, ExecutionContext)=>Unit)](new PointerType("StmDaemons/config"))
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
      threads.getOrElseUpdate(item._1, {
        val thread: Thread = new Thread(new Runnable {
          override def run(): Unit = {
            item._2(cluster, executionContext)
          }
        })
        thread.start()
        thread.setName("StmDaemons-" + item._1)
        println(s"Starting $thread")
        thread
      })
    })
  }
}
