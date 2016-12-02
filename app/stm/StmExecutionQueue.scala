package stm

import storage.Restm
import storage.Restm.PointerType

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext


object StmExecutionQueue {
  val workQueue = LinkedList.static[(Restm,ExecutionContext)=>Unit](new PointerType("StmExecutionQueue/workQueue"))
  private val threads = new ArrayBuffer[Thread]
  private def task(implicit cluster: Restm, executionContext: ExecutionContext) = new Runnable {
    override def run(): Unit = {
      while(!Thread.interrupted()) {
        val item = workQueue.atomic.sync.remove
        if(item.isDefined) {
          item.get(cluster, executionContext)
        } else {
          Thread.sleep(100)
        }
      }
    }
  }
  def start(count : Int = 1)(implicit cluster: Restm, executionContext: ExecutionContext) = {
    threads ++= (for(i <- 1 to count) yield {
      val thread: Thread = new Thread(task)
      thread.setDaemon(true)
      thread.setName(s"StmExecutionQueue-$i")
      thread.start()
      thread
    })
  }
}
