package stm.task

import java.net.InetAddress
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import storage.Restm.PointerType

import scala.collection.concurrent.TrieMap

object ExecutionStatusManager {
  def end(executorName: String, task: Task[AnyRef]) = {
    val executorRecord = currentStatus.getOrElseUpdate(executorName, new TrieMap[PointerType,String]())
    executorRecord.put(task.id, "Complete")
    pool.schedule(new Runnable {
      override def run(): Unit = {
        executorRecord.remove(task.id)
      }
    }, 10, TimeUnit.SECONDS)
  }

  def start(executorName: String, task: Task[AnyRef]) = {
    val executorRecord = currentStatus.getOrElseUpdate(executorName, new TrieMap[PointerType,String]())
    executorRecord.put(task.id, "Started")
  }

  def check(executorId: String, taskId: PointerType) = {
    currentStatus(executorId).contains(taskId)
  }

  def currentlyRunning() = currentStatus.filterNot(_._2.isEmpty).size

  def getName(): String = localName + ":" + Thread.currentThread().getName

  private[this] val pool: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  private[this] val localName: String = InetAddress.getLocalHost.getHostAddress
  private[this] val currentStatus = new TrieMap[String,TrieMap[PointerType,String]]()
}
