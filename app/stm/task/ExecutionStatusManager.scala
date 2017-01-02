package stm.task

import java.net.InetAddress
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.collection.Map
import scala.collection.concurrent.TrieMap

object ExecutionStatusManager {
  def end(executorName: String, task: Task[AnyRef]) = {
    val executorRecord = currentStatus.getOrElseUpdate(executorName, new TrieMap[String,String]())
    executorRecord.put(task.id, "Complete")
    println(s"Task completed: $task on $executorName")
    pool.schedule(new Runnable {
      override def run(): Unit = {
        executorRecord.remove(task.id)
        println(s"Task expired: $task on $executorName")
      }
    }, 10, TimeUnit.SECONDS)
  }

  def start(executorName: String, task: Task[AnyRef]) = {
    val executorRecord = currentStatus.getOrElseUpdate(executorName, new TrieMap[String,String]())
    executorRecord.put(task.id, "Started")
    println(s"Task started: $task on $executorName")
  }

  def check(executorId: String, taskId: String) = {
    currentStatus(executorId).contains(taskId)
  }

  def currentlyRunning() = currentStatus.filterNot(_._2.filter(_._2=="Started").isEmpty).size

  def status(): Map[String, Map[String, String]] = currentStatus.mapValues(_.filter(_._2=="Started").toMap)

  def getName(): String = localName + ":" + Thread.currentThread().getName

  private[this] val pool: ScheduledExecutorService = Executors.newScheduledThreadPool(1,
    new ThreadFactoryBuilder().setNameFormat("exe-status-pool-%d").build())
  private[this] val localName: String = InetAddress.getLocalHost.getHostAddress
  private[this] val currentStatus = new TrieMap[String,TrieMap[String,String]]()
}
