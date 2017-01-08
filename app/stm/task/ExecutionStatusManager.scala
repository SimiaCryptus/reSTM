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

import java.net.InetAddress
import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.collection.Map
import scala.collection.concurrent.TrieMap

object ExecutionStatusManager {
  private[this] val pool: ScheduledExecutorService = Executors.newScheduledThreadPool(1,
    new ThreadFactoryBuilder().setNameFormat("exe-status-pool-%d").build())
  private[this] val localName: String = InetAddress.getLocalHost.getHostAddress
  private[this] val currentStatus = new TrieMap[String, TrieMap[String, String]]()

  def end(executorName: String, task: Task[AnyRef]): ScheduledFuture[_] = {
    val executorRecord = currentStatus.getOrElseUpdate(executorName, new TrieMap[String, String]())
    executorRecord.put(task.id, "Complete")
    println(s"Task completed: $task on $executorName")
    pool.schedule(new Runnable {
      override def run(): Unit = {
        executorRecord.remove(task.id)
        //println(s"Task expired: $task on $executorName")
      }
    }, 10, TimeUnit.SECONDS)
  }

  def start(executorName: String, task: Task[AnyRef]): Unit = {
    val executorRecord = currentStatus.getOrElseUpdate(executorName, new TrieMap[String, String]())
    executorRecord.put(task.id, "Started")
    println(s"Task started: $task on $executorName")
  }

  def check(executorId: String, taskId: String): Boolean = {
    currentStatus(executorId).contains(taskId)
  }

  def currentlyRunning(): Int = currentStatus.filterNot(!_._2.exists(_._2 == "Started")).size

  def status(): Map[String, Map[String, String]] = currentStatus.mapValues(_.filter(_._2 == "Started").toMap)

  def getName: String = localName + ":" + Thread.currentThread().getName
}
