import java.util.concurrent.Executors
import java.util.{Date, UUID}

import _root_.util.Util
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpec}
import stm.collection.{SimpleLinkedList, TreeCollection}
import stm.task._
import storage.Restm._
import storage._
import storage.actors.RestmActors
import storage.cold.BdbColdStorage
import storage.remote.RestmCluster
import storage.types.JacksonValue

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}



abstract class TaskManagementSpecBase extends WordSpec with MustMatchers {
  implicit def cluster: Restm
  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))

  "Task management" should {
    def randomStr = UUID.randomUUID().toString.take(8)
    def randomUUIDs = Stream.continually(randomStr)
    "monitor ongoing work" in {
      //ActorLog.enabled = true
      StmExecutionQueue.get().verbose = false
      RestmActors.IDLE_PTR_TIME = 1 // Stress test pointer expiration and restoration

      val taskTimeout = 120.minutes
      val insertTimeout = 5.minutes
      val taskSize = 1000
      val diagnosticsOperationTimeout = 3.minutes

      System.out.println(s"Starting Test at ${new Date()}")
      val input = randomUUIDs.take(taskSize).toSet
      val collection = new TreeCollection[String](new PointerType)

      val inputBuffer = new mutable.HashSet[String]()
      inputBuffer ++= input
      val threads = (0 to 20).map(_ => new Thread(new Runnable {
        override def run(): Unit = {
          input.filter(x=>inputBuffer.synchronized(inputBuffer.remove(x))).foreach(input => {
            collection.atomic().sync(1.minutes).add(input)
          })
        }
      }))
      threads.foreach(_.start())
      def now = new Date()
      val timeout = new Date(now.getTime + insertTimeout.toMillis)
      while(threads.exists(_.isAlive))  {
        if(!timeout.after(now)) throw new RuntimeException("Time Out")
        Thread.sleep(100)
      }
      System.out.println(s"Input Prepared at ${new Date()}")
      println(JacksonValue.simple(Util.getMetrics).pretty)
      Util.clearMetrics()

      val sortTask: Task[SimpleLinkedList[String]] = collection.atomic().sync.sort()
      System.out.println(s"Task Started at ${new Date()}")
      StmDaemons.start()
      try {
        StmExecutionQueue.get().registerDaemons(8)
        System.out.println(s"Execution Engine Started at ${new Date()}")

        val sortResult = try {
          TaskUtil.awaitTask(sortTask, taskTimeout, diagnosticsOperationTimeout)
        } finally {
          System.out.println(s"Final Data at ${new Date()}")
          println(JacksonValue.simple(Util.getMetrics).pretty)
          println(JacksonValue.simple(sortTask.atomic().sync(60.seconds).getStatusTrace(StmExecutionQueue.get())).pretty)

          System.out.println("Stopping Execution Engine")
          Await.result(StmDaemons.stop(), 30.seconds)
        }

        val output = sortResult.atomic().sync.stream().toList
        output.size mustBe input.size
        output mustBe input.toList.sorted
      } finally {
        Await.result(StmDaemons.stop(), 10.seconds)
      }

    }
  }

}


class LocalClusterTaskManagementSpec extends TaskManagementSpecBase with BeforeAndAfterEach with BeforeAndAfterAll  {
  val shards: List[RestmActors] = (0 until 8).map(_ => new RestmActors(new BdbColdStorage(path = "testDb", dbname = UUID.randomUUID().toString))).toList

  override def beforeEach() {
    //shards.foreach(_.clear())
  }

  override def afterAll() {
  }

  val cluster = new RestmCluster(shards)(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))
}