import java.awt.Desktop
import java.net.URI
import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import controllers.RestmController.storageService
import org.scalatest.{MustMatchers, WordSpec}
import org.scalatestplus.play.OneServerPerTest
import stm.task.{StmDaemons, StmExecutionQueue}
import storage.remote.RestmHttpClient

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}


class ServletUtilitySpec extends WordSpec with MustMatchers with OneServerPerTest {
  private val baseUrl = s"http://localhost:$port"
  implicit val cluster = new RestmHttpClient(baseUrl)(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("restm-pool-%d").build())))
  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build()))


  "Single Node Servlet" should {
    "provide demo sort class" in {
      StmDaemons.start()
      StmExecutionQueue.get().verbose = true
      StmExecutionQueue.get().registerDaemons(4)(storageService, executionContext)
      Thread.sleep(1000) // Allow platform to start
      Desktop.getDesktop.browse(new URI(s"http://localhost:$port/sys/logs/"))
      Await.result(StmDaemons.join(), 300.minutes)
      Thread.sleep(1000) // Allow rest of processes to complete
    }
  }

  //    "Single Node Servlet" should {
  //      "provide demo sort class" in {
  //        StmExecutionQueue.verbose = true
  //        StmExecutionQueue.registerDaemons(4)(storageService,executionContext)
  //        StmDaemons.start()
  //        try {
  //          Thread.sleep(1000) // Allow platform to start
  //
  //          val treeId = UUID.randomUUID().toString
  //
  //          //val result: String = Await.result(Http((url(baseUrl) / "cluster" / treeId).GET OK as.String), 30.seconds)
  //
  //          def insert(label:String, item:Any): String = {
  //            val request = (url(baseUrl) / "cluster" / treeId).addQueryParameter("label", label)
  //            Await.result(Http(request.PUT << JacksonValue(item).toString OK as.String), 30.seconds)
  //          }
  //          def query(item:Any): String = {
  //            val request = (url(baseUrl) / "cluster" / treeId / "find")
  //            Await.result(Http(request.POST << JacksonValue(item).toString OK as.String), 30.seconds)
  //          }
  //          def info(): String = {
  //            val request = (url(baseUrl) / "cluster" / treeId / "config")
  //            Await.result(Http(request.GET OK as.String), 30.seconds)
  //          }
  //
  //          println(insert("A", Map("value" -> 5)))
  //          println(query(Map("value" -> 5)))
  //          println(info())
  //
  //          Await.result(Http((url(baseUrl) / "sys" / "shutdown").GET OK as.String), 30.seconds)
  //          Await.result(StmDaemons.join(), 300.minutes)
  //          Thread.sleep(1000) // Allow rest of processes to complete
  //        } finally {
  //          Await.result(StmDaemons.stop(), 10.seconds)
  //        }
  //      }
  //    }


}
