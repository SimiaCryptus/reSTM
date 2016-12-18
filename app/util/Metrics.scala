package util

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.AtomicDouble

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


object Metrics {
  def now = System.nanoTime().nanoseconds
  val codeMetricsData = new TrieMap[String, CodeMetrics]()

  def codeBlock[T](name:String)(f: =>T):T = codeMetricsData.getOrElseUpdate(name, new CodeMetrics).sync(f)
  def codeFuture[T](name:String)(f: =>Future[T]):Future[T] = codeMetricsData.getOrElseUpdate(name, new CodeMetrics).future(f)

  def get() = Map(
    "code" -> codeMetricsData.toMap.mapValues(_.get()).groupBy(_._1.split("\\.").head)
  )
  private[util] val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
}
import util.Metrics._

class CodeMetrics {

  val invokeCount = new AtomicInteger(0)
  val successCount = new AtomicInteger(0)
  val errorCount = new AtomicInteger(0)
  val totalTime = new AtomicDouble(0.0)

  def sync[T](f: =>T):T = {
    val start = now
    invokeCount.incrementAndGet()
    val result: Try[T] = Try { f }
    totalTime.addAndGet((now-start).toUnit(TimeUnit.SECONDS))
    if(result.isFailure) errorCount.incrementAndGet()
    else successCount.incrementAndGet()
    result.get
  }

  def future[T](f: =>Future[T]):Future[T] = {
    val start = now
    invokeCount.incrementAndGet()
    val result = f
    totalTime.addAndGet((now-start).toUnit(TimeUnit.SECONDS))
    result.onComplete({
      case Success(_) => successCount.incrementAndGet()
      case Failure(_) => errorCount.incrementAndGet()
    })(executionContext)
    result
  }

  def get() = Map(
    "invocations" -> invokeCount.get(),
    "success" -> successCount.get(),
    "failed" -> errorCount.get(),
    "totalTime" -> totalTime.get(),
    "avgTime" -> totalTime.get()/(successCount.get()+errorCount.get())
  )
}
