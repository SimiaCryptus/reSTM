package util

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.AtomicDouble
import storage.LockedException

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


object Util {
  def clearMetrics(): Unit = {
    codeMetricsData.clear()
  }

  def now = System.nanoTime().nanoseconds
  val codeMetricsData = new TrieMap[String, CodeMetrics]()
  val scalarData = new TrieMap[String, AtomicDouble]()
  def delta(name:String, delta:Double) = scalarData.getOrElseUpdate(name,new AtomicDouble(0)).addAndGet(delta)


  def monitorBlock[T](name:String)(f: =>T):T = codeMetricsData.getOrElseUpdate(name, new CodeMetrics).sync(f)
  def monitorFuture[T](name:String)(f: =>Future[T]):Future[T] = codeMetricsData.getOrElseUpdate(name, new CodeMetrics).future(f)

  def chainEx[T](str: =>String, verbose:Boolean = true)(f: =>Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val stackTrace: Array[StackTraceElement] = Thread.currentThread().getStackTrace
    f.recover({
      case e : LockedException =>
        val wrappedEx = new LockedException(e.conflitingTxn, e)
        wrappedEx.setStackTrace(stackTrace)
        throw wrappedEx
      case e =>
        val wrappedEx = new RuntimeException(str, e)
        wrappedEx.setStackTrace(stackTrace)
        throw wrappedEx
    })
  }


  def getMetrics() = Map(
    "code" -> codeMetricsData.toMap.mapValues(_.get()).groupBy(_._1.split("\\.").head),
    "scalars" -> scalarData.toMap.mapValues(_.get()).groupBy(_._1.split("\\.").head)
  )
  private[util] val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
}
import util.Util._

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
    result.onComplete({
      case Success(_) =>
        successCount.incrementAndGet()
        totalTime.addAndGet((now-start).toUnit(TimeUnit.SECONDS))
      case Failure(_) =>
        errorCount.incrementAndGet()
        totalTime.addAndGet((now-start).toUnit(TimeUnit.SECONDS))
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
