package util

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object OperationMetrics {
  val metrics = new TrieMap[String, OperationMetrics]()

  def qos[T](tag: String)(f: => Future[T])(implicit exeCtx: ExecutionContext) = {
    lazy val qosMetrics: OperationMetrics = metrics.getOrElseUpdate(tag, new OperationMetrics)
    val timer = new Timer
    f.andThen({
      case Success(result) =>
        qosMetrics.successRate += 1.0
        qosMetrics.executionTime += timer.elapsed
      case Failure(e) =>
        qosMetrics.successRate += 0.0
    })
  }
}

class OperationMetrics {
  val successRate = new Accumulator
  val executionTime = new Accumulator

  override def toString = Map[String, String](
    "successRate" -> (successRate.total + "/" + successRate.count),
    "executionTime" -> executionTime.toString
  ).toString
}
