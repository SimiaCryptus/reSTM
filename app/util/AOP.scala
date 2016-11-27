package util

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object AOP {
  val metrics = new TrieMap[String, QosMetrics]()
  def qos[T](tag : String)(f: =>Future[T])(implicit exeCtx: ExecutionContext) = {
    lazy val qosMetrics: QosMetrics = metrics.getOrElseUpdate(tag, new QosMetrics)
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
