package util

class QosMetrics {
  val successRate = new Accumulator
  val executionTime = new Accumulator
  override def toString = Map[String,String](
    "successRate" -> (successRate.total + "/" + successRate.count),
    "executionTime" -> executionTime.toString
  ).toString
}
