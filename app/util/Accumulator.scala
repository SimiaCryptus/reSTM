package util

class Accumulator {
  private[this] val moments = Array(0.0,0.0,0.0)
  def +=(x:Double) = synchronized {
    moments(0) = moments(0) + 1
    moments(1) = moments(1) + x
    moments(2) = moments(2) + x*x
  }
  def count = moments(0)
  def total = moments(1)
  def mean = moments(1) / moments(0)
  def stddev = Math.sqrt((moments(2) / moments(0)) - mean * mean)
  override def toString = s"${mean} +/- ${stddev}"
}
