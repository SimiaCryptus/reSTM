package util

class Timer {
  val time = System.currentTimeMillis()

  def elapsed = System.currentTimeMillis() - time
}
