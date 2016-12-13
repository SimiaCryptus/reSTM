package stm

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

abstract class SyncApiBase(duration: Duration) {
  def sync[T](f: =>Future[T]) : T = Await.result(f, duration)
}
