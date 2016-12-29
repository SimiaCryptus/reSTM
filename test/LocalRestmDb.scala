import java.util.concurrent.Executors

import storage.RestmImpl
import storage.actors.RestmActors

import scala.concurrent.ExecutionContext


object LocalRestmDb {
  def apply() = new RestmImpl(new RestmActors())(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
}
