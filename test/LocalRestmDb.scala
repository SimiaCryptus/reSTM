import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import storage.RestmImpl
import storage.actors.RestmActors

import scala.concurrent.ExecutionContext


object LocalRestmDb {
  def apply() = new RestmImpl(new RestmActors())(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("test-pool-%d").build())))
}
