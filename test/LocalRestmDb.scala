import java.util.concurrent.Executors

import storage.{RestmActors, RestmImpl}

import scala.concurrent.ExecutionContext


object LocalRestmDb extends RestmImpl(
  new RestmActors()(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
)(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
