package storage.util

import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import storage.{RestmActors, RestmImpl, RestmInternal}

import scala.concurrent.ExecutionContext


object LocalRestmDb extends RestmImpl(
  new RestmActors()(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
)(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
