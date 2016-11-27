package storage.util

import storage.{RestmImpl, RestmInternal}

import scala.concurrent.ExecutionContext

class RestmCluster(val shards: List[RestmInternal])(implicit executionContext : ExecutionContext) extends RestmImpl(new RestmInternalHashRouter{
  override def shards: List[RestmInternal] = shards
})
