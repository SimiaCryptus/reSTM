package storage.util

import storage.{RestmImpl, RestmInternal}

import scala.concurrent.ExecutionContext

class RestmCluster(val clusterShards: List[RestmInternal])(implicit executionContext : ExecutionContext) extends RestmImpl(new RestmInternalHashRouter{
  override def shards: List[RestmInternal] = clusterShards
})
