package storage.util

import storage.{RestmImpl, RestmInternal, RestmInternalStaticListRouter}

import scala.concurrent.ExecutionContext

class RestmCluster(val clusterShards: List[RestmInternal])(implicit executionContext: ExecutionContext) extends RestmImpl(new RestmInternalStaticListRouter {
  override def shards: List[RestmInternal] = clusterShards
})
