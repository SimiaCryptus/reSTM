package storage.util

import storage.{RestmImpl, RestmInternal}

class RestmCluster(val shards: List[RestmInternal]) extends RestmImpl with RestmInternalHashRouter
