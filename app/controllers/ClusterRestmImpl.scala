/*
 * Copyright (c) 2017 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package controllers

import java.net.InetAddress

import _root_.util.Config._
import storage._
import storage.actors.RestmActors
import storage.cold.{BdbColdStorage, ColdStorage, DynamoColdStorage, HeapColdStorage}
import storage.remote.{RestmInternalRestmHttpClient, RestmInternalStaticListRouter}

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class ClusterRestmImpl(implicit executionContext: ExecutionContext) extends RestmImpl {

  val peers = new mutable.HashSet[String]()
  val table: Option[String] = getConfig("dynamoTable")
  val filestore: Option[String] = getConfig("bdbFile")
  val bdbName: String = getConfig("bdbName").getOrElse("db")
  val peerPort: Int = getConfig("peerPort").map(Integer.parseInt).getOrElse(898)
  private[this] lazy val local: RestmActors = new RestmActors(coldStorage)
  private[this] val localName: String = InetAddress.getLocalHost.getHostAddress
  private[this] val coldStorage: ColdStorage =
    table.map(new DynamoColdStorage(_))
        .orElse(filestore.map(path⇒new BdbColdStorage(path=path, dbname = bdbName)))
      .getOrElse(new HeapColdStorage)
  def peerList: List[String] = (peers.toList ++ Set(localName)).sorted

  val internal: RestmInternal = new RestmInternalStaticListRouter {
    override def shards: List[RestmInternal] = {
      peerList.map(name => {
        if (name == localName) local
        else new RestmInternalRestmHttpClient(s"http://$name:$peerPort")
      })
    }
  }
}
