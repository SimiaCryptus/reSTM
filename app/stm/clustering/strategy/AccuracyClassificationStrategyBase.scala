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

package stm.clustering.strategy


abstract class AccuracyClassificationStrategyBase(
                                                   branchThreshold: Int,
                                                   maxDepth : Int
                                                 ) extends MetricClassificationStrategyBase(branchThreshold, 0, 0, maxDepth) {

  def fitness(left: Map[String, Int], right: Map[String, Int], exceptions: Map[String, Int], ruleName: String, depth: Int): PartitionFitness = {
    val minSize = (left.values.sum + right.values.sum) * 0.15
    if(left.values.sum <= minSize || right.values.sum <= minSize) {
      PartitionFitness.NegativeInfinity
    } else {
      val minorityCount: Int = List(left, right, exceptions).map(partition⇒{
        if(partition.size > 1) partition.toList.sortBy(-_._2).tail.map(_._2).sum
        else 0
      }).sum
      val sumSum = List(left, right, exceptions).map(partition⇒partition.map(_._2).sum).sum
      val result = 1-(minorityCount.toDouble/sumSum)
      val txt = f"[$result%1.3f, {L=$left,R=$right,X=$exceptions}]"
      //println(txt)
      PartitionFitness(result, txt)
    }
  }
}


