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

abstract class LabelingEntropyClassificationStrategyBase(branchThreshold: Int, val smoothingFactor: Double, maxDepth: Int)
  extends MetricClassificationStrategyBase(branchThreshold, minEntropy = Double.NegativeInfinity, 0, maxDepth) {

  def fitness(left: Map[String, Int], right: Map[String, Int], exceptions: Map[String, Int], ruleName: String, depth: Int): PartitionFitness = {
    val exSum = exceptions.values.sum
    val rightSum = right.values.sum
    val leftSum = left.values.sum
    val fullSum = leftSum + rightSum + exSum
    val minSize = fullSum * 0.15
    if(leftSum <= minSize || rightSum <= minSize) {
      //println(f"[null, {L=$left,R=$right,X=$exceptions}]")
      PartitionFitness.NegativeInfinity
    } else {
      val result = List(left, right, exceptions).flatMap(partition⇒{
        val sum = partition.values.sum
        partition.values.map(c⇒(c.toDouble) * Math.log((c.toDouble + smoothingFactor) / (sum + smoothingFactor * partition.size)))
      }).sum / (fullSum*Math.log(2))
      val txt = f"[$result%1.3f, {L=$left,R=$right,X=$exceptions}]"
      //println(txt)
      PartitionFitness(result, txt)
    }
  }
}
