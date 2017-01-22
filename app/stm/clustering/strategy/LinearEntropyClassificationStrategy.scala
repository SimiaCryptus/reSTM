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

class LinearEntropyClassificationStrategy(branchThreshold: Int,
                                          smoothingFactor: Double,
                                          val factor_0 : Double,
                                          val factor_1 : Double,
                                          val factor_2 : Double,
                                          minEntropy : Double,
                                          forceDepth : Int,
                                          maxDepth : Int
                                         ) extends EntropyClassificationStrategyBase(branchThreshold,smoothingFactor, minEntropy, forceDepth, maxDepth) {

  def mix(self_entropy: Double, cross_entropy_1: Double, cross_entropy_2: Double, marginal_entropy: Double): Double = {
    require(factor_0 != 0.0 || factor_1 != 0.0 || factor_2 != 0.0 )
    (factor_2 * cross_entropy_2 + factor_1 * cross_entropy_1 + factor_0 * self_entropy)
  }

}
