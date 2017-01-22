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

class DefaultClassificationStrategy(
                                     branchThreshold: Int = 20,
                                     smoothingFactor: Double = 2.0,
                                     var factor_0 : Double = 0.1,
                                     var factor_1 : Double = 1.0,
                                     var factor_2 : Double = -1.0,
                                     minEntropy : Double = -0.5,
                                     forceDepth : Int = 4,
                                     maxDepth : Int = 24
                                   )
  extends EntropyClassificationStrategyBase(branchThreshold,smoothingFactor, minEntropy, forceDepth, maxDepth) {

  override def mix(self_entropy: Double, cross_entropy_1: Double, cross_entropy_2: Double, marginal_entropy: Double): Double = {
    require(factor_0 != 0.0 || factor_1 != 0.0 || factor_2 != 0.0 )
    if(marginal_entropy > -0.1) {
      // Hard lopsidedness threshold - Don't "shave" the data
      Double.NegativeInfinity
    } else if(Math.abs(marginal_entropy - self_entropy) < 0.00001) {
      // Labal mixture hard threshold - Ignore single-class or nearly single-class nodes
      Double.NegativeInfinity
    } else {
      (factor_2 * cross_entropy_2 + factor_1 * cross_entropy_1 + factor_0 * self_entropy) // / -marginal_entropy
    }
  }

}