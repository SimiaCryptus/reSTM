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
                                     smoothingFactor: Double = 1.0,
                                     factor_0 : Double = 0.1,
                                     factor_1 : Double = -1.0,
                                     factor_2 : Double = 1.0
                                   )
  extends SimpleEntropyClassificationStrategy(branchThreshold,smoothingFactor, factor_0, factor_1, factor_2)