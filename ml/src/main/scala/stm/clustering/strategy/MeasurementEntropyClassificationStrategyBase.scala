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

abstract class MeasurementEntropyClassificationStrategyBase(
                                        branchThreshold: Int,
                                        smoothingFactor: Double,
                                        minEntropy : Double,
                                        forceDepth : Int,
                                        maxDepth : Int
                                      ) extends MetricClassificationStrategyBase(branchThreshold, minEntropy, forceDepth, maxDepth) {

  def fitness(left: Map[String, Int], right: Map[String, Int], exceptions: Map[String, Int], ruleName: String, depth: Int): PartitionFitness = {
    val labelCounts: Map[String, Int] = (left.toList++right.toList++exceptions.toList).groupBy(_._1).mapValues(_.map(_._2).sum)
    val totalCount: Double = labelCounts.values.sum.toDouble
    val leftCount: Int = left.values.sum
    val rightCount: Int = right.values.sum
    val exCount: Int = exceptions.values.sum
    val smoothingFactor: Double = this.smoothingFactor / totalCount
    if(leftCount == 0 || rightCount == 0) PartitionFitness.NegativeInfinity else {
      val cross_entropy_1: Double = 1 * {
        labelCounts.keys.map((label: String) => {
          // This cross entropy is with respect to the pre-and-post-distributions, not between labels
          val x: Double = (left.getOrElse(label, 0)) / totalCount
          val y: Double = labelCounts(label) * leftCount / (totalCount*totalCount)
          //y * Math.log(smoothingFactor + x)
          x * Math.log(smoothingFactor + y)
        }).sum +
          labelCounts.keys.map((label: String) => {
            val x: Double = (right.getOrElse(label, 0)) / totalCount
            val y: Double = labelCounts(label) * rightCount / (totalCount*totalCount)
            x * Math.log(smoothingFactor + y)
          }).sum +
          labelCounts.keys.map((label: String) => {
            val x: Double = (exceptions.getOrElse(label, 0)) / totalCount
            val y: Double = labelCounts(label) * exCount / (totalCount*totalCount)
            x * Math.log(smoothingFactor + y)
          }).sum
      }
      val cross_entropy_2: Double = 1 * {
        labelCounts.keys.map((label: String) => {
          // This cross entropy is with respect to the pre-and-post-distributions, not between labels
          val x: Double = (left.getOrElse(label, 0)) / totalCount
          val y: Double = labelCounts(label) * leftCount / (totalCount * totalCount)
          y * Math.log(smoothingFactor + x)
        }).sum +
          labelCounts.keys.map((label: String) => {
            val x: Double = (right.getOrElse(label, 0)) / totalCount
            val y: Double = (labelCounts(label) * rightCount).toDouble / (totalCount*totalCount)
            y * Math.log(smoothingFactor + x)
          }).sum +
          labelCounts.keys.map((label: String) => {
            val x: Double = (exceptions.getOrElse(label, 0)) / totalCount
            val y: Double = labelCounts(label) * exCount / (totalCount*totalCount)
            y * Math.log(smoothingFactor + x)
          }).sum
      }
      val self_entropy: Double = 1 * {
        labelCounts.keys.map((label: String) => {
          val x = (left.getOrElse(label, 0)) / totalCount
          x * Math.log(smoothingFactor + x)
        }).sum +
          labelCounts.keys.map((label: String) => {
            val x = (right.getOrElse(label, 0)) / totalCount
            x * Math.log(smoothingFactor + x)
          }).sum +
          labelCounts.keys.map((label: String) => {
            val x = (exceptions.getOrElse(label, 0)) / totalCount
            x * Math.log(smoothingFactor + x)
          }).sum
      }
      val marginal_entropy: Double = 1 * { {
          val x = (left.values.sum) / totalCount
          x * Math.log(smoothingFactor + x)
        } + {
          val x = (right.values.sum) / totalCount
          x * Math.log(smoothingFactor + x)
        } + {
          val x = (exceptions.values.sum) / totalCount
          x * Math.log(smoothingFactor + x)
        }
      }
      val result: Double = mix(self_entropy, cross_entropy_1, cross_entropy_2, marginal_entropy)
      //println(s"(left=$left,right=$right,ex=$exceptions) = $cross_entropy_1 + $cross_entropy_2 + $self_entropy = $result ($ruleName)")
      PartitionFitness(result, f"[$result%1.3f, [$cross_entropy_1%1.3f,$cross_entropy_2%1.3f,$self_entropy%1.3f,$marginal_entropy%1.3f], {L=$left,R=$right,X=$exceptions}]")
    }
  }

  def mix(self_entropy: Double, cross_entropy_1: Double, cross_entropy_2: Double, marginal_entropy: Double): Double
}


class LinearEntropyClassificationStrategy(branchThreshold: Int = 20,
                                          smoothingFactor: Double = 1,
                                          val factor_0 : Double = -0.1,
                                          val factor_1 : Double = 1,
                                          val factor_2 : Double = -1,
                                          minEntropy : Double = Double.NegativeInfinity,
                                          forceDepth : Int = 0,
                                          maxDepth : Int
                                         ) extends MeasurementEntropyClassificationStrategyBase(branchThreshold,smoothingFactor, minEntropy, forceDepth, maxDepth) {

  def mix(self_entropy: Double, cross_entropy_1: Double, cross_entropy_2: Double, marginal_entropy: Double): Double = {
    require(factor_0 != 0.0 || factor_1 != 0.0 || factor_2 != 0.0 )
    (factor_2 * cross_entropy_2 + factor_1 * cross_entropy_1 + factor_0 * self_entropy)
  }

}
