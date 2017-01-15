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

import util.Util


class SimpleEntropyClassificationStrategy(
                                        branchThreshold: Int = 20,
                                        smoothingFactor: Double = 1.0,
                                        factor_0 : Double = 0.1,
                                        factor_1 : Double = -1.0,
                                        factor_2 : Double = 1.0
                                      ) extends MetricClassificationStrategyBase(branchThreshold,smoothingFactor) {

  def fitness(left: Map[String, Int], right: Map[String, Int], exceptions: Map[String, Int], ruleName: String): Double = Util.monitorBlock("DefaultClassificationStrategy.fitness") {
    val labelCounts: Map[String, Int] = (left.toList++right.toList++exceptions.toList).groupBy(_._1).mapValues(_.map(_._2).sum)
    val totalCount: Double = labelCounts.values.sum.toDouble
    val leftCount: Int = left.values.sum
    val rightCount: Int = right.values.sum
    val exCount: Int = exceptions.values.sum
    val smoothingFactor: Double = this.smoothingFactor / totalCount
    if(leftCount == 0 || rightCount == 0) Double.NegativeInfinity else {
      val cross_entropy_1: Double = -1 * {
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
      val cross_entropy_2: Double = -1 * {
        labelCounts.keys.map((label: String) => {
          // This cross entropy is with respect to the pre-and-post-distributions, not between labels
          val x: Double = (left.getOrElse(label, 0)) / totalCount
          val y: Double = labelCounts(label) * leftCount / (totalCount*totalCount)
          y * Math.log(smoothingFactor + x)
        }).sum +
          labelCounts.keys.map((label: String) => {
            val x: Double = (right.getOrElse(label, 0)) / totalCount
            val y: Double = labelCounts(label) * rightCount / (totalCount*totalCount)
            y * Math.log(smoothingFactor + x)
          }).sum +
          labelCounts.keys.map((label: String) => {
            val x: Double = (exceptions.getOrElse(label, 0)) / totalCount
            val y: Double = labelCounts(label) * exCount / (totalCount*totalCount)
            y * Math.log(smoothingFactor + x)
          }).sum
      }
      val self_entropy: Double = -1 * {
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
      val result: Double = factor_2 * cross_entropy_2 + factor_1 * cross_entropy_1 + factor_0 * self_entropy
      //println(s"(left=$left,right=$right,ex=$exceptions) = $cross_entropy + $self_entropy = $result ($ruleName)")
      result
    }
  }

}
