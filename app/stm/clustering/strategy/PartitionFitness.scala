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


object PartitionFitness {
  val NegativeInfinity = new PartitionFitness(Double.NegativeInfinity)
}

case class PartitionFitness(fitness:Double, msg:String) extends Ordered[PartitionFitness] {
  def this(fitness:Double) = this(fitness,fitness.toString)

  override def compare(that: PartitionFitness): Int = {
    java.lang.Double.compare(fitness, that.fitness)
  }

  override def toString: String = msg
}