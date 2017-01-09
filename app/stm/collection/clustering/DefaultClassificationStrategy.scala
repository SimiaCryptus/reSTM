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

package stm.collection.clustering

import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder
import stm.STMTxnCtx
import util.{LevenshteinDistance, Util}

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object DefaultClassificationStrategy {
  private val workerPool: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8,
    new ThreadFactoryBuilder().setNameFormat("rule-pool-%d").build()))

}

case class DefaultClassificationStrategy(
                                          branchThreshold: Int = 8
                                        ) extends ClassificationStrategy {
  private implicit def _executionContext = DefaultClassificationStrategy.workerPool
  def getRule(pages: Stream[Page]) = Util.monitorBlock("DefaultClassificationStrategy.getRule") {
    val sampledRows = pages.flatMap(_.rows).take(1000).toList
    val fields = pages.flatMap(_.schema.keys).toSet
    val fieldResults = fields.map((field: String) => Future {
      rules_Levenshtein(sampledRows, field) ++ rules_SimpleScalar(sampledRows, field)
    })
    val rules = Await.result(Future.sequence(fieldResults), 5.minutes).flatten
    if (rules.nonEmpty) rules.maxBy(_._2)._1
    else null
  }

  private def rules_SimpleScalar(values: List[Page#PageRow], field: String) = Util.monitorBlock("DefaultClassificationStrategy.rules_SimpleScalar") {
    metricRules(field, values,
      _.get(field).exists(_.isInstanceOf[Number]),
      _(field).asInstanceOf[Number].doubleValue())
  }

  private def metricRules(field: String, values: List[Page#PageRow], filter: (KeyValue[String,Any]) => Boolean, metric: (KeyValue[String,Any]) => Double) = Util.monitorBlock("DefaultClassificationStrategy.metricRules") {
    val exceptionCounts: Map[String, Int] = values.filter(filter).groupBy(_.label).mapValues(_.size)
    val valueSortMap: Seq[(String, Double)] = values.filter(filter)
      .map((item: Page#PageRow) => item.label -> metric(item))
      .sortBy(_._2)

    val labelCounters: Map[String, mutable.Map[Double, Int]] = valueSortMap.groupBy(_._1)
      .mapValues(_.toList.groupBy(_._2).mapValues(_.size))
      .mapValues(x => new mutable.HashMap() ++ x)
    val valueCounters = new mutable.HashMap[String, mutable.Map[Double, Int]]()
    valueSortMap.distinct.map(item => {
      val (label, threshold: Double) = item
      valueCounters.getOrElseUpdate(label, new mutable.HashMap()).put(threshold, labelCounters(label)(threshold))

      val compliment: Map[String, Map[Double, Int]] = valueCounters.map(e => {
        val (key, leftItems) = e
        val counters: mutable.Map[Double, Int] = labelCounters(key).clone()
        val doubleToInt: mutable.Map[Double, Int] = counters -- leftItems.keys
        key -> doubleToInt
      }).mapValues(_.toMap).toMap
      val ruleFn: (KeyValue[String,Any]) => Boolean = item => {
        metric(item) <= threshold
      }
      val ruleName = s"$field <= $threshold"
      new RuleData(ruleFn, ruleName) -> fitness(valueCounters.mapValues(_.toMap).toMap, compliment, exceptionCounts)
    })
  }

  private def rules_Levenshtein(values: List[Page#PageRow], field: String) = Util.monitorBlock("DefaultClassificationStrategy.rules_Levenshtein") {
    distanceRules(field, "LevenshteinDistance", values,
      _.get(field).exists(_.isInstanceOf[String]),
      _(field).toString(),
      (a: String, b: String) => LevenshteinDistance.getDefaultInstance.apply(a, b))
  }

  private def distanceRules[T](field: String, metricName: String, values: List[Page#PageRow],
                               filter: (KeyValue[String,Any]) => Boolean,
                               metric: (KeyValue[String,Any]) => T,
                               distance: (T, T) => Int) = Util.monitorBlock("DefaultClassificationStrategy.distanceRules") {
    val fileredItems: Seq[Page#PageRow] = values.filter(filter)
    fileredItems.flatMap((center: Page#PageRow) => {
      val exceptionCounts: Map[String, Int] = values.filter(filter).groupBy(_.label).mapValues(_.size)
      val centerMetric = metric(center)
      val valueSortMap: Seq[(String, Double)] = fileredItems.map(item => {
        item.label -> distance(
          centerMetric,
          metric(item)
        ).doubleValue()
      }).sortBy(_._2)

      val labelCounters: Map[String, mutable.Map[Double, Int]] = valueSortMap.groupBy(_._1)
        .mapValues(_.toList.groupBy(_._2).mapValues(_.size))
        .mapValues(x => new mutable.HashMap() ++ x)
      val valueCounters = new mutable.HashMap[String, mutable.Map[Double, Int]]()
      valueSortMap.distinct.map(item => {
        val (label, value: Double) = item
        valueCounters.getOrElseUpdate(label, new mutable.HashMap()).put(value, labelCounters(label)(value))

        val compliment: Map[String, Map[Double, Int]] = valueCounters.map(e => {
          val (key, leftItems) = e
          val counters: mutable.Map[Double, Int] = labelCounters(key).clone()
          val doubleToInt: mutable.Map[Double, Int] = counters -- leftItems.keys
          key -> doubleToInt
        }).mapValues(_.toMap).toMap

        val ruleFn: (KeyValue[String,Any]) => Boolean = (item: KeyValue[String,Any]) => {
          distance(
            centerMetric,
            metric(item)
          ) <= value.asInstanceOf[Number].doubleValue()
        }
        val ruleName = s"$metricName from ${center} <= $value"
        new RuleData(ruleFn, ruleName) -> fitness(valueCounters.mapValues(_.toMap).toMap, compliment, exceptionCounts)
      })
    })
  }

  def fitness(left: Map[String, Map[Double, Int]], right: Map[String, Map[Double, Int]], exceptions: Map[String, Int]): Double = Util.monitorBlock("DefaultClassificationStrategy.fitness") {
    val result = {
      (left.keys ++ right.keys).toSet.map((label: String) => {
        val leftOpt = left.getOrElse(label, Map.empty)
        val rightOpt = right.getOrElse(label, Map.empty)
        val total = leftOpt.values.sum + rightOpt.values.sum
        List(leftOpt, rightOpt).map(map => {
          val sum = map.values.sum.toDouble
          val factor = sum / total
          factor * Math.log(1 - factor) // * map.values.map(x=>x*x).sum
        }).sum
      }).sum
    }
    //println(s"(left=$left,right=$right,ex=$exceptions) = $result")
    result
  }

  def split(buffer: PageTree)(implicit ctx: STMTxnCtx): Boolean = {
    buffer.sync(30.seconds).apxSize() > branchThreshold //|| buffer.sync.size() > branchThreshold
  }
}