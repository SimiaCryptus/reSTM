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

import java.util.concurrent.atomic.AtomicInteger

import stm.STMTxnCtx
import stm.clustering.{KeyValue, _}
import util.Util

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


object MetricClassificationStrategyBase {

  private def ruleFn(metric: (KeyValue[String, Any]) ⇒ Double, threshold: Double) = {
    (item: KeyValue[String, Any]) => {
      metric(item) > threshold
    }
  }

  private def metricFn(field: String) = {
    (x:KeyValue[String,Any]) ⇒ x(field).asInstanceOf[Number].doubleValue()
  }

}

abstract case class MetricClassificationStrategyBase(
                                                      val branchThreshold: Int,
                                                      val smoothingFactor: Double,
                                                      val minEntropy : Double,
                                                      val forceDepth : Int,
                                                      val maxDepth : Int
                                        ) extends ClassificationStrategy {


  private implicit def _executionContext = ClassificationStrategy.workerPool
  def getRule(pages: Stream[Page], depth: Int) = Util.monitorBlock("DefaultClassificationStrategy.getRule") {
    val sampledRows = pages.flatMap(_.rows).take(1000).toList
    val fields: Set[String] = pages.flatMap(_.schema.keys).toSet
    val fieldResults = fields.map((field: String) => Future {
      metricRules(field, sampledRows,
        _.get(field).exists(_.isInstanceOf[Number]),
        MetricClassificationStrategyBase.metricFn(field), depth)
    })
    val rules: Set[(RuleData, PartitionFitness)] = Await.result(Future.sequence(fieldResults), 5.minutes).flatten
      .filterNot(_._2.fitness.isNaN).filterNot(_._2.fitness.isInfinite)
    val rule = rules.maxBy(_._2)
    if (maxDepth > depth && (forceDepth > depth || rules.filter(_._2.fitness > minEntropy).nonEmpty)) {
      println(s"Rule created: ${rule._1.name} at depth $depth with fitness ${rule._2}")
      rule._1
    } else {
      println(s"Rule supressed: ${rule._1.name} at depth $depth with fitness ${rule._2}")
      null
    }
  }

  private def metricRules(field: String, values: List[Page#PageRow], filter: (KeyValue[String,Any]) => Boolean, metric: (KeyValue[String,Any]) => Double, depth: Int) = Util.monitorBlock("DefaultClassificationStrategy.metricRules") {
    val exceptionCounts: Map[String, Int] = values.filterNot(filter).groupBy(_.label).mapValues(_.size)
    val valueSortMap: Seq[(String, Double)] = values.filter(filter)
      .map((item: Page#PageRow) => item.label -> metric(item))
    val labelCounts: Map[String, Int] = valueSortMap.groupBy(_._1).mapValues(_.size)
    val valueCounters = new mutable.HashMap[String, AtomicInteger]()
    valueSortMap.groupBy(_._2).mapValues(_.groupBy(_._1).mapValues(_.size)).toList.sortBy(_._1).map(item => {
      val (threshold: Double, label: Map[String, Int]) = item
      label.foreach(t⇒{
        val (label,count) = t
        valueCounters.getOrElseUpdate(label, new AtomicInteger(0)).addAndGet(count)
      })
      val compliment: Map[String, Int] = labelCounts.keys.map(key⇒{
        val leftItems = valueCounters.get(key).map(_.get()).getOrElse(0)
        val doubleToInt: Int = labelCounts(key) - leftItems
        key -> doubleToInt
      }).toMap
      val ruleFn: (KeyValue[String,Any]) => Boolean = MetricClassificationStrategyBase.ruleFn(metric, threshold)
      val ruleName = s"$field > $threshold"
      new RuleData(ruleFn, ruleName) -> fitness(valueCounters.mapValues(_.get()).toMap, compliment, exceptionCounts, ruleName, depth)
    })
  }

  def fitness(left: Map[String, Int], right: Map[String, Int], exceptions: Map[String, Int], ruleName: String, depth: Int): PartitionFitness

  def split(buffer: PageTree)(implicit ctx: STMTxnCtx): Boolean = {
    val apxSize = buffer.sync(30.seconds).apxSize()
    lazy val realSize = {
      val size = buffer.sync.size()
      //println(s"Apx $apxSize resolved to $size exact size")
      size
    }
    val decision = apxSize > (2.0 * branchThreshold) || (apxSize > (0.5 * branchThreshold) && realSize > branchThreshold)
    //println(s"Apx $apxSize split? $decision")
    decision
  }
}
