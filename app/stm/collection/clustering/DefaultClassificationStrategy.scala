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
import java.util.concurrent.atomic.AtomicInteger

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
                                          branchThreshold: Int = 20,
                                          smoothingFactor: Double = 1.0,
                                          factor_0 : Double = 0.1,
                                          factor_1 : Double = -1.0,
                                          factor_2 : Double = 1.0
                                        ) extends ClassificationStrategy {


  private implicit def _executionContext = DefaultClassificationStrategy.workerPool
  def getRule(pages: Stream[Page]) = Util.monitorBlock("DefaultClassificationStrategy.getRule") {
    val sampledRows = pages.flatMap(_.rows).take(1000).toList
    val fields: Set[String] = pages.flatMap(_.schema.keys).toSet
    val fieldResults = fields.map((field: String) => Future {
      rules_Levenshtein(sampledRows, field) ++ rules_SimpleScalar(sampledRows, field)
    })
    val rules: Set[(RuleData, Double)] = Await.result(Future.sequence(fieldResults), 5.minutes).flatten
      .filterNot(_._2.isNaN).filterNot(_._2.isInfinite)
    if (rules.nonEmpty) {
      val rule = rules.maxBy(_._2)
      println(s"Rule created: ${rule._1.name} with fitness ${rule._2}")
      rule._1
    } else null
  }

  private def rules_SimpleScalar(values: List[Page#PageRow], field: String) = Util.monitorBlock("DefaultClassificationStrategy.rules_SimpleScalar") {
    metricRules(field, values,
      _.get(field).exists(_.isInstanceOf[Number]),
      _(field).asInstanceOf[Number].doubleValue())
  }

  private def metricRules(field: String, values: List[Page#PageRow], filter: (KeyValue[String,Any]) => Boolean, metric: (KeyValue[String,Any]) => Double) = Util.monitorBlock("DefaultClassificationStrategy.metricRules") {
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
      val ruleFn: (KeyValue[String,Any]) => Boolean = item => {
        metric(item) > threshold
      }
      val ruleName = s"$field > $threshold"
      new RuleData(ruleFn, ruleName) -> fitness(valueCounters.mapValues(_.get()).toMap, compliment, exceptionCounts, ruleName)
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
      val exceptionCounts: Map[String, Int] = values.filterNot(filter).groupBy(_.label).mapValues(_.size)
      val centerMetric = metric(center)
      val valueSortMap: Seq[(String, Double)] = fileredItems.map(item => {
        item.label -> distance(
          centerMetric,
          metric(item)
        ).doubleValue()
      }).sortBy(_._2)

      val labelCounts: Map[String, Int] = valueSortMap.groupBy(_._1).mapValues(_.size)
      val valueCounters = new mutable.HashMap[String, AtomicInteger]()
      valueSortMap.groupBy(_._2).mapValues(_.groupBy(_._1).mapValues(_.size)).map(item => {
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

        val ruleFn: (KeyValue[String,Any]) => Boolean = (item: KeyValue[String,Any]) => {
          distance(
            centerMetric,
            metric(item)
          ) <= threshold.asInstanceOf[Number].doubleValue()
        }
        val ruleName = s"$metricName from ${center} <= $threshold"
        new RuleData(ruleFn, ruleName) -> fitness(valueCounters.mapValues(_.get).toMap, compliment, exceptionCounts, ruleName)
      })
    })
  }

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