package stm.collection

import stm.STMTxnCtx
import stm.collection.ClassificationTree.{ClassificationTreeItem, LabeledItem}
import util.{LevenshteinDistance, Util}

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.concurrent.ExecutionContext

trait ClassificationStrategy {

  def getRule(values:List[ClassificationTree.LabeledItem]): (ClassificationTreeItem) => Boolean

  def split(buffer : TreeCollection[ClassificationTree.LabeledItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Boolean

}

case class DefaultClassificationStrategy(
                                          branchThreshold : Int = 8
                                        ) extends ClassificationStrategy
{
  def fitness(left: Map[String, Map[Double,Int]], right: Map[String, Map[Double,Int]], exceptions: Map[String, Int]): Double = Util.monitorBlock("DefaultClassificationStrategy.fitness") {
    val result = {
      (left.keys ++ right.keys).toSet.map((label: String) =>{
        val leftOpt = left.getOrElse(label, Map.empty)
        val rightOpt = right.getOrElse(label, Map.empty)
        val total = leftOpt.values.sum + rightOpt.values.sum
        List(leftOpt,rightOpt).map(map=>{
          val sum = map.values.sum.toDouble
          val factor = sum / total
          factor * Math.log(1-factor) // * map.values.map(x=>x*x).sum
        }).sum
      }).sum
    }
    //println(s"(left=$left,right=$right,ex=$exceptions) = $result")
    result
  }

  def getRule(values:List[ClassificationTree.LabeledItem]): (ClassificationTreeItem) => Boolean = Util.monitorBlock("DefaultClassificationStrategy.getRule") {
    val rules: Set[((ClassificationTreeItem) => Boolean, Double)] = values.flatMap(_.value.attributes.keys).toSet.flatMap((field: String) => {
      rules_Levenshtein(values, field) ++ rules_SimpleScalar(values, field)
    })
    if(!rules.isEmpty) rules.maxBy(_._2)._1
    else null
  }

  private def rules_SimpleScalar(values: List[LabeledItem], field: String) = Util.monitorBlock("DefaultClassificationStrategy.rules_SimpleScalar") {
    metricRules(values,
      _.value.attributes.get(field).filter(_.isInstanceOf[Number]).isDefined,
      _.attributes(field).asInstanceOf[Number].doubleValue())
  }

  private def rules_Levenshtein(values: List[LabeledItem], field: String) = Util.monitorBlock("DefaultClassificationStrategy.rules_Levenshtein") {
    distanceRules(values,
      _.value.attributes.get(field).filter(_.isInstanceOf[String]).isDefined,
      _.attributes(field).toString(),
      (a: String,b: String) => LevenshteinDistance.getDefaultInstance.apply(a,b))
  }

  private def distanceRules[T](values: List[LabeledItem], filter: (LabeledItem) => Boolean, metric: (ClassificationTreeItem) => T, distance: (T, T) => Int) = Util.monitorBlock("DefaultClassificationStrategy.distanceRules") {
    val fileredItems: Seq[LabeledItem] = values.filter(filter)
    fileredItems.flatMap(center => {
      val exceptionCounts: Map[String, Int] = values.filter(filter).groupBy(_.label).mapValues(_.size)
      val valueSortMap: Seq[(String, Double)] = fileredItems.map(item => {
        item.label -> distance(
          metric(center.value),
          metric(item.value)
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

        val rule: (ClassificationTreeItem) => Boolean = item => {
          distance(
            metric(center.value),
            metric(item)
          ) <= value.asInstanceOf[Number].doubleValue()
        }
        rule -> fitness(valueCounters.mapValues(_.toMap).toMap, compliment.toMap, exceptionCounts)
      })
    })
  }

  private def metricRules(values: List[LabeledItem], filter: (LabeledItem) => Boolean, metric: (ClassificationTreeItem) => Double) = Util.monitorBlock("DefaultClassificationStrategy.metricRules") {
    val exceptionCounts: Map[String, Int] = values.filter(filter).groupBy(_.label).mapValues(_.size)
    val valueSortMap: Seq[(String, Double)] = values.filter(filter).map(item => item.label -> metric(item.value))
      .map(item => item._1 -> item._2.asInstanceOf[Number].doubleValue().toDouble)
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
      val rule: (ClassificationTreeItem) => Boolean = item => {
        metric(item) <= threshold
      }
      rule -> fitness(valueCounters.mapValues(_.toMap).toMap, compliment.toMap, exceptionCounts)
    })
  }

  def split(buffer : TreeCollection[ClassificationTree.LabeledItem])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Boolean = {
    buffer.sync.size() > branchThreshold
  }
}
