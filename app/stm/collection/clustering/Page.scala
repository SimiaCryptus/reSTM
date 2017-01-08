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

import com.google.common.primitives.Longs
import stm.collection.clustering.Page.{RefColumn, ValueColumn}

object Page {

  def apply(data : LabeledItem*) : Page = {
    apply(List(data:_*))
  }

  def apply(data : List[LabeledItem]) : Page = {
    val maps: List[Map[String, Any]] = data.map(_.value.attributes)
    val sets = maps.flatMap(_.keys).toSet.toList
      .sorted.groupBy(key⇒maps.map(_.get(key)).filter(_.isDefined).map(_.get).forall(_.isInstanceOf[Number]))
    val (scalars, refs) = (sets.get(true).getOrElse(List.empty),sets.get(false).getOrElse(List.empty))
    val refCols: List[RefColumn[_]] = refs.foldLeft(List.empty[RefColumn[_]])(
      (l,col)⇒l++List(new StringColumn(col,l.lastOption.map(_.index+1).getOrElse(0))))
    val scalarCols: List[ValueColumn[_]] = refs.foldLeft(List.empty[ValueColumn[_]])(
      (l,col)⇒l++List(new DoubleColumn(col,l.lastOption.map(x⇒x.start+x.length).getOrElse(0))))
    val refVals: List[String] = maps.flatMap(data⇒refCols.map(col=>data.get(col.name).map(_.toString).orNull))
    val scalarVals: List[Double] = maps.flatMap(data⇒scalarCols.map(
      col=>data.get(col.name).map(_.asInstanceOf[Number].doubleValue()).getOrElse(Double.NaN)))
    val labels: List[String] = data.map(data⇒data.label)
    val labelNames = labels.distinct.sorted
    new Page(
      schema = (refCols ++ scalarCols).toArray,
      labelNames = labelNames.toArray,
      labels = labels.map(labelNames.indexOf(_)).toArray,
      refs = refVals.toArray,
      values = scalarVals.flatMap(x⇒Longs.toByteArray(java.lang.Double.doubleToLongBits(x))).toArray
    )
  }

  def apply(data : List[Map[String, Any]], labelColumn : String): Page = {
    val sets = data.flatMap(_.keys).filterNot(_==labelColumn).toSet.toList
      .sorted.groupBy(key⇒data.map(_.get(key)).filter(_.isDefined).map(_.get).forall(_.isInstanceOf[Number]))
    val (scalars, refs) = (sets(true),sets(false))
    val refCols: List[RefColumn[_]] = refs.foldLeft(List.empty[RefColumn[_]])(
      (l,col)⇒l++List(new StringColumn(col,l.lastOption.map(_.index+1).getOrElse(0))))
    val scalarCols: List[ValueColumn[_]] = refs.foldLeft(List.empty[ValueColumn[_]])(
      (l,col)⇒l++List(new DoubleColumn(col,l.lastOption.map(x⇒x.start+x.length).getOrElse(0))))
    val refVals: List[String] = data.flatMap(data⇒refCols.map(col=>data.get(col.name).map(_.toString).orNull))
    val scalarVals: List[Double] = data.flatMap(data⇒scalarCols.map(
      col=>data.get(col.name).map(_.asInstanceOf[Number].doubleValue()).getOrElse(Double.NaN)))
    val labels: List[String] = data.map(data⇒data.get(labelColumn).map(_.toString).orNull)
    val labelNames = labels.distinct.sorted
    new Page(
      schema = (refCols ++ scalarCols).toArray,
      labelNames = labelNames.toArray,
      labels = labels.map(labelNames.indexOf(_)).toArray,
      refs = refVals.toArray,
      values = scalarVals.flatMap(x⇒Longs.toByteArray(java.lang.Double.doubleToLongBits(x))).toArray
    )
  }

  abstract sealed class BaseColumn[T](val name:String) {
    def get(row : Page#PageRow):T
  }
  abstract class RefColumn[T <: AnyRef](name:String, val index: Int) extends BaseColumn[T](name) {
    def get(row : Page#PageRow):T = row.ref(index).asInstanceOf[T]
  }
  class StringColumn(name:String, index: Int) extends RefColumn[String](name, index)

  abstract class ValueColumn[T <: AnyVal](name:String, val start: Int, private[clustering] val length: Int) extends BaseColumn[T](name) {
    def raw(row : Page#PageRow):Array[Byte] = row.value(start, length)
  }
  class DoubleColumn(name:String, start: Int) extends ValueColumn[Double](name, start, 4) {
    override def get(row: Page#PageRow): Double = {
      val bits : Long = Longs.fromByteArray(raw(row))
      java.lang.Double.longBitsToDouble(bits)
    }
  }
}

class Page(val schema: Array[Page.BaseColumn[_]],
           val labelNames: Array[String],
           val values: Array[Byte],
           val refs: Array[AnyRef],
           val labels: Array[Int]) {

  def getAll(rows: List[this.PageRow]) = {
    val indexes = rows.map(_.row).distinct.sorted.toArray
    new Page(
      schema = schema,
      labelNames = labelNames,
      labels = indexes.map(i ⇒ labels(i)),
      refs = indexes.map(i ⇒ refs(i)),
      values = indexes.flatMap(i ⇒ java.util.Arrays.copyOfRange(values, i * rowValueSize, (i + 1) * rowValueSize))
    )
  }

  private val rowValueSize : Int = schema.map({ case x:ValueColumn[_] ⇒ x.length; case _ ⇒ 0 }).sum
  private val rowRefSize : Int = schema.count({ case x:RefColumn[_] ⇒ true; case _ ⇒ false })

  val size = labels.length
  def apply(n : Int) = new PageRow(n)

  def rows = (0 until size).map(new PageRow(_))

  class PageRow(val row : Int) {
    def keys = schema
    def ref(col : Int) = refs(row * rowRefSize + col)
    def value(startIndex : Int, length : Int) = java.util.Arrays.copyOfRange(values,
      row * rowValueSize + startIndex,
      row * rowValueSize + startIndex + length)
    def get(key:String) = schema.find(_.name==key).map(_.get(PageRow.this))
    def apply(key:String) = get(key).get
    def label = labelNames(labels(row))
    def asMap: Map[String, Any] = schema.map(col⇒col.name→col.get(PageRow.this)).toMap
    def asClassificationTreeItem = ClassificationTreeItem(asMap)
    def asLabeledItem = LabeledItem(label, asClassificationTreeItem)
  }
}


case class ClassificationTreeItem(attributes: Map[String, Any])

object ClassificationTreeItem {
  lazy val empty = ClassificationTreeItem(Map.empty)
}

case class LabeledItem(label: String, value: ClassificationTreeItem)
