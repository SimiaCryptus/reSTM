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

import java.nio.ByteBuffer

import com.google.common.primitives.Longs
import stm.collection.clustering.Page.{BaseColumn, DoubleColumn, RefColumn, ValueColumn}

import scala.collection.immutable.{ListMap, Seq}

object Page {
  val empty = new Page(schema = ListMap.empty, labelNames = Array.empty,
    values = Array.empty, refs = Array.empty, Array.empty)

  def apply(data : LabeledItem*) : Page = {
    apply(List(data:_*))
  }

  def fromRows(data : List[Page#PageRow]) : Page = {
    val keys: Set[BaseColumn[_]] = data.flatMap(_.keys.values).distinct.toSet
    require(keys.groupBy(_.name).forall(_._2.map(_.getClass).toSet.size==1), "Incompatible Schema")
    val (scalarCols, refCols) = (
      keys.filter(_.isInstanceOf[ValueColumn[_]]).map(_.asInstanceOf[ValueColumn[_]]),
      keys.filter(_.isInstanceOf[RefColumn[_]]).map(_.asInstanceOf[RefColumn[_]])
    )
    require(scalarCols.groupBy(_.name).forall(_._2.map(_.length).toSet.size==1), "Incompatible Schema")

    val newScalaCols: List[ValueColumn[_]] = scalarCols.scanLeft(None:Option[ValueColumn[_]])((l, x)⇒{
      require(x.isInstanceOf[DoubleColumn])
      Option(new DoubleColumn(x.name, l.map(x⇒x.start+x.length).getOrElse(0)))
    }).flatten.toList

    val refVals: Array[AnyRef] = data
      .flatMap(data ⇒ refCols.map(_.asInstanceOf[RefColumn[AnyRef]].get(data).asInstanceOf[AnyRef]))
      .toArray[AnyRef]


    val buffer = ByteBuffer.allocate(500*1024*1024)
    val longBuffer = buffer.asLongBuffer()
    for(row ← data) {
      for(col ← newScalaCols) {
        val value: Double = try {
          row(col.name).asInstanceOf[Number].doubleValue()
        } catch {
          case e : IllegalArgumentException ⇒ Double.NaN
        }
        longBuffer.put(java.lang.Double.doubleToLongBits(value))
      }
    }
    val bytes: Array[Byte] = java.util.Arrays.copyOfRange(buffer.array(), 0, longBuffer.position() * 8)


    val labels: Array[String] = data.map(data⇒data.label).toArray
    val labelNames = labels.distinct.sorted
    new Page(
      schema = ListMap((refCols ++ scalarCols).toArray.map(x⇒x.name→x).sortBy(_._1):_*),
      labelNames = labelNames,
      labels = labels.map(labelNames.indexOf(_)),
      refs = refVals,
      values = bytes
    )
  }

  def apply(data : List[LabeledItem]) : Page = {
    val maps: List[Map[String, Any]] = data.map(_.value.attributes)
    val sets = maps.flatMap(_.keys).toSet.toList
      .sorted.groupBy(key⇒maps.map(_.get(key)).filter(_.isDefined).map(_.get).forall(_.isInstanceOf[Number]))
    val (scalars, refs) = (sets.get(true).getOrElse(List.empty),sets.get(false).getOrElse(List.empty))
    val refCols: List[RefColumn[_]] = refs.foldLeft(List.empty[RefColumn[_]])(
      (l,col)⇒l++List(new StringColumn(col,l.lastOption.map(_.index+1).getOrElse(0))))
    val scalarCols: List[ValueColumn[_]] = scalars.foldLeft(List.empty[ValueColumn[_]])(
      (l,col)⇒l++List(new DoubleColumn(col,l.lastOption.map(x⇒x.start+x.length).getOrElse(0))))
    val refVals: List[String] = maps.flatMap(data⇒refCols.map(col=>data.get(col.name).map(_.toString).orNull))
    val scalarVals: List[Double] = maps.flatMap(data⇒scalarCols.map(
      col=>data.get(col.name).map(_.asInstanceOf[Number].doubleValue()).getOrElse(Double.NaN)))
    val labels: List[String] = data.map(data⇒data.label)
    val labelNames = labels.distinct.sorted
    new Page(
      schema = ListMap((refCols ++ scalarCols).toArray.map(x⇒x.name→x).sortBy(_._1):_*),
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
    val scalarCols: List[ValueColumn[_]] = scalars.foldLeft(List.empty[ValueColumn[_]])(
      (l,col)⇒l++List(new DoubleColumn(col,l.lastOption.map(x⇒x.start+x.length).getOrElse(0))))
    val refVals: List[String] = data.flatMap(data⇒refCols.map(col=>data.get(col.name).map(_.toString).orNull))
    val scalarVals: List[Double] = data.flatMap(data⇒scalarCols.map(
      col=>data.get(col.name).map(_.asInstanceOf[Number].doubleValue()).getOrElse(Double.NaN)))
    val labels: List[String] = data.map(data⇒data.get(labelColumn).map(_.toString).orNull)
    val labelNames = labels.distinct.sorted
    new Page(
      schema = ListMap((refCols ++ scalarCols).toArray.map(x⇒x.name→x).sortBy(_._1):_*),
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
  class DoubleColumn(name:String, start: Int) extends ValueColumn[Double](name, start, 8) {
    override def get(row: Page#PageRow): Double = {
      val bits : Long = Longs.fromByteArray(raw(row))
      java.lang.Double.longBitsToDouble(bits)
    }
  }
}

class Page(val schema: ListMap[String,Page.BaseColumn[_]],
           val labelNames: Array[String],
           val values: Array[Byte],
           val refs: Array[AnyRef],
           val labels: Array[Int]) {

  def labelCounts = labels.groupBy(x⇒x).mapValues(_.size).map(t⇒labelNames(t._1) → t._2)
  val size = labels.length

  private def valCols = schema.values.map(x⇒{
    if(classOf[ValueColumn[_]].isAssignableFrom(x.getClass)) {
      x.asInstanceOf[ValueColumn[_]].length
    } else 0
  })
  private val rowValueSize : Int = valCols.sum
  private val rowRefSize : Int = schema.count({ case x:RefColumn[_] ⇒ true; case _ ⇒ false })
  def apply(n : Int) = new PageRow(n)
  def rows = (0 until size).map(new PageRow(_))

  if(size * rowValueSize != values.length) {
    require(size * rowValueSize == values.length)
  }
  require(size * rowRefSize == refs.length)
  require(labels.forall(i⇒i>=0&&i<labelNames.length))

  def +(left:Page) : Page = {
    val rawkeys: Set[BaseColumn[_]] = (this.schema.values ++ left.schema.values).toSet
    require(rawkeys.groupBy(_.name).forall(_._2.map(_.getClass).toSet.size==1), "Incompatible Schema")
    val keys = rawkeys.groupBy(_.name).map(_._2.head)
    val (scalarCols, refCols) = (
      keys.filter(_.isInstanceOf[ValueColumn[_]]).map(_.asInstanceOf[ValueColumn[_]]),
      keys.filter(_.isInstanceOf[RefColumn[_]]).map(_.asInstanceOf[RefColumn[_]])
    )

    val newScalarCols: List[ValueColumn[AnyVal]] = scalarCols.scanLeft(None:Option[ValueColumn[AnyVal]])((l, x)⇒{
      require(x.isInstanceOf[DoubleColumn])
      Option(new DoubleColumn(x.name, l.map(x⇒x.start+x.length).getOrElse(0)).asInstanceOf[ValueColumn[AnyVal]])
    }).flatten.toList

    val allRows: Seq[Page#PageRow] = rows ++ left.rows
    val refVals: Array[AnyRef] = allRows
      .flatMap(data ⇒ refCols.map(_.asInstanceOf[RefColumn[AnyRef]].get(data).asInstanceOf[AnyRef]))
      .toArray[AnyRef]

    val buffer = ByteBuffer.allocate(500*1024*1024)
    val longBuffer = buffer.asLongBuffer()
    val thisColTuples = newScalarCols.map(col⇒this.schema.get(col.name))
    for(row ← rows) {
      for(thisCol ← thisColTuples) {
        val value: Double = try {
          thisCol.map(_.get(row).asInstanceOf[Number].doubleValue()).getOrElse(Double.NaN)
        } catch {
          case e : IllegalArgumentException ⇒ Double.NaN
        }
        longBuffer.put(java.lang.Double.doubleToLongBits(value))
      }
    }
    val leftColTuples = newScalarCols.map(col⇒left.schema.get(col.name))
    for(row ← left.rows) {
      for(leftCol ← leftColTuples) {
        val value: Double = try {
          leftCol.map(_.get(row).asInstanceOf[Number].doubleValue()).getOrElse(Double.NaN)
        } catch {
          case e : IllegalArgumentException ⇒ Double.NaN
        }
        longBuffer.put(java.lang.Double.doubleToLongBits(value))
      }
    }

    val bytes: Array[Byte] = java.util.Arrays.copyOfRange(buffer.array(), 0, longBuffer.position() * 8)
    val labels: Array[String] = allRows.map(data⇒data.label).toArray
    val labelNames = labels.distinct.sorted
    val map: List[(String, ValueColumn[AnyVal])] = newScalarCols.map((x: ValueColumn[AnyVal]) ⇒ (x.name,x))
    val result = new Page(
      schema = ListMap(map: _*),
      labelNames = labelNames,
      labels = labels.map(labelNames.indexOf(_)),
      refs = refVals,
      values = bytes
    )
    //println(s"$this + $left => $result")
    result
  }



  def getAll(rows: List[this.PageRow]) = {
    val indexes = rows.map(_.row).distinct.sorted.toArray
    new Page(
      schema = schema,
      labelNames = labelNames,
      labels = indexes.map(i ⇒ labels(i)),
      refs = indexes.flatMap(i ⇒ java.util.Arrays.copyOfRange[AnyRef](refs, i * rowRefSize, (i + 1) * rowRefSize)),
      values = indexes.flatMap(i ⇒ java.util.Arrays.copyOfRange(values, i * rowValueSize, (i + 1) * rowValueSize))
    )
  }

  class PageRow(val row : Int) extends KeyValue[String,Any] {
    def keys = schema
    def ref(col : Int) = refs(row * rowRefSize + col)
    def value(startIndex : Int, length : Int) = java.util.Arrays.copyOfRange(values,
      row * rowValueSize + startIndex,
      row * rowValueSize + startIndex + length)
    def get(key:String) = if(schema.contains(key)) Some(schema(key).get(PageRow.this)) else None
    override def apply(key:String) = schema(key).get(PageRow.this)
    def label = labelNames(labels(row))
    def asMap: Map[String, Any] = schema.mapValues(_.get(PageRow.this)).toMap
    def asClassificationTreeItem = new ClassificationTreeItem(asMap)
    def asLabeledItem = new LabeledItem(label, asClassificationTreeItem)
  }

  override def toString = s"Page($size rows, ${schema.size} fields, labels = $labelCounts)"
}

trait KeyValue[F,T] {
  def apply(key : F) : T = get(key).get
  def get(key : F) : Option[T]
}

case class ClassificationTreeItem(attributes: Map[String, Any]) extends KeyValue[String,Any] {
  override def get(key: String): Option[Any] = attributes.get(key)
}

object ClassificationTreeItem {
  lazy val empty = ClassificationTreeItem(Map.empty)
}

case class LabeledItem(label: String, value: ClassificationTreeItem)
