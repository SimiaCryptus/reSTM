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

import java.io.FileInputStream
import java.nio.ByteBuffer
import java.util.zip.GZIPInputStream

import com.google.common.primitives.Longs
import org.apache.commons.io.IOUtils
import stm.collection.clustering.Page.DoubleColumn
import stm.collection.clustering.{ClassificationTreeItem, Page}

import scala.collection.JavaConverters._
import scala.collection.immutable.{ListMap, Seq}
import scala.util.Random

object ForestCoverDataset {
  lazy val fields = List(
    List("Elevation"), //                              quantitative    meters                       Elevation in meters
    List("Aspect"), //                                 quantitative    azimuth                      Aspect in degrees azimuth
    List("Slope"), //                                  quantitative    degrees                      Slope in degrees
    List("Horizontal_Distance_To_Hydrology"), //       quantitative    meters                       Horz Dist to nearest surface water features
    List("Vertical_Distance_To_Hydrology"), //         quantitative    meters                       Vert Dist to nearest surface water features
    List("Horizontal_Distance_To_Roadways"), //        quantitative    meters                       Horz Dist to nearest roadway
    List("Hillshade_9am"), //                          quantitative    0 to 255 index               Hillshade index at 9am, summer solstice
    List("Hillshade_Noon"), //                         quantitative    0 to 255 index               Hillshade index at noon, summer soltice
    List("Hillshade_3pm"), //                          quantitative    0 to 255 index               Hillshade index at 3pm, summer solstice
    List("Horizontal_Distance_To_Fire_Points"), //    quantitative    meters                       Horz Dist to nearest wildfire ignition points
    (1 to 4).map(i => s"Wilderness_Area_$i"), // (4 binary columns)     qualitative     0 (absence) or 1 (presence)  Wilderness area designation
    (1 to 40).map(i => s"Soil_Type_$i"), // (40 binary columns)          qualitative     0 (absence) or 1 (presence)  Soil Type designation
    List("Cover_Type") // (7 types)                    integer         1 to 7                       Forest Cover Type designation
  ).flatten.toArray
  lazy val dataSet: Page = {
    val inputStream = new GZIPInputStream(new FileInputStream("covtype.data.gz"))
    //println("Reading covtype")
    val lines = Random.shuffle(IOUtils.readLines(inputStream, "UTF8").asScala.map(_.trim).filterNot(_.isEmpty)).toArray
    //println(s"Read ${lines.size} lines")
    val items: ListMap[String, Array[Array[Byte]]] = ListMap(lines.map((line: String) ⇒{
      line.split(",").map(Integer.parseInt(_).toDouble)
    }).groupBy(_.last)
      .mapValues(_.map(_.dropRight(1)).map(_.map(java.lang.Double.doubleToLongBits).flatMap(Longs.toByteArray)))
      .map(t⇒t._1.toString → t._2)
      .toList.sortBy(_._1):_*)
    //println(s"Parsed ${items.size} types")
    val labelNames: Array[String] = items.map(_._1).toArray
    val labels: Array[Int] = items.flatMap(t⇒{
      val (key: String, block) = t;
      val i = labelNames.indexOf(key)
      (0 until block.length).map(_⇒i)
    }).toArray
    val schema = (0 until fields.length-1).map(i⇒new DoubleColumn(fields(i),i*8)).toArray[Page.BaseColumn[_]]
    val buffer = ByteBuffer.allocate(500*1024*1024)
    for(item ← items) {
      for(row ← item._2) {
        buffer.put(row)
      }
    }
    val data: Array[Byte] = java.util.Arrays.copyOfRange(buffer.array(), 0, buffer.position())
    val page = new Page(
      schema = ListMap(schema.map(x⇒x.name→x).sortBy(_._1):_*),
      labelNames = labelNames,
      labels = labels,
      refs = Array.empty,
      values = data
    )
    //println(s"Page Example Datum: ${page.rows.head.asMap}")
    //println(s"Page Ready: ${data.length} bytes")
    page
  }
  def asTrainingSet(dataSet: Seq[ClassificationTreeItem]): Map[String, List[ClassificationTreeItem]] = {
    val grouped = dataSet.map(item ⇒ {
      item.attributes("Cover_Type").toString → item.copy(attributes = item.attributes - "Cover_Type")
    }).groupBy(_._1).map(t => {
      val (cover_type: String, stream: Seq[(String, ClassificationTreeItem)]) = t
      cover_type → stream.map(_._2).toList
    })
    require(dataSet.size == grouped.map(_._2.size).sum)
    grouped
  }
}
