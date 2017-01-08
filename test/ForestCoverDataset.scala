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
import java.util.zip.GZIPInputStream

import org.apache.commons.io.IOUtils
import stm.collection.clustering.ClassificationTreeItem

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
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
  lazy val dataSet: List[ClassificationTreeItem] = {
    val inputStream = new GZIPInputStream(new FileInputStream("covtype.data.gz"))
    println("Reading covtype")
    val rawLines = IOUtils.readLines(inputStream, "UTF8").asScala.map(_.trim).filterNot(_.isEmpty).toList
    println("Shuffling data")
    val lines = Random.shuffle(rawLines).toArray
    println(s"Read ${lines.size} lines")
    val items = lines
      .map(_.split(",").map(Integer.parseInt).toArray)
      .map((values: Array[Int]) => {
        val combined = fields.zip(values).toMap
        ClassificationTreeItem(combined)
      }).filter(_.attributes.contains("Cover_Type")).toList

    println(s"Loaded covtype ${items.size} items")
    items
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
