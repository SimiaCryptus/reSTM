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

package util

import java.io.File

import org.apache.commons.io.FileUtils

import scala.collection.JavaConverters._


object Config {
  lazy val configFile: Map[String, String] = Option(new File("restm.config"))
    .filter(_.exists())
    .map(FileUtils.readLines(_, "UTF-8"))
    .map(_.asScala.toList).getOrElse(List.empty)
    .map(_.trim).filterNot(_.startsWith("//"))
    .map(_.split("=").map(_.trim)).filter(2 == _.length)
    .map(split => split(0) -> split(1)).toMap
  val properties: Map[String, String] = System.getProperties.asScala.toMap

  def getConfig(key: String): Option[String] = {
    configFile.get(key).orElse(properties.get(key))
  }
}
