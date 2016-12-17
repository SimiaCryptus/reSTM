package util

import java.io.File

import org.apache.commons.io.FileUtils

import scala.collection.JavaConverters._


object Config {
  val properties: Map[String, String] = System.getProperties.asScala.toMap
  lazy val configFile: Map[String, String] = Option(new File("restm.config"))
    .filter(_.exists())
    .map(FileUtils.readLines(_, "UTF-8"))
    .map(_.asScala.toList).getOrElse(List.empty)
    .map(_.trim).filterNot(_.startsWith("//"))
    .map(_.split("=").map(_.trim)).filter(2 == _.size)
    .map(split => split(0) -> split(1)).toMap
  def getConfig(key:String) = {
    configFile.get(key).orElse(properties.get(key))
  }
}
