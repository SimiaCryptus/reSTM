package storage.data

import java.io.StringWriter
import java.nio.charset.Charset

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule


object JacksonValue {
  val utf8: Charset = java.nio.charset.Charset.forName("UTF-8")
  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

}

import storage.data.JacksonValue._

class JacksonValue(val data: String) {

  def this(value: AnyRef) = this({
    if (value.isInstanceOf[String]) {
      value.toString
    } else {
      val writer: StringWriter = new StringWriter()
      mapper.writeValue(writer, value)
      writer.toString
    }
  })

  def deserialize[T](clazz: Class[T]): Option[T] = {
    Option(this.toString).filterNot(_.isEmpty).flatMap(json => Option(mapper.readValue(json, clazz)))
  }

  override def toString: String = data

  override def hashCode(): Int = data.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: JacksonValue => data == x.data
    case _ => false
  }

}
