package storage.data

import java.io.StringWriter
import java.nio.charset.Charset

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect._

object JacksonValue {
  val utf8: Charset = java.nio.charset.Charset.forName("UTF-8")
  val mapper = new ObjectMapper().registerModule(DefaultScalaModule).enableDefaultTyping(DefaultTyping.NON_FINAL)

  def apply(value: Any) = new JacksonValue({
    if (value.isInstanceOf[String]) {
      value.toString
    } else {
      val writer: StringWriter = new StringWriter()
      mapper.writeValue(writer, value)
      writer.toString
    }
  })
}

import storage.data.JacksonValue._

class JacksonValue(val data: String) {

  def deserialize[T <: AnyRef : ClassTag](): Option[T] = {
    Option(this.toString).filterNot(_.isEmpty).map[T](json => {
      //def prototype = new TypeReference[T]() {}
      def prototype = classTag[T].runtimeClass.asInstanceOf[Class[T]]
      mapper.readValue[T](json, prototype)
    })
  }

  override def toString: String = data

  override def hashCode(): Int = data.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: JacksonValue => data == x.data
    case _ => false
  }

}



