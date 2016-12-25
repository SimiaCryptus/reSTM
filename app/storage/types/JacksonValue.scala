package storage.types

import java.io.StringWriter
import java.nio.charset.Charset

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.gson.{Gson, GsonBuilder, JsonElement}

import scala.reflect._

object JacksonValue {
  val utf8: Charset = java.nio.charset.Charset.forName("UTF-8")
  val mapper = new ObjectMapper().registerModule(DefaultScalaModule).enableDefaultTyping(DefaultTyping.NON_FINAL)
  val simpleMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def simple(value: Any) = new JacksonValue({
    if (value.isInstanceOf[String]) {
      value.toString
    } else {
      val writer: StringWriter = new StringWriter()
      simpleMapper.writeValue(writer, value)
      writer.toString
    }
  })

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

import storage.types.JacksonValue._

class JacksonValue(val data: String) {

  def deserialize[T <: AnyRef : ClassTag](): Option[T] = {
    if(classOf[String] == classTag[T].runtimeClass) {
      Option(data).asInstanceOf[Option[T]]
    } else {
      Option(this.toString).filterNot(_.isEmpty).map[T](json => {
        //def prototype = new TypeReference[T]() {}
        def prototype = classTag[T].runtimeClass.asInstanceOf[Class[T]]
        mapper.readValue[T](json, prototype)
      })
    }
  }

  def pretty: String = {
    val gson: Gson = new GsonBuilder().setPrettyPrinting().create()
    gson.toJson(gson.fromJson(toString, classOf[JsonElement]))
  }

  override def toString: String = data

  override def hashCode(): Int = data.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: JacksonValue => data == x.data
    case _ => false
  }

}



