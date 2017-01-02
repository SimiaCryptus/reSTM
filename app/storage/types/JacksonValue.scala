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
  val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule).enableDefaultTyping(DefaultTyping.NON_FINAL)
  val simpleMapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def simple(value: Any) = new JacksonValue(// Util.monitorBlock("JacksonValue.simple")
    {
      if (value.isInstanceOf[String]) {
        value.toString
      } else {
        val writer: StringWriter = new StringWriter()
        simpleMapper.writeValue(writer, value)
        writer.toString
      }
    })

  def apply(value: Any) = new JacksonValue(
    //Util.monitorBlock("JacksonValue.serialize")
    {
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


  def deserialize[T <: AnyRef : ClassTag](): Option[T] = //Util.monitorBlock("JacksonValue.deserialize")
  {
    val runtimeClass = classTag[T].runtimeClass
    if (classOf[String] == runtimeClass) {
      Option(data).asInstanceOf[Option[T]]
      //    } else if(classOf[scala.collection.Seq[_]].isAssignableFrom(runtimeClass) && runtimeClass.isAssignableFrom(classOf[scala.collection.immutable.List[_]])) {
      //      Option(this.toString).filterNot(_.isEmpty).map[T](json => {
      //        //def prototype = new TypeReference[T]() {}
      //        try { // See https://github.com/FasterXML/jackson-module-scala/issues/107
      //          //          mapper.readValue[T](json, new TypeReference[T] {})
      //          def prototype = runtimeClass.asInstanceOf[Class[T]]
      //          def prototypeInner = runtimeClass.getTypeParameters.head.getGenericDeclaration.asInstanceOf[Class[AnyRef]]
      //          import scala.collection.JavaConverters._
      //          val gson: Gson = new GsonBuilder().setPrettyPrinting().create()
      //          val element = gson.fromJson(json, classOf[JsonElement])
      //          val items: Iterator[String] = element.getAsJsonArray.iterator().asScala.map(gson.toJson(_))
      //          val returnValue = items.map(json => {
      //            try {
      //              mapper.readValue[AnyRef](new JsonFactory().createParser(json), prototypeInner)
      //            } catch {
      //              case e : Throwable => throw new IllegalArgumentException(s"Error deserializing to ${prototypeInner}: $json",e)
      //            }
      //          }).toList
      //          returnValue.asInstanceOf[T]
      //        } catch {
      //          case e : Throwable => throw new IllegalArgumentException(s"Error deserializing to ${runtimeClass}: $pretty",e)
      //        }
      //      })
    } else {
      Option(this.toString).filterNot(_.isEmpty).map[T](json => {
        //def prototype = new TypeReference[T]() {}
        try {
          // See https://github.com/FasterXML/jackson-module-scala/issues/107
          //          mapper.readValue[T](json, new TypeReference[T] {})
          def prototype = classTag[T].runtimeClass.asInstanceOf[Class[T]]

          mapper.readValue[T](json, prototype)
        } catch {
          case e: Throwable => throw new IllegalArgumentException(s"Error deserializing to $runtimeClass: $pretty", e)
        }
      })
    }
  }

  def pretty: String = // Util.monitorBlock("JacksonValue.pretty")
  {
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



