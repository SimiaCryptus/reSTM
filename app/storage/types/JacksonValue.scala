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

package storage.types

import java.io.StringWriter
import java.nio.charset.Charset

import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping
import com.fasterxml.jackson.databind.{MappingIterator, ObjectMapper}
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

  def deserializeSimple[T <: AnyRef : ClassTag](data:String): Option[T] = //Util.monitorBlock("JacksonValue.deserialize")
  {
    val runtimeClass = classTag[T].runtimeClass
    if (classOf[String] == runtimeClass) {
      Option(data).asInstanceOf[Option[T]]
    } else {
      Option(this.toString).filterNot(_.isEmpty).map[T](json => {
        try {
          def prototype = classTag[T].runtimeClass.asInstanceOf[Class[T]]
          simpleMapper.readValue[T](json, prototype)
        } catch {
          case e: Throwable => throw new IllegalArgumentException(s"Error deserializing to $runtimeClass: $data", e)
        }
      })
    }
  }

  def deserializeSimpleStream[T <: AnyRef : ClassTag](data:String): Stream[T] = //Util.monitorBlock("JacksonValue.deserialize")
  {
    val runtimeClass = classTag[T].runtimeClass
    if (classOf[String] == runtimeClass) {
      List(data.asInstanceOf[T]).toStream
    } else {
      Option(this.toString).filterNot(_.isEmpty).map[Stream[T]](json => {
        try {
          def prototype = classTag[T].runtimeClass.asInstanceOf[Class[T]]
          val iterator: MappingIterator[T] = simpleMapper.readValues[T](mapper.getFactory.createParser(data), prototype)
          def next = Option(iterator.hasNext).filter(x ⇒ x).map(_⇒iterator.next())
          Stream.continually(next).takeWhile(_.isDefined).map(_.get)
        } catch {
          case e: Throwable => throw new IllegalArgumentException(s"Error deserializing to $runtimeClass: $data", e)
        }
      }).getOrElse(Stream.empty)
    }
  }

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
    } else {
      Option(this.toString).filterNot(_.isEmpty).map[T](json => {
        try {
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



