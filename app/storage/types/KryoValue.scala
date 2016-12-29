package storage.types

import java.io.ByteArrayOutputStream
import java.util.Base64

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.twitter.chill.{Output, ScalaKryoInstantiator}
import util.Util

import scala.reflect._

object KryoValue {
  private val kryo: ThreadLocal[Kryo] = new ThreadLocal[Kryo]{
    override def initialValue(): Kryo = (new ScalaKryoInstantiator).setRegistrationRequired(false).newKryo()
  }

  def apply(value: AnyRef) = new KryoValue(toString(value))

  def toString(value: AnyRef): String = Base64.getEncoder.encodeToString(serialize(value))

  def serialize(value: AnyRef): Array[Byte] = Util.monitorBlock("KryoValue.serialize") {
    val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream
    val output = new Output(byteArrayOutputStream)
    kryo.get().writeClassAndObject(output, value)
    output.close()
    byteArrayOutputStream.toByteArray
  }

  def deserialize[T <: AnyRef : ClassTag](data: String): Option[T] = {
    Option(data)
      .filterNot(_.isEmpty)
      .map(Base64.getDecoder.decode(_))
      .filterNot(_.isEmpty)
      .flatMap(deserialize[T])
  }

  def deserialize[T <: AnyRef : ClassTag](data: Array[Byte]): Option[T] = Util.monitorBlock("KryoValue.deserialize") {
    Option(data)
      .map(new Input(_))
      .map(kryo.get().readClassAndObject(_))
      .map(_.asInstanceOf[T])
  }
}

class KryoValue(val data: String) {

  def deserialize[T <: AnyRef : ClassTag](): Option[T] = KryoValue.deserialize[T](data)

  override def toString: String = data

  override def hashCode(): Int = data.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: KryoValue => data == x.data
    case _ => false
  }

}