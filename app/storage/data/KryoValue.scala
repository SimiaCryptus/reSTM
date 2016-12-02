package storage.data

import java.io.ByteArrayOutputStream
import java.util.Base64

import com.esotericsoftware.kryo.io.Input
import com.twitter.chill.{Output, ScalaKryoInstantiator}

import scala.reflect._

object KryoValue {
  private def kryo = (new ScalaKryoInstantiator).setRegistrationRequired(false).newKryo()

  def apply(value: AnyRef) = new KryoValue({
    val byteArrayOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream
    val output = new Output(byteArrayOutputStream)
    kryo.writeClassAndObject(output, value)
    output.close()
    val str: String = Base64.getEncoder.encodeToString(byteArrayOutputStream.toByteArray)
    str
  })
}
import storage.data.KryoValue._

class KryoValue(val data: String) {

  def deserialize[T<:AnyRef:ClassTag](): Option[T] = {
    val maybeBytes: Option[Array[Byte]] = Option(this.toString)
      .filterNot(_.isEmpty)
      .map(Base64.getDecoder.decode(_))
      .filterNot(_.isEmpty)
    val retVal: Option[AnyRef] = maybeBytes
      .map(new Input(_))
      .map(kryo.readClassAndObject(_))
    retVal
      .map(_.asInstanceOf[T])
  }

  override def toString: String = data

  override def hashCode(): Int = data.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: KryoValue => data == x.data
    case _ => false
  }

}