package storage.data

import java.io.ByteArrayOutputStream
import java.util.Base64

import com.esotericsoftware.kryo.io.Input
import com.twitter.chill.{Output, ScalaKryoInstantiator}

import scala.reflect._

object KryoValue {
  private def kryo = (new ScalaKryoInstantiator).setRegistrationRequired(false).newKryo()
}
import storage.data.KryoValue._

class KryoValue(val data: String) {

  def this(value: AnyRef) = this({
    val output = new Output(new ByteArrayOutputStream)
    kryo.writeObject(output, value)
    Base64.getEncoder.encodeToString(output.toBytes)
  })

  def deserialize[T<:AnyRef:ClassTag](): Option[T] = {
    Option(this.toString)
      .filterNot(_.isEmpty)
      .map(Base64.getDecoder.decode(_))
      .filterNot(_.isEmpty)
      .flatMap(bytes => Option(kryo.readObject(new Input(bytes), classTag[T].runtimeClass.asInstanceOf[Class[T]])))
  }

  override def toString: String = data

  override def hashCode(): Int = data.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: KryoValue => data == x.data
    case _ => false
  }

}