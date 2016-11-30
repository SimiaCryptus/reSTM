package storage.data

import java.io.ByteArrayOutputStream
import java.util.Base64

import com.esotericsoftware.kryo.io.Input
import com.twitter.chill.{KryoInstantiator, KryoPool, Output, ScalaKryoInstantiator}

object KryoValue {
  private def kryo = (new ScalaKryoInstantiator).setRegistrationRequired(false).newKryo()
}
import KryoValue._

class KryoValue(val data: String) {

  def this(value: AnyRef) = this({
    val output = new Output(new ByteArrayOutputStream)
    kryo.writeObject(output, value)
    Base64.getEncoder.encodeToString(output.toBytes)
  })

  def deserialize[T](clazz: Class[T]): Option[T] = {
    Option(this.toString)
      .filterNot(_.isEmpty)
      .map(Base64.getDecoder.decode(_))
      .filterNot(_.isEmpty)
      .flatMap(bytes => Option(kryo.readObject(new Input(bytes), clazz)))
  }

  override def toString: String = data

  override def hashCode(): Int = data.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: KryoValue => data == x.data
    case _ => false
  }

}