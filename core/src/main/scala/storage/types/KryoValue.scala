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

import java.util.Base64

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.twitter.chill.{Output, ScalaKryoInstantiator}
import net.jpountz.lz4.{LZ4Compressor, LZ4Factory, LZ4SafeDecompressor}

import scala.reflect._

object KryoValue {
  def empty[T <: AnyRef] : KryoValue[T] = new KryoValue[T](null:String)

  private def lZ4Factory = LZ4Factory.fastestInstance()

  private val kryo: ThreadLocal[Kryo] = new ThreadLocal[Kryo] {
    override def initialValue(): Kryo = (new ScalaKryoInstantiator).setRegistrationRequired(false).newKryo()
  }
  private val compressor: ThreadLocal[LZ4Compressor] = new ThreadLocal[LZ4Compressor] {
    override def initialValue(): LZ4Compressor = lZ4Factory.fastCompressor()
  }
  private val decompressor = new ThreadLocal[LZ4SafeDecompressor] {
    override def initialValue() = lZ4Factory.safeDecompressor()
  }
  private val buffer: ThreadLocal[Array[Byte]] = new ThreadLocal[Array[Byte]] {
    override def initialValue(): Array[Byte] = new Array[Byte](8*1024*1024)
  }

  def apply[T <: AnyRef](value: T) = new KryoValue[T](toString(value))

  def toString(value: AnyRef): String = Base64.getEncoder.encodeToString(serialize(value))


  def serialize(value: AnyRef): Array[Byte] = // Util.monitorBlock("KryoValue.serialize")
  {
    val output = new Output(buffer.get())
    kryo.get().writeClassAndObject(output, value)
    output.close()
    compressor.get().compress(output.getBuffer, 0, output.position())
  }

  def deserialize[T <: AnyRef](data: String)(implicit classTag: ClassTag[T]): Option[T] = {
    Option(data)
      .filterNot(_.isEmpty)
      .map(Base64.getDecoder.decode(_))
      .filterNot(_.isEmpty)
      .flatMap(deserialize[T])
  }

  def deserialize[T <: AnyRef : ClassTag](data: Array[Byte]): Option[T] = // Util.monitorBlock("KryoValue.deserialize")
  {
    Option(data)
      .map(decompressor.get().decompress(_, buffer.get()))
      .map(new Input(buffer.get, 0, _))
      .map(kryo.get().readClassAndObject(_))
      .map(_.asInstanceOf[T])
  }
}

class KryoValue[T <: AnyRef](val data: String) {

  def deserialize()(implicit classTag: ClassTag[T]): Option[T] = KryoValue.deserialize[T](data)

  override def toString: String = data

  override def hashCode(): Int = data.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: KryoValue[_] => data == x.data
    case _ => false
  }

}