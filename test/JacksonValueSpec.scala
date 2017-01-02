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

import org.scalatest.{MustMatchers, WordSpec}
import storage.types.{JacksonValue, KryoValue}

import scala.reflect.ClassTag

case class TestObj(name: String)

class JacksonValueSpec extends WordSpec with MustMatchers {

  "JacksonValue" should {
    "serialize values" in {
      val input: TestObj = TestObj("foo")
      val string: String = JacksonValue(input).toString
      val output: TestObj = new JacksonValue(string).deserialize[TestObj]().get
      input mustBe output
    }
    "serialize generic values" in {
      verify[TestObj](TestObj("foo"))
      verify[Option[TestObj]](Option(TestObj("foo")))
      verify[Option[TestObj]](None)
      verify(Option(List(TestObj("foo"))))
      //verify[List[Option[TestObj]]](List(Option(new TestObj("foo"))))
    }
  }

  def verify[T <: AnyRef : ClassTag](input: T): Unit = {
    val string: String = JacksonValue(input).toString
    val output = new JacksonValue(string).deserialize[T]().get
    input mustBe output
  }
}

class KryoValueSpec extends WordSpec with MustMatchers {

  "KryoValue" should {
    "serialize values" in {
      val input: TestObj = TestObj("foo")
      val string: String = KryoValue(input).toString
      val output: TestObj = new KryoValue[TestObj](string).deserialize().get
      input mustBe output
    }
    "serialize generic values" in {
      verify[TestObj](TestObj("foo"))
      verify[Option[TestObj]](Option(TestObj("foo")))
      verify[Option[TestObj]](None)
      verify("foo")
      verify(List("foo"))
      verify(List(TestObj("foo")))
      verify(Option(List(TestObj("foo"))))
      verify[List[Option[TestObj]]](List(Option(TestObj("foo"))))
    }
  }

  def verify[T <: AnyRef : ClassTag](input: T): Unit = {
    val string: String = KryoValue(input).toString
    val output = new KryoValue[T](string).deserialize().get
    input mustBe output
  }
}
