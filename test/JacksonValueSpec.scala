import org.scalatest.{MustMatchers, WordSpec}
import storage.types.{JacksonValue, KryoValue}

import scala.reflect.ClassTag

case class TestObj(name: String)

class JacksonValueSpec extends WordSpec with MustMatchers {

  "JacksonValue" should {
    "serialize values" in {
      val input: TestObj = new TestObj("foo")
      val string: String = JacksonValue(input).toString
      val output: TestObj = new JacksonValue(string).deserialize[TestObj]().get
      input mustBe output
    }
    "serialize generic values" in {
      verify[TestObj](new TestObj("foo"))
      verify[Option[TestObj]](Option(new TestObj("foo")))
      verify[Option[TestObj]](None)
      verify(Option(List(new TestObj("foo"))))
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
      val input: TestObj = new TestObj("foo")
      val string: String = KryoValue(input).toString
      val output: TestObj = new KryoValue[TestObj](string).deserialize().get
      input mustBe output
    }
    "serialize generic values" in {
      verify[TestObj](new TestObj("foo"))
      verify[Option[TestObj]](Option(new TestObj("foo")))
      verify[Option[TestObj]](None)
      verify("foo")
      verify(List("foo"))
      verify(List(new TestObj("foo")))
      verify(Option(List(new TestObj("foo"))))
      verify[List[Option[TestObj]]](List(Option(new TestObj("foo"))))
    }
  }

  def verify[T <: AnyRef : ClassTag](input: T): Unit = {
    val string: String = KryoValue(input).toString
    val output = new KryoValue[T](string).deserialize().get
    input mustBe output
  }
}
