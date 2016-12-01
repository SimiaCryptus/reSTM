import org.scalatest.{MustMatchers, WordSpec}
import storage.data.JacksonValue

import scala.reflect.ClassTag

case class TestObj(name: String)

class JacksonValueSpec extends WordSpec with MustMatchers {

  "JacksonValue" should {
    "serialize values" in {
      val input: TestObj = new TestObj("foo")
      val string: String = new JacksonValue(input).toString
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

  def verify[T<:AnyRef:ClassTag](input: T): Unit = {
    val string: String = new JacksonValue(input).toString
    val output = new JacksonValue(string).deserialize[T]().get
    input mustBe output
  }
}
