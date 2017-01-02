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

import java.util.UUID

import org.scalatestplus.play._
import play.api.test.Helpers._
import play.api.test._

class ApplicationSpec extends PlaySpec with OneAppPerTest {

  "HomeController" should {
    "render the index page" in {
      val home = route(app, FakeRequest(GET, "/")).get
      status(home) mustBe OK
      contentType(home) mustBe Some("text/html")
      contentAsString(home) must include("Your new application is ready.")
    }
  }

  "TxnController" should {

    "write values on commit" in {
      val txnA: String = contentAsString(route(app, FakeRequest(GET, "/txn")).get)
      require(!txnA.isEmpty)

      val valId: String = UUID.randomUUID().toString
      route(app, FakeRequest(GET, s"/mem/$valId?time=$txnA")).map(status) mustBe Some(404)
      route(app, FakeRequest(POST, s"/mem/$valId?time=$txnA")).map(status) mustBe Some(200)
      route(app, FakeRequest(PUT, s"/mem/$valId?time=$txnA").withTextBody("TestData")).map(status) mustBe Some(200)
      route(app, FakeRequest(POST, s"/txn/$txnA").withTextBody("TestData")).map(status) mustBe Some(200)

      val txnB: String = contentAsString(route(app, FakeRequest(GET, "/txn")).get)
      route(app, FakeRequest(GET, s"/mem/$valId?time=$txnB")).map(status) mustBe Some(200)
      route(app, FakeRequest(GET, s"/mem/$valId?time=$txnB")).map(contentAsString) mustBe Some("TestData")
    }

    "discard values on rollback" in {
      val txnA: String = contentAsString(route(app, FakeRequest(GET, "/txn")).get)
      require(!txnA.isEmpty)

      val valId: String = UUID.randomUUID().toString
      route(app, FakeRequest(GET, s"/mem/$valId?time=$txnA")).map(status) mustBe Some(404)
      route(app, FakeRequest(POST, s"/mem/$valId?time=$txnA")).map(status) mustBe Some(200)
      route(app, FakeRequest(PUT, s"/mem/$valId?time=$txnA").withTextBody("TestData")).map(status) mustBe Some(200)
      route(app, FakeRequest(DELETE, s"/txn/$txnA").withTextBody("TestData")).map(status) mustBe Some(200)

      val txnB: String = contentAsString(route(app, FakeRequest(GET, "/txn")).get)
      route(app, FakeRequest(GET, s"/mem/$valId?time=$txnB")).map(status) mustBe Some(404)
    }

  }

}
