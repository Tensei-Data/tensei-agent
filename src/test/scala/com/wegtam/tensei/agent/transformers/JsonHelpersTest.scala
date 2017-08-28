/*
 * Copyright (C) 2014 - 2017  Contributors as noted in the AUTHORS.md file
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.wegtam.tensei.agent.transformers

import java.math.BigDecimal

import argonaut._, Argonaut._

import java.lang.{ Double, Short }

import com.wegtam.tensei.agent.DefaultSpec

class JsonHelpersTest extends DefaultSpec with JsonHelpers {
  describe("Transformers") {
    describe("JsonHelpers") {
      describe("createJson") {
        describe("with an unsupported data type") {
          it("should throw an exception") {
            an[RuntimeException] should be thrownBy createJson(Short.valueOf("1"), "")
          }
        }

        describe("with a supported data type") {
          it("should encode a string") {
            createJson("foo", "") should be("foo".asJson)
          }

          it("should encode an integer") {
            val number = new Integer(13)
            createJson(number, "") should be(number.asJson)
          }

          it("should encode a list of strings as array") {
            val data = List("foo", "bar")
            createJson(data, "") should be(data.asJson)
          }

          it("should encode a big decimal") {
            // FIXME Adopt this test if argonaut supports big decimal as jNumber!
            val data = new BigDecimal("3.141592653589793")
            createJson(data, "") should be("3.141592653589793".asJson)
          }
        }
      }

      describe("createJsonArray") {
        describe("containing an unsupported data type") {
          it("should throw an exception") {
            val data = List("foo", "bar", Short.valueOf("1"), "baz")

            an[RuntimeException] should be thrownBy createJsonArray(data)
          }
        }

        describe("containing only supported data types") {
          it("should create the array properly") {
            val data =
              List("foo", "bar", new Integer(9), "fancy stuff", new Double(2.71), "ending")

            val json = """["foo","bar","9","fancy stuff",2.71,"ending"]"""

            createJsonArray(data).nospaces should be(json)
          }
        }
      }

      describe("createJsonObject") {
        describe("containing an unsupported data type") {
          it("should throw an exception") {
            val data   = List("foo", "bar", Short.valueOf("1"), "baz")
            val labels = List("1", "2", "3", "4")

            an[RuntimeException] should be thrownBy createJsonObject(data, labels)
          }
        }

        describe("containing only supported data types") {
          it("should create the object properly") {
            val data =
              List("foo", "bar", new Integer(9), "fancy stuff", new Double(2.71), "ending", None)
            val lables = List("1", "2", "3", "4", "5", "6", "7")

            val json =
              """
                |{
                |  "1": "foo",
                |  "2": "bar",
                |  "3": "9",
                |  "4": "fancy stuff",
                |  "5": 2.71,
                |  "6": "ending",
                |  "7": null
                |}
              """.stripMargin

            val expectedJson = json.parseOption.get

            createJsonObject(data, lables).nospaces should be(expectedJson.nospaces)
          }

          it("should create a proper object if the data contains lists") {
            val data = List("foo",
                            "bar",
                            new Integer(9),
                            List("fancy stuff", "not so fancy stuff"),
                            new Double(2.71),
                            "ending")
            val lables = List("1", "2", "3", "4", "5", "6")

            val json =
              """
                |{
                |  "1": "foo",
                |  "2": "bar",
                |  "3": "9",
                |  "4": ["fancy stuff", "not so fancy stuff"],
                |  "5": 2.71,
                |  "6": "ending"
                |}
              """.stripMargin

            val expectedJson = json.parseOption.get

            createJsonObject(data, lables).nospaces should be(expectedJson.nospaces)
          }
        }
      }
    }
  }
}
