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

package com.wegtam.tensei.agent.adt

import argonaut._, Argonaut._
import com.wegtam.tensei.agent.DefaultSpec
import com.wegtam.tensei.agent.adt.ParserStatus._

class ParserStatus$Test extends DefaultSpec {
  describe("ParserStatus") {
    describe("JsonCodec") {
      describe("encode") {
        it("should properly encode an object to json") {
          ParserStatus.ABORTED.asJson.nospaces shouldEqual """"ABORTED""""
          ParserStatus.COMPLETED.asJson.nospaces shouldEqual """"COMPLETED""""
          ParserStatus.COMPLETED_WITH_ERROR.asJson.nospaces shouldEqual """"COMPLETED_WITH_ERROR""""
          ParserStatus.END_OF_DATA.asJson.nospaces shouldEqual """"END_OF_DATA""""
        }
      }

      describe("decode") {
        it("should properly decode json to an object") {
          def checkValue(name: String, expected: ParserStatusType): Unit = {
            val s = Parse.decodeOption[ParserStatusType](s""""$name"""")
            s.isDefined should be(true)
            s.get should be(expected)
            ()
          }

          val names = List("ABORTED", "COMPLETED", "COMPLETED_WITH_ERROR", "END_OF_DATA")
          val types = List(ABORTED, COMPLETED, COMPLETED_WITH_ERROR, END_OF_DATA)
          val both  = names zip types
          both.foreach(entry => checkValue(entry._1, entry._2))
        }
      }
    }
  }
}
