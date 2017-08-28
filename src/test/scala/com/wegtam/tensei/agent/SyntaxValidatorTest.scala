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

package com.wegtam.tensei.agent

import akka.testkit.TestActorRef
import com.wegtam.tensei.adt.DFASDL
import com.wegtam.tensei.agent.SyntaxValidator.SyntaxValidatorMessages

import scalaz._, Scalaz._

class SyntaxValidatorTest extends ActorSpec {
  describe("SyntaxValidator") {
    describe("when receiving a list of valid dfasdls") {
      it("should return a list of Success(DFASDL)") {
        val dfasdls = List(
          new DFASDL(
            "DFASDL-01",
            """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem"><str id="foo"/></dfasdl>"""
          ),
          new DFASDL(
            "DFASDL-02",
            """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem"><str id="bar"/></dfasdl>"""
          )
        )
        val validator = TestActorRef(SyntaxValidator.props(Option("SyntaxValidatorTest")))

        validator ! SyntaxValidatorMessages.ValidateDFASDLs(dfasdls)

        val response = expectMsgType[SyntaxValidatorMessages.ValidateDFASDLsResults]

        response.results.size should be(dfasdls.size)
        response.results.foreach(_.isSuccess should be(true))
        val responseDfasdls = response.results.map(_.getOrElse(new DFASDL("FAILED", "")))
        responseDfasdls shouldEqual dfasdls
      }
    }

    describe("when receiving some invalid dfasdls") {
      it("should return the appropriate failures") {
        val dfasdls = List(
          new DFASDL(
            "DFASDL-01",
            """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem"><str id="foo"/></dfasdl>"""
          ),
          new DFASDL(
            "DFASDL-02",
            """<?xml version="1.0" encoding="UTF-8"?><dfasdl><seq id="empty-sequence"/></dfasdl>"""
          ),
          new DFASDL(
            "DFASDL-03",
            """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem"><str id="bar"/></dfasdl>"""
          )
        )
        val validator = TestActorRef(SyntaxValidator.props(Option("SyntaxValidatorTest")))

        validator ! SyntaxValidatorMessages.ValidateDFASDLs(dfasdls)

        val response = expectMsgType[SyntaxValidatorMessages.ValidateDFASDLsResults]

        response.results.size should be(dfasdls.size)
        response.results(0) should be(dfasdls(0).successNel[String])
        response.results(1).isFailure should be(true)
        response.results(2) should be(dfasdls(2).successNel[String])
      }
    }
  }
}
