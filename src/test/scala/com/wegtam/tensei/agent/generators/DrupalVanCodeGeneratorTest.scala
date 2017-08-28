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

package com.wegtam.tensei.agent.generators

import akka.testkit.{ EventFilter, TestActorRef }
import akka.util.ByteString
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.generators.BaseGenerator.BaseGeneratorMessages.{
  GeneratorResponse,
  PrepareToGenerate,
  ReadyToGenerate,
  StartGenerator
}

class DrupalVanCodeGeneratorTest extends ActorSpec {
  describe("Generators") {
    describe("VancodeGenerator") {
      describe("when given no params") {
        it("should log an error") {
          val actor = TestActorRef(DrupalVanCodeGenerator.props)
          actor ! PrepareToGenerate
          expectMsg(ReadyToGenerate)
          EventFilter[NoSuchElementException](
            message = "Vancode transformer couldn't find the article field.",
            source = actor.path.toString,
            occurrences = 1
          ) intercept {
            actor ! StartGenerator(List())
          }
        }
      }

      describe("when given an articleID and a commentID") {
        it("should return 01/") {
          val actor = TestActorRef(DrupalVanCodeGenerator.props)
          actor ! PrepareToGenerate
          expectMsg(ReadyToGenerate)
          val params = List(("article", "1"), ("commentid", "1"))
          actor ! StartGenerator(params)
          val d = expectMsgType[GeneratorResponse]
          d.data.head should be(ByteString("01/"))
        }
      }

      describe("when given two comments on the same article") {
        it("should return two different vancodes") {
          val actor = TestActorRef(DrupalVanCodeGenerator.props)
          actor ! PrepareToGenerate
          expectMsg(ReadyToGenerate)
          val params = List(("article", "2"), ("commentid", "1"), ("parent", "0"))
          actor ! StartGenerator(params)
          val d = expectMsgType[GeneratorResponse]
          d.data.head should be(ByteString("01/"))
          val params2 = List(("article", "2"), ("commentid", "2"), ("parent", "0"))
          actor ! StartGenerator(params2)
          val e = expectMsgType[GeneratorResponse]
          e.data.head should be(ByteString("02/"))
        }
      }

      describe("when given two comments on different articles") {
        it("should return the same vancodes") {
          val actor = TestActorRef(DrupalVanCodeGenerator.props)
          actor ! PrepareToGenerate
          expectMsg(ReadyToGenerate)
          val params = List(("article", "3"), ("commentid", "1"))
          actor ! StartGenerator(params)
          val d = expectMsgType[GeneratorResponse]
          d.data.head should be(ByteString("01/"))
          val params2 = List(("article", "4"), ("commentid", "2"))
          actor ! StartGenerator(params2)
          val e = expectMsgType[GeneratorResponse]
          e.data.head should be(ByteString("01/"))
        }
      }

      describe("when given a comment and a reply on this comment") {
        it("should return two different vancodes") {
          val actor = TestActorRef(DrupalVanCodeGenerator.props)
          actor ! PrepareToGenerate
          expectMsg(ReadyToGenerate)
          val params = List(("article", "5"), ("commentid", "1"), ("parent", "0"))
          actor ! StartGenerator(params)
          val d = expectMsgType[GeneratorResponse]
          d.data.head should be(ByteString("01/"))

          val params2 = List(("article", "5"), ("commentid", "2"), ("parent", "1"))
          actor ! StartGenerator(params2)
          val e = expectMsgType[GeneratorResponse]
          e.data.head should be(ByteString("01.00/"))
        }
      }

      describe("when given 36 comments on the same level") {
        it("should return 110/ for the last comment") {
          val actor = TestActorRef(DrupalVanCodeGenerator.props)
          actor ! PrepareToGenerate
          expectMsg(ReadyToGenerate)
          var i = 1
          while (i < 36) {
            val params = List(("article", "1"), ("commentid", i.toString), ("parent", "0"))
            actor ! StartGenerator(params)
            expectMsgType[GeneratorResponse]
            i += 1
          }
          val params2 = List(("article", "1"), ("commentid", "36"), ("parent", "0"))
          actor ! StartGenerator(params2)
          val d = expectMsgType[GeneratorResponse]
          d.data.head should be(ByteString("110/"))
        }
      }
    }
  }
}
