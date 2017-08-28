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

class IDGeneratorTest extends ActorSpec {
  describe("Generators") {
    describe("IDGenerator") {
      describe("when given params with type=uuid") {
        it("should return an UUID as string") {
          val actor = TestActorRef(IDGenerator.props)
          actor ! PrepareToGenerate
          expectMsg(ReadyToGenerate)
          val params = List(("type", "uuid"))
          actor ! StartGenerator(params)
          val d = expectMsgType[GeneratorResponse]
          d.data.foreach {
            case bs: ByteString =>
              bs.utf8String should fullyMatch regex """[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"""
            case otherData =>
              otherData.toString should fullyMatch regex """[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"""
          }
        }
      }

      describe("when given params with a system name") {
        it("should return id 1") {
          val actor = TestActorRef(IDGenerator.props)
          actor ! PrepareToGenerate
          expectMsg(ReadyToGenerate)
          val params = List(("type", "long"), ("field", "Test"), ("start", "10"))
          actor ! StartGenerator(params)
          val d = expectMsgType[GeneratorResponse]
          d.data shouldEqual List(10L)
        }
      }

      describe("when given JoomlaUser as field name") {
        it("should return id 820") {
          val actor = TestActorRef(IDGenerator.props)
          actor ! PrepareToGenerate
          expectMsg(ReadyToGenerate)
          val params = List(("field", "JoomlaUserID"))
          actor ! StartGenerator(params)
          val d = expectMsgType[GeneratorResponse]
          d.data shouldEqual List(820L)
        }
      }

      describe("when given params with a known system and field and start the transformer twice") {
        it("should return id 1 and 2") {
          val actor = TestActorRef(IDGenerator.props)
          actor ! PrepareToGenerate
          expectMsg(ReadyToGenerate)
          val params = List(("field", "DrupalUser"), ("type", "long"))
          actor ! StartGenerator(params)
          val d = expectMsgType[GeneratorResponse]
          d.data shouldEqual List(1L)
          actor ! StartGenerator(params)
          val e = expectMsgType[GeneratorResponse]
          e.data shouldEqual List(2L)
        }
      }

      describe("when given a start value") {
        it("should be returned the start value") {
          val actor = TestActorRef(IDGenerator.props)
          actor ! PrepareToGenerate
          expectMsg(ReadyToGenerate)
          val params = List(("start", "42"), ("field", "Test"))
          actor ! StartGenerator(params)
          val d = expectMsgType[GeneratorResponse]
          d.data shouldEqual List(42L)
        }
      }

      describe("when given parameters without a field name") {
        it("should throw an exception") {
          val actor = TestActorRef(IDGenerator.props)
          actor ! PrepareToGenerate
          expectMsg(ReadyToGenerate)
          val params = List(("type", "long"), ("start", "41"))
          EventFilter[IllegalArgumentException](message = "Parameter 'field' missing!",
                                                source = actor.path.toString,
                                                occurrences = 1) intercept {
            actor ! StartGenerator(params)
          }
        }
      }
    }
  }
}
