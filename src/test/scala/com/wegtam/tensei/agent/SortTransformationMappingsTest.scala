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

import java.io.InputStream

import akka.testkit.TestActorRef
import argonaut._
import com.wegtam.tensei.adt.AgentStartTransformationMessage
import com.wegtam.tensei.agent.SortTransformationMappings.SortTransformationMappingsMessages

import scalaz._

class SortTransformationMappingsTest extends ActorSpec {
  describe("SortTransformationMappings") {
    describe("when receiving a SortProcessingMessage") {
      describe("with only one recipe") {
        it("should return a SortProcessingMessageResponse containing a sorted recipe") {
          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/AgentStartTransformationMessage01.json"
          )
          val json = scala.io.Source.fromInputStream(in).mkString

          Parse.decodeEither[AgentStartTransformationMessage](json) match {
            case -\/(failure) => fail(failure)
            case \/-(message) =>
              val cookbook = message.cookbook

              val sorter = TestActorRef(SortTransformationMappings.props)
              sorter ! SortTransformationMappingsMessages.SortMappings(cookbook)

              val inSorted: InputStream = getClass.getResourceAsStream(
                "/com/wegtam/tensei/agent/AgentStartTransformationMessage01-sorted.json"
              )
              val jsonSorted = scala.io.Source.fromInputStream(inSorted).mkString

              Parse.decodeEither[AgentStartTransformationMessage](jsonSorted) match {
                case -\/(failure) => fail(failure)
                case \/-(expectedMessage) =>
                  val expectedCookbook = expectedMessage.cookbook

                  val expected =
                    SortTransformationMappingsMessages.SortedMappings(expectedCookbook)
                  val response = expectMsg(expected)
                  response.cookbook should be(expectedCookbook)
              }
          }
        }
      }

      describe("with more than one recipe") {
        it("should return a SortProcessingMessageResponse containing a sorted list of recipes") {
          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/AgentStartTransformationMessage02.json"
          )
          val json = scala.io.Source.fromInputStream(in).mkString

          Parse.decodeEither[AgentStartTransformationMessage](json) match {
            case -\/(failure) => fail(failure)
            case \/-(message) =>
              val cookbook = message.cookbook

              val inSorted: InputStream = getClass.getResourceAsStream(
                "/com/wegtam/tensei/agent/AgentStartTransformationMessage02-sorted.json"
              )
              val jsonSorted = scala.io.Source.fromInputStream(inSorted).mkString

              Parse.decodeEither[AgentStartTransformationMessage](jsonSorted) match {
                case -\/(failure) => fail(failure)
                case \/-(expectedMessage) =>
                  val expectedCookbook = expectedMessage.cookbook

                  val sorter = TestActorRef(SortTransformationMappings.props)
                  sorter ! SortTransformationMappingsMessages.SortMappings(cookbook)

                  val response = expectMsgType[SortTransformationMappingsMessages.SortedMappings]

                  response.cookbook should be(expectedCookbook)
              }
          }
        }
      }
    }
  }
}
