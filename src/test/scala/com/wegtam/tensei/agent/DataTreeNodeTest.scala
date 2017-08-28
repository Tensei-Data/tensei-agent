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

import akka.pattern.ask
import akka.testkit.TestActorRef
import akka.util.Timeout
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages._
import com.wegtam.tensei.agent.adt.ParserDataContainer

import scala.concurrent.Future
import scala.concurrent.duration._

class DataTreeNodeTest extends ActorSpec with XmlTestHelpers {
  val agentRunIdentifier = Option("DataTreeNodeTest")

  describe("DataTreeNode") {
    describe("when initialized with data") {
      describe("when receiving a ReturnContent message") {
        describe("on a simple element") {
          it("should return the encapsulated content") {
            val data = ParserDataContainer("I am a lonely string.", "ID3")
            val node = TestActorRef(DataTreeNode.props(agentRunIdentifier))
            node ! AppendData(data)

            node ! ReturnContent(None)

            val message      = expectMsgType[Content]
            val expectedData = Vector.empty[ParserDataContainer] :+ data
            message.data should be(expectedData)
          }
        }

        describe("on sequence rows") {
          describe("using no parameters") {
            it("should return the rows") {
              val node = TestActorRef(DataTreeNode.props(agentRunIdentifier))
              node ! AppendData(ParserDataContainer("TEST-01", "ID1", None, 0))
              node ! AppendData(ParserDataContainer("TEST-02", "ID1", None, 1))
              node ! AppendData(ParserDataContainer("TEST-03", "ID1", None, 2))
              node ! AppendData(ParserDataContainer("TEST-01a", "ID2", None, 0))
              node ! AppendData(ParserDataContainer("TEST-02a", "ID2", None, 1))
              node ! AppendData(ParserDataContainer("TEST-03a", "ID2", None, 2))

              node ! ReturnContent(None)

              val message = expectMsgType[Content]
              message.data.size should be(6)
            }
          }

          describe("using an element id") {
            it("should return the correct column") {
              val node = TestActorRef(DataTreeNode.props(agentRunIdentifier))
              node ! AppendData(ParserDataContainer("TEST-01", "ID1", None, 0))
              node ! AppendData(ParserDataContainer("TEST-02", "ID1", None, 1))
              node ! AppendData(ParserDataContainer("TEST-03", "ID1", None, 2))
              node ! AppendData(ParserDataContainer("TEST-01a", "ID2", None, 0))
              node ! AppendData(ParserDataContainer("TEST-02a", "ID2", None, 1))
              node ! AppendData(ParserDataContainer("TEST-03a", "ID2", None, 2))

              node ! ReturnContent(None, None, Option("ID2"))

              val message = expectMsgType[Content]
              message.data.size should be(3)
              message.data.head.data should be("TEST-01a")
              message.data.last.data should be("TEST-03a")
            }
          }

          describe("using a sequence row") {
            it("should return the correct row") {
              val node = TestActorRef(DataTreeNode.props(agentRunIdentifier))
              node ! AppendData(ParserDataContainer("TEST-01", "ID1", None, 0))
              node ! AppendData(ParserDataContainer("TEST-02", "ID1", None, 1))
              node ! AppendData(ParserDataContainer("TEST-03", "ID1", None, 2))
              node ! AppendData(ParserDataContainer("TEST-01a", "ID2", None, 0))
              node ! AppendData(ParserDataContainer("TEST-02a", "ID2", None, 1))
              node ! AppendData(ParserDataContainer("TEST-03a", "ID2", None, 2))

              node ! ReturnContent(None, Option(1), None)

              val message = expectMsgType[Content]
              message.data.size should be(2)
              message.data.head.data should be("TEST-02")
              message.data.last.data should be("TEST-02a")
            }
          }

          describe("using an element id and a sequence row") {
            it("should return the correct element") {
              val node = TestActorRef(DataTreeNode.props(agentRunIdentifier))
              node ! AppendData(ParserDataContainer("TEST-01", "ID1", None, 0))
              node ! AppendData(ParserDataContainer("TEST-02", "ID1", None, 1))
              node ! AppendData(ParserDataContainer("TEST-03", "ID1", None, 2))
              node ! AppendData(ParserDataContainer("TEST-01a", "ID2", None, 0))
              node ! AppendData(ParserDataContainer("TEST-02a", "ID2", None, 1))
              node ! AppendData(ParserDataContainer("TEST-03a", "ID2", None, 2))

              node ! ReturnContent(None, Option(1L), Option("ID2"))

              val message = expectMsgType[Content]
              message.data.size should be(1)
              message.data.head.data should be("TEST-02a")
            }
          }
        }
      }

      describe("when asked for the content") {
        it("should return the encapsulated content") {
          val data = ParserDataContainer("I am a lonely string.", "ID3")
          val node = TestActorRef(DataTreeNode.props(agentRunIdentifier))
          node ! AppendData(data)

          implicit val timeout = Timeout(5.seconds)

          val future: Future[Content] = ask(node, ReturnContent(None)).mapTo[Content]

          val result = future.value.get
          result.get shouldBe an[Content]
          val expectedData = Vector.empty[ParserDataContainer] :+ data
          result.get.data should be(expectedData)
        }
      }

      describe("when receiving a HasContent message") {
        describe("that matches") {
          it("should return the content") {
            val data = ParserDataContainer("I am a lonely string.", "ID3")
            val node = TestActorRef(DataTreeNode.props(agentRunIdentifier))
            node ! AppendData(data)

            node ! HasContent(data.elementId, data.data, None)

            val message = expectMsgType[FoundContent]
            message.container should be(data)
          }
        }

        describe("that doesn't match the element id") {
          it("should not return the content") {
            val data = ParserDataContainer("I am a lonely string.", "ID3")
            val node = TestActorRef(DataTreeNode.props(agentRunIdentifier))
            node ! AppendData(data)

            node ! HasContent("Another-ID", data.data, None)

            val thrown = the[java.lang.AssertionError] thrownBy expectMsgType[FoundContent]
            thrown.getMessage should include("timeout")
          }
        }

        describe("that doesn't match the data") {
          it("should not return the content") {
            val data = ParserDataContainer("I am a lonely string.", "ID3")
            val node = TestActorRef(DataTreeNode.props(agentRunIdentifier))
            node ! AppendData(data)

            node ! HasContent(data.elementId, "Another string...", None)

            val thrown = the[java.lang.AssertionError] thrownBy expectMsgType[FoundContent]
            thrown.getMessage should include("timeout")
          }
        }
      }
    }
  }
}
