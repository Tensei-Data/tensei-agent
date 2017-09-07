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

package com.wegtam.tensei.agent.parsers

import java.net.URI

import akka.actor.Terminated
import akka.testkit.{ TestActorRef, TestProbe }
import akka.util.ByteString
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.{ ActorSpec, DataTreeDocument, XmlTestHelpers }

class JsonFileParserTest extends ActorSpec with XmlTestHelpers {
  describe("JsonFileParser") {
    val data =
      getClass.getResource("/com/wegtam/tensei/agent/parsers/FileParsers/JSON/example.json").toURI
    val dfasdl = DFASDL(
      "SIMPLE-DFASDL",
      scala.io.Source
        .fromInputStream(
          getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/parsers/FileParsers/JSON/example-dfasdl.xml"
          )
        )
        .mkString
    )
    val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
    val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

    describe("when receiving a stop message") {
      it("should stop itself") {
        val dataTree = TestActorRef(
          DataTreeDocument.props(dfasdl, Option("JsonFileParserTest"), Set.empty[String])
        )
        val parser = TestActorRef(
          JsonFileParser.props(source, cookbook, dataTree, Option("JsonFileParserTest"))
        )

        val p = TestProbe()
        p.watch(parser)
        parser ! BaseParserMessages.Stop
        val t = p.expectMsgType[Terminated]
        t.actor shouldEqual parser
      }
    }

    describe("when initialising") {
      describe("using an invalid source uri") {
        it("should return an error message") {
          val invalidSource = ConnectionInformation(
            new URI("file:///this/path/and/file/does/hopefully/not/exist/test.txt"),
            Option(DFASDLReference(cookbook.id, dfasdl.id))
          )
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("JsonFileParserTest"), Set.empty[String])
          )
          val parser = TestActorRef(
            JsonFileParser.props(invalidSource, cookbook, dataTree, Option("JsonFileParserTest"))
          )

          parser ! BaseParserMessages.SubParserInitialize
          val m = expectMsgType[GlobalMessages.ErrorOccured]
          m.error.message should include("/this/path/and/file/does/hopefully/not/exist/test.txt")
        }
      }

      describe("using a valid source uri") {
        it("should return SubParserInitialized") {
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("JsonFileParserTest"), Set.empty[String])
          )
          val parser = TestActorRef(
            JsonFileParser.props(source, cookbook, dataTree, Option("JsonFileParserTest"))
          )

          parser ! BaseParserMessages.SubParserInitialize
          expectMsg(BaseParserMessages.SubParserInitialized)
        }
      }
    }

    describe("when receiving start message") {
      it("should parse the given data") {
        val dataTree = TestActorRef(
          DataTreeDocument.props(dfasdl, Option("JsonFileParserTest"), Set.empty[String])
        )
        val parser = TestActorRef(
          JsonFileParser.props(source, cookbook, dataTree, Option("JsonFileParserTest"))
        )

        parser ! BaseParserMessages.SubParserInitialize
        expectMsg(BaseParserMessages.SubParserInitialized)

        parser ! BaseParserMessages.Start
        val response = expectMsgType[ParserStatusMessage]
        response.status should be(ParserStatus.COMPLETED)

        dataTree ! DataTreeDocumentMessages.ReturnData("house-street")
        val d1 = expectMsgType[DataTreeNodeMessages.Content]
        d1.data.size should be(1)
        d1.data.head.data should be(ByteString("Musterstreet"))

        dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
          ElementReference(dfasdl.id, "house-size-seq")
        )
        val c1 = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
        c1.ref.elementId should be("house-size-seq")
        c1.rows should be(Option(3))

        dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
          ElementReference(dfasdl.id, "persons-seq")
        )
        val c2 = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
        c2.ref.elementId should be("persons-seq")
        c2.rows should be(Option(2))

        dataTree ! DataTreeDocumentMessages.ReturnData("persons-seq-row-firstname", Option(0))
        val d2 = expectMsgType[DataTreeNodeMessages.Content]
        d2.data.size should be(1)
        d2.data.head.data should be(ByteString("Max"))

        dataTree ! DataTreeDocumentMessages.ReturnData("persons-seq-row-firstname", Option(1))
        val d3 = expectMsgType[DataTreeNodeMessages.Content]
        d3.data.size should be(1)
        d3.data.head.data should be(ByteString("Eva"))

        dataTree ! DataTreeDocumentMessages.ReturnData("persons-seq-row-apartment", Option(1))
        val d4 = expectMsgType[DataTreeNodeMessages.Content]
        d4.data.size should be(1)
        d4.data.head.data shouldBe a[java.lang.Long]
        d4.data.head.data should be(4L)
      }
    }
  }
}
