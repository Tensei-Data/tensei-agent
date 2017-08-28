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

import akka.testkit.{ EventFilter, TestActorRef }
import akka.util.ByteString
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.{ ActorSpec, DataTreeDocument, XmlTestHelpers }

class FileParserTest extends ActorSpec with XmlTestHelpers {
  describe("FileParser") {
    describe("using a simple csv file") {
      val data =
        getClass.getResource("/com/wegtam/tensei/agent/parsers/simple-dfasdl-data.csv").toURI
      val dfasdl = DFASDL(
        "SIMPLE-DFASDL",
        scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/simple-dfasdl.xml")
          )
          .mkString
      )
      val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
      val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

      it("should initialize itself upon request") {
        val dataTree =
          TestActorRef(DataTreeDocument.props(dfasdl, Option("FileParserTest"), Set.empty[String]))
        val fileParser =
          TestActorRef(FileParser.props(source, cookbook, dataTree, Option("FileParserTest")))

        fileParser ! BaseParserMessages.SubParserInitialize

        expectMsg(BaseParserMessages.SubParserInitialized)
      }

      it("should stop itself upon request") {
        val dataTree =
          TestActorRef(DataTreeDocument.props(dfasdl, Option("FileParserTest"), Set.empty[String]))
        val fileParser =
          TestActorRef(FileParser.props(source, cookbook, dataTree, Option("FileParserTest")))

        EventFilter.debug(message = "stopped", occurrences = 1) intercept {
          fileParser ! BaseParserMessages.Stop
        }
      }

      it("should parse upon request") {
        val dataTree =
          TestActorRef(DataTreeDocument.props(dfasdl, Option("FileParserTest"), Set.empty[String]))
        val fileParser =
          TestActorRef(FileParser.props(source, cookbook, dataTree, Option("FileParserTest")))

        fileParser ! BaseParserMessages.Start
        val response = expectMsgType[ParserStatusMessage]
        response.status should be(ParserStatus.COMPLETED)

        dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
          ElementReference(dfasdl.id, "rows")
        )
        val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
        dataRows.rows.getOrElse(0L) should be(5L)

        dataTree ! DataTreeDocumentMessages.ReturnData("header")
        val h1 = expectMsgType[DataTreeNodeMessages.Content]
        h1.data.size should be(1)
        h1.data.head.data should be(ByteString("firstname,lastname,email,birthday"))

        dataTree ! DataTreeDocumentMessages.ReturnData("firstname")
        val column1 = expectMsgType[DataTreeNodeMessages.Content]
        column1.data.size should be(1)
        column1.data.head.data should be(ByteString("Albert"))

        dataTree ! DataTreeDocumentMessages.ReturnData("lastname", Option(4L))
        val cell = expectMsgType[DataTreeNodeMessages.Content]
        cell.data.size should be(1)
        cell.data.head.data should be(ByteString("Leibnitz"))
      }
    }

    describe("using a csv file containing incorrect datetime values") {
      it("should parse the data properly") {
        val data = getClass
          .getResource(
            "/com/wegtam/tensei/agent/parsers/simple-dfasdl-data-datetime-incorrect.csv"
          )
          .toURI
        val dfasdl = DFASDL(
          "SIMPLE-DFASDL",
          scala.io.Source
            .fromInputStream(
              getClass.getResourceAsStream(
                "/com/wegtam/tensei/agent/parsers/simple-dfasdl-datetime-incorrect.xml"
              )
            )
            .mkString
        )
        val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
        val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

        val dataTree =
          TestActorRef(DataTreeDocument.props(dfasdl, Option("FileParserTest"), Set.empty[String]))
        val fileParser =
          TestActorRef(FileParser.props(source, cookbook, dataTree, Option("FileParserTest")))

        fileParser ! BaseParserMessages.Start
        val response = expectMsgType[ParserStatusMessage]
        response.status should be(ParserStatus.COMPLETED)

        dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
          ElementReference(dfasdl.id, "rows")
        )
        val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
        dataRows.rows.getOrElse(0L) should be(5L)

        dataTree ! DataTreeDocumentMessages.ReturnData("header")
        val h1 = expectMsgType[DataTreeNodeMessages.Content]
        h1.data.size should be(1)
        h1.data.head.data should be(ByteString("firstname,lastname,email,birthday"))

        dataTree ! DataTreeDocumentMessages.ReturnData("firstname")
        val column1 = expectMsgType[DataTreeNodeMessages.Content]
        column1.data.size should be(1)
        column1.data.head.data should be(ByteString("Albert"))

        dataTree ! DataTreeDocumentMessages.ReturnData("birthday")
        val birthday1 = expectMsgType[DataTreeNodeMessages.Content]
        birthday1.data.size should be(1)
        birthday1.data.head.data should be(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.0"))

        dataTree ! DataTreeDocumentMessages.ReturnData("lastname", Option(4L))
        val cell = expectMsgType[DataTreeNodeMessages.Content]
        cell.data.size should be(1)
        cell.data.head.data should be(ByteString("Leibnitz"))

        dataTree ! DataTreeDocumentMessages.ReturnData("birthday", Option(4L))
        val birthday2 = expectMsgType[DataTreeNodeMessages.Content]
        birthday2.data.size should be(1)
        birthday2.data.head.data should be(java.sql.Timestamp.valueOf("1646-07-01 15:15:15.0"))
      }
    }

    describe("using a csv file containing incorrect date values") {
      it("should parse the data properly") {
        val data = getClass
          .getResource("/com/wegtam/tensei/agent/parsers/simple-dfasdl-data-date-incorrect.csv")
          .toURI
        val dfasdl = DFASDL(
          "SIMPLE-DFASDL",
          scala.io.Source
            .fromInputStream(
              getClass.getResourceAsStream(
                "/com/wegtam/tensei/agent/parsers/simple-dfasdl-date-incorrect.xml"
              )
            )
            .mkString
        )
        val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
        val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

        val dataTree =
          TestActorRef(DataTreeDocument.props(dfasdl, Option("FileParserTest"), Set.empty[String]))
        val fileParser =
          TestActorRef(FileParser.props(source, cookbook, dataTree, Option("FileParserTest")))

        fileParser ! BaseParserMessages.Start
        val response = expectMsgType[ParserStatusMessage]
        response.status should be(ParserStatus.COMPLETED)

        dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
          ElementReference(dfasdl.id, "rows")
        )
        val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
        dataRows.rows.getOrElse(0L) should be(5L)

        dataTree ! DataTreeDocumentMessages.ReturnData("header")
        val h1 = expectMsgType[DataTreeNodeMessages.Content]
        h1.data.size should be(1)
        h1.data.head.data should be(ByteString("firstname,lastname,email,birthday"))

        dataTree ! DataTreeDocumentMessages.ReturnData("firstname")
        val column1 = expectMsgType[DataTreeNodeMessages.Content]
        column1.data.size should be(1)
        column1.data.head.data should be(ByteString("Albert"))

        dataTree ! DataTreeDocumentMessages.ReturnData("birthday")
        val birthday1 = expectMsgType[DataTreeNodeMessages.Content]
        birthday1.data.size should be(1)
        birthday1.data.head.data should be(java.sql.Date.valueOf("1970-01-01"))

        dataTree ! DataTreeDocumentMessages.ReturnData("lastname", Option(4L))
        val cell = expectMsgType[DataTreeNodeMessages.Content]
        cell.data.size should be(1)
        cell.data.head.data should be(ByteString("Leibnitz"))

        dataTree ! DataTreeDocumentMessages.ReturnData("birthday", Option(4L))
        val birthday2 = expectMsgType[DataTreeNodeMessages.Content]
        birthday2.data.size should be(1)
        birthday2.data.head.data should be(java.sql.Date.valueOf("1646-07-01"))
      }
    }

    describe("using a csv containing multiple sequences") {
      describe("with the second sequence being empty") {
        it("should parse the data properly") {
          val data =
            getClass.getResource("/com/wegtam/tensei/agent/parsers/multiple-seqs-data.csv").toURI
          val dfasdl = DFASDL("SIMPLE-DFASDL",
                              scala.io.Source
                                .fromInputStream(
                                  getClass.getResourceAsStream(
                                    "/com/wegtam/tensei/agent/parsers/multiple-seqs-dfasdl.xml"
                                  )
                                )
                                .mkString)
          val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
          val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("FileParserTest"), Set.empty[String])
          )
          val fileParser =
            TestActorRef(FileParser.props(source, cookbook, dataTree, Option("FileParserTest")))

          fileParser ! BaseParserMessages.Start
          val response = expectMsgType[ParserStatusMessage]
          response.status should be(ParserStatus.COMPLETED)

          dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
            ElementReference(dfasdl.id, "rows1")
          )
          val dataRows1 = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
          dataRows1.rows.getOrElse(0L) should be(2L)

          dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
            ElementReference(dfasdl.id, "rows2")
          )
          val dataRows2 = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
          dataRows2.rows.getOrElse(0L) should be(1L) // FIXME This should be 0!

          dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
            ElementReference(dfasdl.id, "rows3")
          )
          val dataRows3 = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
          dataRows3.rows.getOrElse(0L) should be(3L)
        }
      }
    }
  }
}
