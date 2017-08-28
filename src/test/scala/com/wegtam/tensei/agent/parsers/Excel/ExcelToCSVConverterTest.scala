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

package com.wegtam.tensei.agent.parsers.Excel

import java.io.FileNotFoundException
import java.net.URI

import akka.testkit.TestActorRef
import akka.util.ByteString
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.exceptions.AccessValidationException
import com.wegtam.tensei.agent.helpers.ExcelToCSVConverter
import com.wegtam.tensei.agent.helpers.ExcelToCSVConverter.ExcelConverterMessages.{
  Convert,
  ConvertResult
}
import com.wegtam.tensei.agent.parsers.{ BaseParserMessages, FileParser }
import com.wegtam.tensei.agent.{ ActorSpec, DataTreeDocument, XmlTestHelpers }

class ExcelToCSVConverterTest extends ActorSpec with XmlTestHelpers {

  describe("ExcelToCSVConverterTest") {

    val locale = "de_DE"

    describe("when getting invalid files") {
      describe("that does not exist") {
        it("should return an access error") {
          val fileUri = new URI("/com/wegtam/tensei/agent/parsers/Excel/file-not-exist.xls")
          val dfasdl = DFASDL(
            "SIMPLE-DFASDL",
            scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/Excel/empty.xml")
              )
              .mkString
          )
          val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
          val source = ConnectionInformation(uri = fileUri,
                                             dfasdlRef =
                                               Option(DFASDLReference(cookbook.id, dfasdl.id)),
                                             languageTag = Option(locale))

          // Convert
          val converter = TestActorRef(ExcelToCSVConverter.props(source, None))
          converter ! Convert
          expectMsgType[FileNotFoundException]
        }
      }

      describe("that has an invalid file extension") {
        it("should return a type error") {
          val fileUri =
            getClass.getResource("/com/wegtam/tensei/agent/parsers/Excel/file-not-excel.txt").toURI
          val dfasdl = DFASDL(
            "SIMPLE-DFASDL",
            scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/Excel/empty.xml")
              )
              .mkString
          )
          val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
          val source = ConnectionInformation(uri = fileUri,
                                             dfasdlRef =
                                               Option(DFASDLReference(cookbook.id, dfasdl.id)),
                                             languageTag = Option(locale))

          // Convert
          val converter = TestActorRef(ExcelToCSVConverter.props(source, None))
          converter ! Convert
          expectMsgType[AccessValidationException]
        }
      }

      describe("that has no file extension") {
        it("should return a type error") {
          val fileUri = new URI("/com/wegtam/tensei/agent/parsers/Excel/file-without-extension")
          val dfasdl = DFASDL(
            "SIMPLE-DFASDL",
            scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/Excel/empty.xml")
              )
              .mkString
          )
          val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
          val source = ConnectionInformation(uri = fileUri,
                                             dfasdlRef =
                                               Option(DFASDLReference(cookbook.id, dfasdl.id)),
                                             languageTag = Option(locale))

          // Convert
          val converter = TestActorRef(ExcelToCSVConverter.props(source, None))
          converter ! Convert
          expectMsgType[AccessValidationException]
        }
      }
    }

    describe("when getting a .xls file") {
      describe("that is empty") {
        it("should create an empty csv file") {
          val fileUri =
            getClass.getResource("/com/wegtam/tensei/agent/parsers/Excel/empty.xls").toURI
          val dfasdl = DFASDL(
            "SIMPLE-DFASDL",
            scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/Excel/empty.xml")
              )
              .mkString
          )
          val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
          val source = ConnectionInformation(uri = fileUri,
                                             dfasdlRef =
                                               Option(DFASDLReference(cookbook.id, dfasdl.id)),
                                             languageTag = Option(locale))

          // Convert
          val converter = TestActorRef(ExcelToCSVConverter.props(source, None))
          converter ! Convert
          val response = expectMsgType[ConvertResult]

          // Compare
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("FileParserTest"), Set.empty[String])
          )
          val fileParser = TestActorRef(
            FileParser.props(response.source, cookbook, dataTree, Option("FileParserTest"))
          )

          fileParser ! BaseParserMessages.Start
          val fileContent = expectMsgType[ParserStatusMessage]
          fileContent.status should be(ParserStatus.COMPLETED)

          dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
            ElementReference(dfasdl.id, "rows")
          )
          val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
          dataRows.rows.getOrElse(0L) should be(0L)
        }
      }

      describe("that contains diverse formats") {
        it("should create a corresponding csv file") {
          val fileUri =
            getClass.getResource("/com/wegtam/tensei/agent/parsers/Excel/divers.xls").toURI
          val dfasdl = DFASDL(
            "SIMPLE-DFASDL",
            scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/Excel/divers.xml")
              )
              .mkString
          )
          val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
          val source = ConnectionInformation(uri = fileUri,
                                             dfasdlRef =
                                               Option(DFASDLReference(cookbook.id, dfasdl.id)),
                                             languageTag = Option(locale))

          // Convert
          val converter = TestActorRef(ExcelToCSVConverter.props(source, None))
          converter ! Convert
          val response = expectMsgType[ConvertResult]

          // Compare
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("FileParserTest"), Set.empty[String])
          )
          val fileParser = TestActorRef(
            FileParser.props(response.source, cookbook, dataTree, Option("FileParserTest"))
          )

          fileParser ! BaseParserMessages.Start
          val fileContent = expectMsgType[ParserStatusMessage]
          fileContent.status should be(ParserStatus.COMPLETED)

          dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
            ElementReference(dfasdl.id, "products")
          )
          val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
          dataRows.rows.getOrElse(0L) should be(22L)

          dataTree ! DataTreeDocumentMessages.ReturnData("vorname")
          val column11 = expectMsgType[DataTreeNodeMessages.Content]
          column11.data.size should be(1)
          column11.data.head.data should be(ByteString("Hans"))

          dataTree ! DataTreeDocumentMessages.ReturnData("vorname", Option(1L))
          val column12 = expectMsgType[DataTreeNodeMessages.Content]
          column12.data.size should be(1)
          column12.data.head.data should be(ByteString("Dieter"))

          dataTree ! DataTreeDocumentMessages.ReturnData("vorname", Option(3L))
          val column13 = expectMsgType[DataTreeNodeMessages.Content]
          column13.data.size should be(1)
          column13.data.head.data should be(ByteString("Klaus-Werner"))

          dataTree ! DataTreeDocumentMessages.ReturnData("vorname", Option(21L))
          val column14 = expectMsgType[DataTreeNodeMessages.Content]
          column14.data.size should be(1)
          column14.data.head.data should be(ByteString("Mike Hannes"))

          dataTree ! DataTreeDocumentMessages.ReturnData("us")
          val column21 = expectMsgType[DataTreeNodeMessages.Content]
          column21.data.size should be(1)
          BigDecimal(column21.data.head.data.toString) should be(BigDecimal(100.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("us", Option(1L))
          val column22 = expectMsgType[DataTreeNodeMessages.Content]
          column22.data.size should be(1)
          BigDecimal(column22.data.head.data.toString) should be(BigDecimal(1000.01))

          dataTree ! DataTreeDocumentMessages.ReturnData("us", Option(3L))
          val column23 = expectMsgType[DataTreeNodeMessages.Content]
          column23.data.size should be(1)
          BigDecimal(column23.data.head.data.toString) should be(BigDecimal(100.03))

          dataTree ! DataTreeDocumentMessages.ReturnData("us", Option(21L))
          val column24 = expectMsgType[DataTreeNodeMessages.Content]
          column24.data.size should be(1)
          BigDecimal(column24.data.head.data.toString) should be(BigDecimal(100.22))

          dataTree ! DataTreeDocumentMessages.ReturnData("adresse")
          val column31 = expectMsgType[DataTreeNodeMessages.Content]
          column31.data.size should be(1)
          column31.data.head.data should be(ByteString("Paulstr. 2"))

          dataTree ! DataTreeDocumentMessages.ReturnData("adresse", Option(1L))
          val column32 = expectMsgType[DataTreeNodeMessages.Content]
          column32.data.size should be(1)
          column32.data.head.data should be(ByteString("Augusten Straße 1"))

          dataTree ! DataTreeDocumentMessages.ReturnData("adresse", Option(3L))
          val column33 = expectMsgType[DataTreeNodeMessages.Content]
          column33.data.size should be(1)
          column33.data.head.data should be(ByteString("Rostock; Körpeliner Str. 34 c"))

          dataTree ! DataTreeDocumentMessages.ReturnData("plz")
          val column41 = expectMsgType[DataTreeNodeMessages.Content]
          column41.data.size should be(1)
          column41.data.head.data should be(ByteString("18055"))

          dataTree ! DataTreeDocumentMessages.ReturnData("plz", Option(1L))
          val column42 = expectMsgType[DataTreeNodeMessages.Content]
          column42.data.size should be(1)
          column42.data.head.data should be(ByteString("D-18055"))

          dataTree ! DataTreeDocumentMessages.ReturnData("plz", Option(3L))
          val column43 = expectMsgType[DataTreeNodeMessages.Content]
          column43.data.size should be(1)
          column43.data.head.data should be(ByteString("(F) 18055"))

          dataTree ! DataTreeDocumentMessages.ReturnData("datum")
          val column51 = expectMsgType[DataTreeNodeMessages.Content]
          column51.data.size should be(1)
          column51.data.head.data should be(java.sql.Date.valueOf("2017-01-01"))

          dataTree ! DataTreeDocumentMessages.ReturnData("datum", Option(1L))
          val column52 = expectMsgType[DataTreeNodeMessages.Content]
          column52.data.size should be(1)
          column52.data.head.data should be(java.sql.Date.valueOf("2017-02-01"))

          dataTree ! DataTreeDocumentMessages.ReturnData("datum", Option(2L))
          val column53 = expectMsgType[DataTreeNodeMessages.Content]
          column53.data.size should be(1)
          column53.data.head.data should be(java.sql.Date.valueOf("1999-03-31"))

          dataTree ! DataTreeDocumentMessages.ReturnData("preis")
          val column61 = expectMsgType[DataTreeNodeMessages.Content]
          column61.data.size should be(1)
          BigDecimal(column61.data.head.data.toString) should be(BigDecimal(8.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("preis", Option(1L))
          val column62 = expectMsgType[DataTreeNodeMessages.Content]
          column62.data.size should be(1)
          BigDecimal(column62.data.head.data.toString) should be(BigDecimal(12.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("preis", Option(2L))
          val column63 = expectMsgType[DataTreeNodeMessages.Content]
          column63.data.size should be(1)
          BigDecimal(column63.data.head.data.toString) should be(BigDecimal(-24.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("preis", Option(21L))
          val column64 = expectMsgType[DataTreeNodeMessages.Content]
          column64.data.size should be(1)
          BigDecimal(column64.data.head.data.toString) should be(BigDecimal(50000.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("prozent")
          val column71 = expectMsgType[DataTreeNodeMessages.Content]
          column71.data.size should be(1)
          BigDecimal(column71.data.head.data.toString) should be(BigDecimal(0.50))

          dataTree ! DataTreeDocumentMessages.ReturnData("prozent", Option(1L))
          val column72 = expectMsgType[DataTreeNodeMessages.Content]
          column72.data.size should be(1)
          BigDecimal(column72.data.head.data.toString) should be(BigDecimal(10.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("prozent", Option(2L))
          val column73 = expectMsgType[DataTreeNodeMessages.Content]
          column73.data.size should be(1)
          BigDecimal(column73.data.head.data.toString) should be(BigDecimal(50.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("prozent", Option(21L))
          val column74 = expectMsgType[DataTreeNodeMessages.Content]
          column74.data.size should be(1)
          BigDecimal(column74.data.head.data.toString) should be(BigDecimal(1000.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("bemerkung")
          val column81 = expectMsgType[DataTreeNodeMessages.Content]
          column81.data.size should be(1)
          column81.data.head.data should be(ByteString("Laberraber"))

          dataTree ! DataTreeDocumentMessages.ReturnData("bemerkung", Option(1L))
          val column82 = expectMsgType[DataTreeNodeMessages.Content]
          column82.data.size should be(1)
          column82.data.head.data should be(ByteString("Nörgel Mörgel"))

          dataTree ! DataTreeDocumentMessages.ReturnData("bemerkung", Option(3L))
          val column83 = expectMsgType[DataTreeNodeMessages.Content]
          column83.data.size should be(1)
          column83.data.head.data should be(ByteString("Nörgel_Mörgel."))

          dataTree ! DataTreeDocumentMessages.ReturnData("uhrzeit")
          val column91 = expectMsgType[DataTreeNodeMessages.Content]
          column91.data.size should be(1)
          column91.data.head.data should be(java.sql.Time.valueOf("1:00:00"))

          dataTree ! DataTreeDocumentMessages.ReturnData("uhrzeit", Option(1L))
          val column92 = expectMsgType[DataTreeNodeMessages.Content]
          column92.data.size should be(1)
          column92.data.head.data should be(java.sql.Time.valueOf("12:02:00"))

          dataTree ! DataTreeDocumentMessages.ReturnData("uhrzeit", Option(3L))
          val column93 = expectMsgType[DataTreeNodeMessages.Content]
          column93.data.size should be(1)
          column93.data.head.data should be(java.sql.Time.valueOf("13:55:00"))

          dataTree ! DataTreeDocumentMessages.ReturnData("sonderzeichen")
          val column101 = expectMsgType[DataTreeNodeMessages.Content]
          column101.data.size should be(1)
          column101.data.head.data should be(ByteString("1=3% * 1.000"))

          dataTree ! DataTreeDocumentMessages.ReturnData("sonderzeichen", Option(1L))
          val column102 = expectMsgType[DataTreeNodeMessages.Content]
          column102.data.size should be(1)
          column102.data.head.data should be(ByteString("$4 & 7/3"))

          dataTree ! DataTreeDocumentMessages.ReturnData("sonderzeichen", Option(3L))
          val column103 = expectMsgType[DataTreeNodeMessages.Content]
          column103.data.size should be(1)
          column103.data.head.data should be(ByteString("# auf's Größte §4"))
        }
      }
    }

    describe("when getting a .xlsx file") {
      describe("that is empty") {
        it("should create an empty csv file") {
          val fileUri =
            getClass.getResource("/com/wegtam/tensei/agent/parsers/Excel/empty.xlsx").toURI
          val dfasdl = DFASDL(
            "SIMPLE-DFASDL",
            scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/Excel/empty.xml")
              )
              .mkString
          )
          val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
          val source = ConnectionInformation(uri = fileUri,
                                             dfasdlRef =
                                               Option(DFASDLReference(cookbook.id, dfasdl.id)),
                                             languageTag = Option(locale))

          // Convert
          val converter = TestActorRef(ExcelToCSVConverter.props(source, None))
          converter ! Convert
          val response = expectMsgType[ConvertResult]

          // Compare
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("FileParserTest"), Set.empty[String])
          )
          val fileParser = TestActorRef(
            FileParser.props(response.source, cookbook, dataTree, Option("FileParserTest"))
          )

          fileParser ! BaseParserMessages.Start
          val fileContent = expectMsgType[ParserStatusMessage]
          fileContent.status should be(ParserStatus.COMPLETED)

          dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
            ElementReference(dfasdl.id, "rows")
          )
          val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
          dataRows.rows.getOrElse(0L) should be(0L)
        }
      }

      describe("that contains diverse formats") {
        it("should create a corresponding csv file") {
          val fileUri =
            getClass.getResource("/com/wegtam/tensei/agent/parsers/Excel/divers.xlsx").toURI
          val dfasdl = DFASDL(
            "SIMPLE-DFASDL",
            scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/Excel/divers.xml")
              )
              .mkString
          )
          val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
          val source = ConnectionInformation(uri = fileUri,
                                             dfasdlRef =
                                               Option(DFASDLReference(cookbook.id, dfasdl.id)),
                                             languageTag = Option(locale))

          // Convert
          val converter = TestActorRef(ExcelToCSVConverter.props(source, None))
          converter ! Convert
          val response = expectMsgType[ConvertResult]

          // Compare
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("FileParserTest"), Set.empty[String])
          )
          val fileParser = TestActorRef(
            FileParser.props(response.source, cookbook, dataTree, Option("FileParserTest"))
          )

          fileParser ! BaseParserMessages.Start
          val fileContent = expectMsgType[ParserStatusMessage]
          fileContent.status should be(ParserStatus.COMPLETED)

          dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
            ElementReference(dfasdl.id, "products")
          )
          val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
          dataRows.rows.getOrElse(0L) should be(22L)

          dataTree ! DataTreeDocumentMessages.ReturnData("vorname")
          val column11 = expectMsgType[DataTreeNodeMessages.Content]
          column11.data.size should be(1)
          column11.data.head.data should be(ByteString("Hans"))

          dataTree ! DataTreeDocumentMessages.ReturnData("vorname", Option(1L))
          val column12 = expectMsgType[DataTreeNodeMessages.Content]
          column12.data.size should be(1)
          column12.data.head.data should be(ByteString("Dieter"))

          dataTree ! DataTreeDocumentMessages.ReturnData("vorname", Option(3L))
          val column13 = expectMsgType[DataTreeNodeMessages.Content]
          column13.data.size should be(1)
          column13.data.head.data should be(ByteString("Klaus-Werner"))

          dataTree ! DataTreeDocumentMessages.ReturnData("vorname", Option(21L))
          val column14 = expectMsgType[DataTreeNodeMessages.Content]
          column14.data.size should be(1)
          column14.data.head.data should be(ByteString("Mike Hannes"))

          dataTree ! DataTreeDocumentMessages.ReturnData("us")
          val column21 = expectMsgType[DataTreeNodeMessages.Content]
          column21.data.size should be(1)
          BigDecimal(column21.data.head.data.toString) should be(BigDecimal(100.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("us", Option(1L))
          val column22 = expectMsgType[DataTreeNodeMessages.Content]
          column22.data.size should be(1)
          BigDecimal(column22.data.head.data.toString) should be(BigDecimal(1000.01))

          dataTree ! DataTreeDocumentMessages.ReturnData("us", Option(3L))
          val column23 = expectMsgType[DataTreeNodeMessages.Content]
          column23.data.size should be(1)
          BigDecimal(column23.data.head.data.toString) should be(BigDecimal(100.03))

          dataTree ! DataTreeDocumentMessages.ReturnData("us", Option(21L))
          val column24 = expectMsgType[DataTreeNodeMessages.Content]
          column24.data.size should be(1)
          BigDecimal(column24.data.head.data.toString) should be(BigDecimal(100.22))

          dataTree ! DataTreeDocumentMessages.ReturnData("adresse")
          val column31 = expectMsgType[DataTreeNodeMessages.Content]
          column31.data.size should be(1)
          column31.data.head.data should be(ByteString("Paulstr. 2"))

          dataTree ! DataTreeDocumentMessages.ReturnData("adresse", Option(1L))
          val column32 = expectMsgType[DataTreeNodeMessages.Content]
          column32.data.size should be(1)
          column32.data.head.data should be(ByteString("Augusten Straße 1"))

          dataTree ! DataTreeDocumentMessages.ReturnData("adresse", Option(3L))
          val column33 = expectMsgType[DataTreeNodeMessages.Content]
          column33.data.size should be(1)
          column33.data.head.data should be(ByteString("Rostock; Körpeliner Str. 34 c"))

          dataTree ! DataTreeDocumentMessages.ReturnData("plz")
          val column41 = expectMsgType[DataTreeNodeMessages.Content]
          column41.data.size should be(1)
          column41.data.head.data should be(ByteString("18055"))

          dataTree ! DataTreeDocumentMessages.ReturnData("plz", Option(1L))
          val column42 = expectMsgType[DataTreeNodeMessages.Content]
          column42.data.size should be(1)
          column42.data.head.data should be(ByteString("D-18055"))

          dataTree ! DataTreeDocumentMessages.ReturnData("plz", Option(3L))
          val column43 = expectMsgType[DataTreeNodeMessages.Content]
          column43.data.size should be(1)
          column43.data.head.data should be(ByteString("(F) 18055"))

          dataTree ! DataTreeDocumentMessages.ReturnData("datum")
          val column51 = expectMsgType[DataTreeNodeMessages.Content]
          column51.data.size should be(1)
          column51.data.head.data should be(java.sql.Date.valueOf("2017-01-01"))

          dataTree ! DataTreeDocumentMessages.ReturnData("datum", Option(1L))
          val column52 = expectMsgType[DataTreeNodeMessages.Content]
          column52.data.size should be(1)
          column52.data.head.data should be(java.sql.Date.valueOf("2017-02-01"))

          dataTree ! DataTreeDocumentMessages.ReturnData("datum", Option(2L))
          val column53 = expectMsgType[DataTreeNodeMessages.Content]
          column53.data.size should be(1)
          column53.data.head.data should be(java.sql.Date.valueOf("1999-03-31"))

          dataTree ! DataTreeDocumentMessages.ReturnData("preis")
          val column61 = expectMsgType[DataTreeNodeMessages.Content]
          column61.data.size should be(1)
          BigDecimal(column61.data.head.data.toString) should be(BigDecimal(8.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("preis", Option(1L))
          val column62 = expectMsgType[DataTreeNodeMessages.Content]
          column62.data.size should be(1)
          BigDecimal(column62.data.head.data.toString) should be(BigDecimal(12.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("preis", Option(2L))
          val column63 = expectMsgType[DataTreeNodeMessages.Content]
          column63.data.size should be(1)
          BigDecimal(column63.data.head.data.toString) should be(BigDecimal(-24.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("preis", Option(21L))
          val column64 = expectMsgType[DataTreeNodeMessages.Content]
          column64.data.size should be(1)
          BigDecimal(column64.data.head.data.toString) should be(BigDecimal(50000.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("prozent")
          val column71 = expectMsgType[DataTreeNodeMessages.Content]
          column71.data.size should be(1)
          BigDecimal(column71.data.head.data.toString) should be(BigDecimal(0.50))

          dataTree ! DataTreeDocumentMessages.ReturnData("prozent", Option(1L))
          val column72 = expectMsgType[DataTreeNodeMessages.Content]
          column72.data.size should be(1)
          BigDecimal(column72.data.head.data.toString) should be(BigDecimal(10.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("prozent", Option(2L))
          val column73 = expectMsgType[DataTreeNodeMessages.Content]
          column73.data.size should be(1)
          BigDecimal(column73.data.head.data.toString) should be(BigDecimal(50.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("prozent", Option(21L))
          val column74 = expectMsgType[DataTreeNodeMessages.Content]
          column74.data.size should be(1)
          BigDecimal(column74.data.head.data.toString) should be(BigDecimal(1000.00))

          dataTree ! DataTreeDocumentMessages.ReturnData("bemerkung")
          val column81 = expectMsgType[DataTreeNodeMessages.Content]
          column81.data.size should be(1)
          column81.data.head.data should be(ByteString("Laberraber"))

          dataTree ! DataTreeDocumentMessages.ReturnData("bemerkung", Option(1L))
          val column82 = expectMsgType[DataTreeNodeMessages.Content]
          column82.data.size should be(1)
          column82.data.head.data should be(ByteString("Nörgel Mörgel"))

          dataTree ! DataTreeDocumentMessages.ReturnData("bemerkung", Option(3L))
          val column83 = expectMsgType[DataTreeNodeMessages.Content]
          column83.data.size should be(1)
          column83.data.head.data should be(ByteString("Nörgel_Mörgel."))

          dataTree ! DataTreeDocumentMessages.ReturnData("uhrzeit")
          val column91 = expectMsgType[DataTreeNodeMessages.Content]
          column91.data.size should be(1)
          column91.data.head.data should be(java.sql.Time.valueOf("1:00:00"))

          dataTree ! DataTreeDocumentMessages.ReturnData("uhrzeit", Option(1L))
          val column92 = expectMsgType[DataTreeNodeMessages.Content]
          column92.data.size should be(1)
          column92.data.head.data should be(java.sql.Time.valueOf("12:02:00"))

          dataTree ! DataTreeDocumentMessages.ReturnData("uhrzeit", Option(3L))
          val column93 = expectMsgType[DataTreeNodeMessages.Content]
          column93.data.size should be(1)
          column93.data.head.data should be(java.sql.Time.valueOf("13:55:00"))

          dataTree ! DataTreeDocumentMessages.ReturnData("sonderzeichen")
          val column101 = expectMsgType[DataTreeNodeMessages.Content]
          column101.data.size should be(1)
          column101.data.head.data should be(ByteString("1=3% * 1.000"))

          dataTree ! DataTreeDocumentMessages.ReturnData("sonderzeichen", Option(1L))
          val column102 = expectMsgType[DataTreeNodeMessages.Content]
          column102.data.size should be(1)
          column102.data.head.data should be(ByteString("$4 & 7/3"))

          dataTree ! DataTreeDocumentMessages.ReturnData("sonderzeichen", Option(3L))
          val column103 = expectMsgType[DataTreeNodeMessages.Content]
          column103.data.size should be(1)
          column103.data.head.data should be(ByteString("# auf's Größte §4"))
        }
      }
    }
  }
}
