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

import java.io.StringReader
import javax.xml.xpath.{ XPathConstants, XPathFactory }

import akka.actor.Terminated
import akka.testkit.{ TestActorRef, TestProbe }
import akka.util.ByteString
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.{ DataTreeDocument, XmlActorSpec }
import org.w3c.dom.NodeList
import org.xml.sax.InputSource

class XmlFileParserTest extends XmlActorSpec {
  val agentRunIdentifier = Option("XmlFileParserTest")

  describe("XmlFileParser") {
    it("should initialize itself upon request") {
      val data =
        getClass.getResource("/com/wegtam/tensei/agent/parsers/simple-dfasdl-data.xml").toURI
      val dfasdl = DFASDL(
        "SIMPLE-DFASDL",
        scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/simple-xml-dfasdl.xml")
          )
          .mkString
      )
      val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
      val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

      val dataTree = TestActorRef(
        DataTreeDocument.props(dfasdl, Option("XmlFileParserTest"), Set.empty[String])
      )
      val xmlFileParser =
        TestActorRef(XmlFileParser.props(source, cookbook, dataTree, agentRunIdentifier))

      xmlFileParser ! BaseParserMessages.SubParserInitialize

      expectMsg(BaseParserMessages.SubParserInitialized)
    }

    it("should stop itself upon request") {
      val data =
        getClass.getResource("/com/wegtam/tensei/agent/parsers/simple-dfasdl-data.xml").toURI
      val dfasdl = DFASDL(
        "SIMPLE-DFASDL",
        scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/simple-xml-dfasdl.xml")
          )
          .mkString
      )
      val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
      val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

      val dataTree = TestActorRef(
        DataTreeDocument.props(dfasdl, Option("XmlFileParserTest"), Set.empty[String])
      )
      val xmlFileParser =
        TestActorRef(XmlFileParser.props(source, cookbook, dataTree, agentRunIdentifier))

      val p = TestProbe()
      p.watch(xmlFileParser)
      xmlFileParser ! BaseParserMessages.Stop
      val t = p.expectMsgType[Terminated]
      t.actor shouldEqual xmlFileParser
    }

    it("should parse upon request") {
      val data =
        getClass.getResource("/com/wegtam/tensei/agent/parsers/simple-dfasdl-data.xml").toURI
      val dfasdl = DFASDL(
        "SIMPLE-DFASDL",
        scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/simple-xml-dfasdl.xml")
          )
          .mkString
      )
      val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
      val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

      val dataTree = TestActorRef(
        DataTreeDocument.props(dfasdl, Option("XmlFileParserTest"), Set.empty[String])
      )
      val xmlFileParser =
        TestActorRef(XmlFileParser.props(source, cookbook, dataTree, agentRunIdentifier))

      xmlFileParser ! BaseParserMessages.Start
      val response = expectMsgType[ParserStatusMessage]
      response.status should be(ParserStatus.COMPLETED)

      dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(ElementReference(dfasdl.id, "rows"))
      val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
      dataRows.rows.getOrElse(0L) should be(5L)

      dataTree ! DataTreeDocumentMessages.ReturnData("firstname")
      val column1 = expectMsgType[DataTreeNodeMessages.Content]
      column1.data.size should be(1)
      column1.data.head.data should be(ByteString("Albert"))

      dataTree ! DataTreeDocumentMessages.ReturnData("lastname", Option(4L))
      val cell = expectMsgType[DataTreeNodeMessages.Content]
      cell.data.size should be(1)
      cell.data.head.data should be(ByteString("Leibnitz"))
    }

    it("should parse a simple xml with stacked elements") {
      val data = getClass
        .getResource("/com/wegtam/tensei/agent/parsers/simple-dfasdl-stacked-data.xml")
        .toURI
      val dfasdl = DFASDL(
        "SIMPLE-DFASDL",
        scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/parsers/simple-xml-stacked-dfasdl.xml"
            )
          )
          .mkString
      )
      val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
      val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

      val dataTree = TestActorRef(
        DataTreeDocument.props(dfasdl, Option("XmlFileParserTest"), Set.empty[String])
      )
      val xmlFileParser =
        TestActorRef(XmlFileParser.props(source, cookbook, dataTree, agentRunIdentifier))

      xmlFileParser ! BaseParserMessages.Start
      val response = expectMsgType[ParserStatusMessage]
      response.status should be(ParserStatus.COMPLETED)

      dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(ElementReference(dfasdl.id, "rows"))
      val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
      dataRows.rows.getOrElse(0L) should be(5L)

      dataTree ! DataTreeDocumentMessages.ReturnData("name-first")
      val column1 = expectMsgType[DataTreeNodeMessages.Content]
      column1.data.size should be(1)
      column1.data.head.data should be(ByteString("Albert"))

      dataTree ! DataTreeDocumentMessages.ReturnData("name-last", Option(4L))
      val cell = expectMsgType[DataTreeNodeMessages.Content]
      cell.data.size should be(1)
      cell.data.head.data should be(ByteString("Leibnitz"))
    }

    it("should parse a simple xml with stacked elements including empty ones") {
      val data = getClass
        .getResource(
          "/com/wegtam/tensei/agent/parsers/simple-dfasdl-stacked-data-including-empty.xml"
        )
        .toURI
      val dfasdl = DFASDL(
        "SIMPLE-DFASDL",
        scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/parsers/simple-xml-stacked-dfasdl.xml"
            )
          )
          .mkString
      )
      val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
      val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

      val dataTree = TestActorRef(
        DataTreeDocument.props(dfasdl, Option("XmlFileParserTest"), Set.empty[String])
      )
      val xmlFileParser =
        TestActorRef(XmlFileParser.props(source, cookbook, dataTree, agentRunIdentifier))

      xmlFileParser ! BaseParserMessages.Start
      val response = expectMsgType[ParserStatusMessage]
      response.status should be(ParserStatus.COMPLETED)

      dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(ElementReference(dfasdl.id, "rows"))
      val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
      dataRows.rows.getOrElse(0L) should be(5L)

      val expectedData = Map(
        0L -> Map(
          "name-first" -> ByteString("Albert"),
          "name-last"  -> ByteString("Einstein"),
          "email"      -> ByteString("albert.einstein@example.com"),
          "birthday"   -> java.time.LocalDate.parse("1879-03-14")
        ),
        1L -> Map(
          "name-first" -> None,
          "name-last"  -> None,
          "email"      -> ByteString("br@example.com"),
          "birthday"   -> java.time.LocalDate.parse("1826-09-17")
        ),
        2L -> Map(
          "name-first" -> ByteString("Johann Carl Friedrich"),
          "name-last"  -> None,
          "email"      -> ByteString("gauss@example.com"),
          "birthday"   -> java.time.LocalDate.parse("1777-04-30")
        ),
        3L -> Map(
          "name-first" -> ByteString("Johann Benedict"),
          "name-last"  -> ByteString("Listing"),
          "email"      -> ByteString("bl@example.com"),
          "birthday"   -> None
        ),
        4L -> Map(
          "name-first" -> None,
          "name-last"  -> ByteString("Leibnitz"),
          "email"      -> None,
          "birthday"   -> java.time.LocalDate.parse("1646-07-01")
        )
      )

      withClue("Parsed data incorrect!") {
        expectedData.foreach { e =>
          val row = e._1
          val d   = e._2
          withClue(s"Data for row $row incorrect!") {
            d.foreach { entry =>
              val key   = entry._1
              val value = entry._2

              withClue(s"Cell $row, $key incorrect!") {
                dataTree ! DataTreeDocumentMessages.ReturnData(key, Option(row))
                val cell = expectMsgType[DataTreeNodeMessages.Content]
                cell.data.size should be(1)
                cell.data.head.data should be(value)
              }
            }
          }
        }
      }
    }

    // FIXME There exists a ticket for this and maybe we'll rewrite the xml parser either way.
    ignore("should parse a small complex xml with a choice") {
      val data = getClass
        .getResource("/com/wegtam/tensei/agent/parsers/complex-dfasdl-small-data.xml")
        .toURI
      val dfasdl = DFASDL(
        "SIMPLE-DFASDL",
        scala.io.Source
          .fromInputStream(
            getClass
              .getResourceAsStream("/com/wegtam/tensei/agent/parsers/complex-small-xml-dfasdl.xml")
          )
          .mkString
      )
      val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
      val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

      val dataTree = TestActorRef(
        DataTreeDocument.props(dfasdl, Option("XmlFileParserTest"), Set.empty[String])
      )
      val xmlFileParser =
        TestActorRef(XmlFileParser.props(source, cookbook, dataTree, agentRunIdentifier))

      xmlFileParser ! BaseParserMessages.Start
      val response = expectMsgType[ParserStatusMessage]
      response.status should be(ParserStatus.COMPLETED)

      dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(ElementReference(dfasdl.id, "rows"))
      val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
      dataRows.rows.getOrElse(0L) should be(5L)

      val expectedDataXml = scala.io.Source
        .fromInputStream(
          getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/parsers/complex-small-xml-dfasdl-expected-data.xml"
          )
        )
        .mkString
      val expectedDataTree =
        createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

      compareSequenceData("firstname", expectedDataTree, dataTree)
      compareSequenceData("lastname", expectedDataTree, dataTree)
      compareSequenceData("email", expectedDataTree, dataTree)
      compareSequenceData("birthday", expectedDataTree, dataTree)

      withClue("The number of subsequence elements should be correct!") {
        dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
          ElementReference(dfasdl.id, "awards")
        )
        val awardsCounter = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
        awardsCounter.rows.getOrElse(0L) should be(8L)
      }
      withClue("The data within the subsequence elements should be correct!") {
        List(
          List(("awards", 0L), ("rows", 0L)),
          List(("awards", 1L), ("rows", 0L)),
          List(("awards", 2L), ("rows", 0L)),
          List(("awards", 0L), ("rows", 1L)),
          List(("awards", 0L), ("rows", 3L)),
          List(("awards", 1L), ("rows", 3L)),
          List(("awards", 0L), ("rows", 4L)),
          List(("awards", 1L), ("rows", 4L))
        ).zipWithIndex.foreach { entry =>
          val sequenceData = entry._1
          val idHelper     = entry._2
          val row          = expectedDataTree.getElementById(s"award-row-${idHelper + 1}")
          row should not be null
          val xpath   = XPathFactory.newInstance().newXPath()
          val rowHash = calculateDataElementStorageHash("what-ever", sequenceData)
          withClue(s"Data for $sequenceData should be correct!") {
            if (idHelper < 4) {
              val dataNodes = xpath
                .evaluate(s"""descendant::*""", row, XPathConstants.NODESET)
                .asInstanceOf[NodeList]
              dataTree ! DataTreeDocumentMessages.ReturnHashedData("award-complete-year",
                                                                   rowHash,
                                                                   Option(sequenceData.head._2))
              val column1 = expectMsgType[DataTreeNodeMessages.Content]
              column1.data.size should be > 0
              column1.data.head.data should be(dataNodes.item(0).getTextContent)
            } else if (idHelper >= 4 && idHelper < 6) {} else {}
          }
        }
      }
    }

    it("should parse a small complex xml with attributes") {
      val data = getClass
        .getResource("/com/wegtam/tensei/agent/parsers/complex-dfasdl-small-data-with-attr.xml")
        .toURI
      val dfasdl = DFASDL(
        "SIMPLE-DFASDL",
        scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/parsers/complex-small-xml-dfasdl-with-attr.xml"
            )
          )
          .mkString
      )
      val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
      val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

      val dataTree = TestActorRef(
        DataTreeDocument.props(dfasdl, Option("XmlFileParserTest"), Set.empty[String])
      )
      val xmlFileParser =
        TestActorRef(XmlFileParser.props(source, cookbook, dataTree, agentRunIdentifier))

      xmlFileParser ! BaseParserMessages.Start
      val response = expectMsgType[ParserStatusMessage]
      response.status should be(ParserStatus.COMPLETED)

      dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(ElementReference(dfasdl.id, "rows"))
      val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
      dataRows.rows.getOrElse(0L) should be(5L)

      val expectedDataXml = scala.io.Source
        .fromInputStream(
          getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/parsers/complex-small-xml-dfasdl-expected-data.xml"
          )
        )
        .mkString
      val expectedDataTree =
        createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

      compareSequenceData("firstname", expectedDataTree, dataTree)
      compareSequenceData("lastname", expectedDataTree, dataTree)
      compareSequenceData("email", expectedDataTree, dataTree)
      compareSequenceData("birthday", expectedDataTree, dataTree)

      withClue("The number of subsequence elements should be correct!") {
        dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
          ElementReference(dfasdl.id, "awards")
        )
        val awardsCounter = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
        awardsCounter.rows.getOrElse(0L) should be(6L)
      }

      val sequenceData =
        List(
          List(("awards", 0L), ("rows", 0L)),
          List(("awards", 1L), ("rows", 0L)),
          List(("awards", 2L), ("rows", 0L)),
          List(("awards", 0L), ("rows", 1L)),
          List(("awards", 0L), ("rows", 3L)),
          List(("awards", 0L), ("rows", 4L))
        )

      withClue("The stored data should be correct!") {
        val dataIndex = sequenceData.head(1)._2.toInt

        dataTree ! DataTreeDocumentMessages.ReturnHashedData(
          "year",
          calculateDataElementStorageHash("year", sequenceData.head),
          Option(0L)
        )
        val column1 = expectMsgType[DataTreeNodeMessages.Content]
        column1.data.size should be > 0
        column1.data(dataIndex).data shouldBe a[java.lang.Long]
        column1.data(dataIndex).data should be(1914L)

        dataTree ! DataTreeDocumentMessages.ReturnHashedData(
          "name",
          calculateDataElementStorageHash("name", sequenceData.head),
          Option(0L)
        )
        val column2 = expectMsgType[DataTreeNodeMessages.Content]
        column2.data.size should be > 0
        column2.data(dataIndex).data should be(
          ByteString("Ordentliches Mitglied der Preußischen Akademie der Wissenschaften")
        )
      }

      withClue("The stored data should be correct!") {
        val dataIndex = sequenceData(1)(1)._2.toInt

        dataTree ! DataTreeDocumentMessages.ReturnHashedData(
          "year",
          calculateDataElementStorageHash("year", sequenceData(1)),
          Option(1L)
        )
        val column1 = expectMsgType[DataTreeNodeMessages.Content]
        column1.data.size should be > 0
        column1.data(dataIndex).data should be(1917)

        dataTree ! DataTreeDocumentMessages.ReturnHashedData(
          "name",
          calculateDataElementStorageHash("name", sequenceData(1)),
          Option(1L)
        )
        val column2 = expectMsgType[DataTreeNodeMessages.Content]
        column2.data.size should be > 0
        column2.data(dataIndex).data should be(
          ByteString("Ehrenpreis der Peter-Wilhelm-Müller-Stiftung")
        )
      }

      withClue("The stored data should be correct!") {
        val dataIndex = sequenceData(2)(1)._2.toInt

        dataTree ! DataTreeDocumentMessages.ReturnHashedData(
          "year",
          calculateDataElementStorageHash("year", sequenceData(2)),
          Option(2L)
        )
        val column1 = expectMsgType[DataTreeNodeMessages.Content]
        column1.data.size should be > 0
        column1.data(dataIndex).data should be(1919)

        dataTree ! DataTreeDocumentMessages.ReturnHashedData(
          "name",
          calculateDataElementStorageHash("name", sequenceData(2)),
          Option(2L)
        )
        val column2 = expectMsgType[DataTreeNodeMessages.Content]
        column2.data.size should be > 0
        column2.data(dataIndex).data should be(
          ByteString("Ehrendoktorwürde (Dr. h.c.) der Universität Rostock")
        )
      }

      withClue("The stored data should be correct!") {
        val dataIndex = sequenceData(3)(1)._2.toInt

        dataTree ! DataTreeDocumentMessages.ReturnHashedData(
          "year",
          calculateDataElementStorageHash("year", sequenceData(3)),
          Option(0L)
        )
        val column1 = expectMsgType[DataTreeNodeMessages.Content]
        column1.data.size should be > 0
        column1.data(dataIndex).data should be(1868)

        dataTree ! DataTreeDocumentMessages.ReturnHashedData(
          "name",
          calculateDataElementStorageHash("name", sequenceData(3)),
          Option(0L)
        )
        val column2 = expectMsgType[DataTreeNodeMessages.Content]
        column2.data.size should be > 0
        column2.data(dataIndex).data should be(ByteString("Riemann-Helmholtz-Raumproblem"))
      }

      withClue("The stored data should be correct!") {
        val h1 = calculateDataElementStorageHash("year", sequenceData(4))
        dataTree ! DataTreeDocumentMessages.ReturnHashedData("year", h1, Option(0L))
        val column1 = expectMsgType[DataTreeNodeMessages.Content]
        column1.data.size should be > 0
        column1.data
          .find(_.dataElementHash.getOrElse(0L) == h1)
          .fold(fail(s"Data element with hash $h1 not found in $column1!"))(
            c => c.data should be(1861)
          )

        val h2 = calculateDataElementStorageHash("name", sequenceData(4))
        dataTree ! DataTreeDocumentMessages.ReturnHashedData("name", h2, Option(0L))
        val column2 = expectMsgType[DataTreeNodeMessages.Content]
        column2.data.size should be > 0
        column2.data
          .find(_.dataElementHash.getOrElse(0L) == h2)
          .fold(fail(s"Data element with hash $h2 not found in $column2!"))(
            c => c.data should be(ByteString("Mitglied Akademie der Wissenschaften in Göttingen"))
          )
      }

      withClue("The stored data should be correct!") {
        dataTree ! DataTreeDocumentMessages.ReturnHashedData(
          "year",
          calculateDataElementStorageHash("year", sequenceData(5)),
          Option(0L)
        )
        val column1 = expectMsgType[DataTreeNodeMessages.Content]
        column1.data.size should be > 0
        column1.data
          .find(
            _.dataElementHash.getOrElse("") == calculateDataElementStorageHash("year",
                                                                               sequenceData(5))
          )
          .get
          .data should be(2008)

        dataTree ! DataTreeDocumentMessages.ReturnHashedData(
          "name",
          calculateDataElementStorageHash("name", sequenceData(5)),
          Option(0L)
        )
        val column2 = expectMsgType[DataTreeNodeMessages.Content]
        column2.data.size should be > 0
        column2.data
          .find(
            _.dataElementHash.getOrElse("") == calculateDataElementStorageHash("name",
                                                                               sequenceData(5))
          )
          .get
          .data should be(ByteString("Denkmal in Hannover"))
      }
    }

    it("should parse a medium sized complex xml with attributes") {
      val data =
        getClass.getResource("/com/wegtam/tensei/agent/parsers/complex-dfasdl-02-data.xml").toURI
      val dfasdl = DFASDL(
        "SIMPLE-DFASDL",
        scala.io.Source
          .fromInputStream(
            getClass
              .getResourceAsStream("/com/wegtam/tensei/agent/parsers/complex-xml-01-dfasdl.xml")
          )
          .mkString
      )
      val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
      val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

      val dataTree = TestActorRef(
        DataTreeDocument.props(dfasdl, Option("XmlFileParserTest"), Set.empty[String])
      )
      val xmlFileParser =
        TestActorRef(XmlFileParser.props(source, cookbook, dataTree, agentRunIdentifier))

      xmlFileParser ! BaseParserMessages.Start
      val response = expectMsgType[ParserStatusMessage]
      response.status should be(ParserStatus.COMPLETED)

      val expectedNumberOfRows = 15

      withClue("It should read all entries.") {
        dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
          ElementReference(dfasdl.id, "reservation")
        )
        val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
        dataRows.rows.getOrElse(0L) should be(expectedNumberOfRows)
      }

      withClue("It should read all entries of subsequence 'kinder' correctly!") {
        dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
          ElementReference(dfasdl.id, "kinder")
        )
        val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
        dataRows.rows.getOrElse(0L) should be(1L)
      }

      withClue("It should read all entries of subsequence 'zusaetze' correctly!") {
        dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
          ElementReference(dfasdl.id, "zusaetze")
        )
        val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
        dataRows.rows.getOrElse(0L) should be(47L)
      }

      val expectedCustomerIds = List("34981",
                                     "72749",
                                     "74751",
                                     "74751",
                                     "74751",
                                     "74761",
                                     "74755",
                                     "71919",
                                     "74749",
                                     "72728",
                                     "72749",
                                     "46602",
                                     "76774",
                                     "71951",
                                     "76775")

      expectedCustomerIds.zipWithIndex.foreach { entry =>
        val index              = entry._2
        val expectedCustomerId = entry._1

        withClue(s"Comparing expected entry for `k_id` '$expectedCustomerId' at position $index.") {
          val rowHash = calculateDataElementStorageHash("k_id",
                                                        List(("reservation", 0L),
                                                             ("reservations", index.toLong)))
          dataTree ! DataTreeDocumentMessages.ReturnHashedData("k_id", rowHash, Option(0L))
          val response = expectMsgType[DataTreeNodeMessages.Content]
          withClue("No data was returned!")(response.data.size should be > 0)
          withClue("Data not correct!") {
            val d = response.data.find(_.dataElementHash.getOrElse(0L) == rowHash)
            println(response)
            d.isDefined should be(true)
            d.foreach(c => c.data.toString should be(expectedCustomerId))
          }
        }
      }

      val expectedBuchungsNr = List(
        "4a1012198c",
        "48140349f6",
        "48141052f9",
        "481410532c",
        "481411017f",
        "4814113286",
        "4814113331",
        "4814113344",
        "48141135c3",
        "481411370a",
        "4814113875",
        "481411531f",
        "4814120561",
        "4814121183",
        "481412131e"
      )

      expectedBuchungsNr.zipWithIndex.foreach { entry =>
        val index        = entry._2
        val expectedBuNr = entry._1

        withClue(s"Comparing expected entry for `buchungsnr` '$expectedBuNr' at position $index.") {
          val rowHash = calculateDataElementStorageHash("buchungsnr",
                                                        List(("reservation", 0L),
                                                             ("reservations", index.toLong)))
          dataTree ! DataTreeDocumentMessages.ReturnHashedData("buchungsnr", rowHash, Option(0L))
          val response = expectMsgType[DataTreeNodeMessages.Content]
          withClue("No data was returned!")(response.data.size should be > 0)
          withClue("Data not correct!")(
            response.data.find(_.dataElementHash.getOrElse("") == rowHash).get.data should be(
              ByteString(expectedBuNr)
            )
          )
        }
      }

      withClue("Sample attributes should be correct!") {
        Map(
          "zusatz-name" -> (("Frühstück",
                             List(("zusaetze", 0L), ("reservation", 0L), ("reservations", 0L)))),
          "zusatz-anzahl" -> (("4",
                               List(("zusaetze", 0L), ("reservation", 0L), ("reservations", 0L)))),
          "zusatz-einzelpreis" -> (("0.00",
                                    List(("zusaetze", 0L),
                                         ("reservation", 0L),
                                         ("reservations", 0L)))),
          "zusatz-buchungsnr" -> (("4a1012198c",
                                   List(("zusaetze", 0L),
                                        ("reservation", 0L),
                                        ("reservations", 0L)))),
          "zusatz-provision" -> (("0.00",
                                  List(("zusaetze", 0L),
                                       ("reservation", 0L),
                                       ("reservations", 0L)))),
          "zusatz-provision-split" -> (("0.00",
                                        List(("zusaetze", 0L),
                                             ("reservation", 0L),
                                             ("reservations", 0L)))),
          "zusatz-mwst-satz" -> (("19.00",
                                  List(("zusaetze", 0L),
                                       ("reservation", 0L),
                                       ("reservations", 0L)))),
          "zusatz-kuerzel" -> (("",
                                List(("zusaetze", 0L), ("reservation", 0L), ("reservations", 0L))))
        ).foreach { entry =>
          val rowHash = calculateDataElementStorageHash(entry._1, entry._2._2)
          dataTree ! DataTreeDocumentMessages.ReturnHashedData(entry._1,
                                                               rowHash,
                                                               Option(entry._2._2.head._2))
          val response = expectMsgType[DataTreeNodeMessages.Content]
          withClue("No data was returned!")(response.data.size should be > 0)
          withClue("Data not correct!")(
            response.data.find(_.dataElementHash.getOrElse("") == rowHash).get.data match {
              case bs: ByteString => bs should be(ByteString(entry._2._1))
              case otherData      => otherData.toString should be(entry._2._1)
            }
          )
        }

        Map(
          "zusatz-name" -> (("Parkplatz",
                             List(("zusaetze", 1L), ("reservation", 0L), ("reservations", 0L)))),
          "zusatz-anzahl" -> (("2",
                               List(("zusaetze", 1L), ("reservation", 0L), ("reservations", 0L)))),
          "zusatz-einzelpreis" -> (("0.00",
                                    List(("zusaetze", 1L),
                                         ("reservation", 0L),
                                         ("reservations", 0L)))),
          "zusatz-buchungsnr" -> (("4a1012198c",
                                   List(("zusaetze", 1L),
                                        ("reservation", 0L),
                                        ("reservations", 0L)))),
          "zusatz-provision" -> (("0.00",
                                  List(("zusaetze", 1L),
                                       ("reservation", 0L),
                                       ("reservations", 0L)))),
          "zusatz-provision-split" -> (("0.00",
                                        List(("zusaetze", 1L),
                                             ("reservation", 0L),
                                             ("reservations", 0L)))),
          "zusatz-mwst-satz" -> (("7.00",
                                  List(("zusaetze", 1L),
                                       ("reservation", 0L),
                                       ("reservations", 0L)))),
          "zusatz-kuerzel" -> (("",
                                List(("zusaetze", 1L), ("reservation", 0L), ("reservations", 0L))))
        ).foreach { entry =>
          val rowHash = calculateDataElementStorageHash(entry._1, entry._2._2)
          dataTree ! DataTreeDocumentMessages.ReturnHashedData(entry._1,
                                                               rowHash,
                                                               Option(entry._2._2.head._2))
          val response = expectMsgType[DataTreeNodeMessages.Content]
          withClue("No data was returned!")(response.data.size should be > 0)
          withClue("Data not correct!")(
            response.data.find(_.dataElementHash.getOrElse("") == rowHash).get.data match {
              case bs: ByteString => bs should be(ByteString(entry._2._1))
              case otherData      => otherData.toString should be(entry._2._1)
            }
          )
        }

        Map(
          "zusatz-name" -> (("Parkplatz",
                             List(("zusaetze", 0L), ("reservation", 0L), ("reservations", 7L)))),
          "zusatz-anzahl" -> (("4",
                               List(("zusaetze", 0L), ("reservation", 0L), ("reservations", 7L)))),
          "zusatz-einzelpreis" -> (("0.00",
                                    List(("zusaetze", 0L),
                                         ("reservation", 0L),
                                         ("reservations", 7L)))),
          "zusatz-buchungsnr" -> (("4814113344",
                                   List(("zusaetze", 0L),
                                        ("reservation", 0L),
                                        ("reservations", 7L)))),
          "zusatz-provision" -> (("0.00",
                                  List(("zusaetze", 0L),
                                       ("reservation", 0L),
                                       ("reservations", 7L)))),
          "zusatz-provision-split" -> (("0.00",
                                        List(("zusaetze", 0L),
                                             ("reservation", 0L),
                                             ("reservations", 7L)))),
          "zusatz-mwst-satz" -> (("7.00",
                                  List(("zusaetze", 0L),
                                       ("reservation", 0L),
                                       ("reservations", 7L)))),
          "zusatz-kuerzel" -> (("",
                                List(("zusaetze", 0L), ("reservation", 0L), ("reservations", 7L))))
        ).foreach { entry =>
          val rowHash = calculateDataElementStorageHash(entry._1, entry._2._2)
          dataTree ! DataTreeDocumentMessages.ReturnHashedData(entry._1,
                                                               rowHash,
                                                               Option(entry._2._2.head._2))
          val response = expectMsgType[DataTreeNodeMessages.Content]
          withClue("No data was returned!")(response.data.size should be > 0)
          withClue("Data not correct!")(
            response.data.find(_.dataElementHash.getOrElse("") == rowHash).get.data match {
              case bs: ByteString => bs should be(ByteString(entry._2._1))
              case otherData      => otherData.toString should be(entry._2._1)
            }
          )
        }

        Map(
          "zusatz-name" -> (("Logis Splitting B",
                             List(("zusaetze", 1L), ("reservation", 0L), ("reservations", 7L)))),
          "zusatz-anzahl" -> (("1",
                               List(("zusaetze", 1L), ("reservation", 0L), ("reservations", 7L)))),
          "zusatz-einzelpreis" -> (("78.00",
                                    List(("zusaetze", 1L),
                                         ("reservation", 0L),
                                         ("reservations", 7L)))),
          "zusatz-buchungsnr" -> (("4814113344",
                                   List(("zusaetze", 1L),
                                        ("reservation", 0L),
                                        ("reservations", 7L)))),
          "zusatz-provision" -> (("0.00",
                                  List(("zusaetze", 1L),
                                       ("reservation", 0L),
                                       ("reservations", 7L)))),
          "zusatz-provision-split" -> (("0.00",
                                        List(("zusaetze", 1L),
                                             ("reservation", 0L),
                                             ("reservations", 7L)))),
          "zusatz-mwst-satz" -> (("19.00",
                                  List(("zusaetze", 1L),
                                       ("reservation", 0L),
                                       ("reservations", 7L)))),
          "zusatz-kuerzel" -> (("",
                                List(("zusaetze", 1L), ("reservation", 0L), ("reservations", 7L))))
        ).foreach { entry =>
          val rowHash = calculateDataElementStorageHash(entry._1, entry._2._2)
          dataTree ! DataTreeDocumentMessages.ReturnHashedData(entry._1,
                                                               rowHash,
                                                               Option(entry._2._2.head._2))
          val response = expectMsgType[DataTreeNodeMessages.Content]
          withClue("No data was returned!")(response.data.size should be > 0)
          withClue("Data not correct!")(
            response.data.find(_.dataElementHash.getOrElse("") == rowHash).get.data match {
              case bs: ByteString => bs should be(ByteString(entry._2._1))
              case otherData      => otherData.toString should be(entry._2._1)
            }
          )
        }

        Map(
          "kinder-alter-age" -> (("14",
                                  List(("kinder", 0L), ("reservation", 0L), ("reservations", 1L)))),
          "kinder-alter-anzahl" -> (("1",
                                     List(("kinder", 0L),
                                          ("reservation", 0L),
                                          ("reservations", 1L))))
        ).foreach { entry =>
          val rowHash = calculateDataElementStorageHash(entry._1, entry._2._2)
          dataTree ! DataTreeDocumentMessages.ReturnHashedData(entry._1,
                                                               rowHash,
                                                               Option(entry._2._2.head._2))
          val response = expectMsgType[DataTreeNodeMessages.Content]
          withClue("No data was returned!")(response.data.size should be > 0)
          withClue("Data not correct!")(
            response.data.find(_.dataElementHash.getOrElse("") == rowHash).get.data match {
              case bs: ByteString => bs should be(ByteString(entry._2._1))
              case otherData      => otherData.toString should be(entry._2._1)
            }
          )
        }
      }
    }
  }
}
