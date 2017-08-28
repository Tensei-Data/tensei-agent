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

package com.wegtam.tensei.agent.parsers.FileParsers.vCard

import java.io.StringReader
import javax.xml.xpath.{ XPathConstants, XPathFactory }

import akka.util.ByteString
import com.wegtam.tensei.adt.ElementReference
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.XmlActorSpec
import org.w3c.dom.{ Node, NodeList }
import org.xml.sax.InputSource

class ComplexVCardTest extends XmlActorSpec {
  describe("FileParser") {
    describe("VCard") {
      describe("when given a file containing several vcards") {
        val dataFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-01.vcf"

        describe("with a simple description using a sequence") {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-01.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-01-expected-structure.xml"

            val r = prepareFileParserStructureComparison(dataFile, dfasdlFile, expectedFile)

            val expectedNodes = r._1
            val actualNodes   = r._2

            actualNodes.size should be(expectedNodes.size)
            compareXmlStructureNodes(expectedNodes, actualNodes)
          }

          it("should extract the correct data") {
            val expectedDataXml = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-01-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
            compareSequenceData("start_tag", expectedDataTree, dataTree)
            compareSequenceData("version", expectedDataTree, dataTree)
            compareSequenceData("name", expectedDataTree, dataTree)
            compareSequenceData("full_name", expectedDataTree, dataTree)
            compareSequenceData("organisation", expectedDataTree, dataTree)
            compareSequenceData("title", expectedDataTree, dataTree)
            compareSequenceData("photo", expectedDataTree, dataTree)
            compareSequenceData("phone_work", expectedDataTree, dataTree)
            compareSequenceData("phone_home", expectedDataTree, dataTree)
            compareSequenceData("address_work", expectedDataTree, dataTree)
            compareSequenceData("label_work", expectedDataTree, dataTree)
            compareSequenceData("address_home", expectedDataTree, dataTree)
            compareSequenceData("label_home", expectedDataTree, dataTree)
            compareSequenceData("email", expectedDataTree, dataTree)
            compareSequenceData("revision", expectedDataTree, dataTree)
            compareSequenceData("end_tag", expectedDataTree, dataTree)
          }
        }

        describe("with a simple description using a sequence with a maximum") {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-02.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-02-expected-structure.xml"

            val r = prepareFileParserStructureComparison(dataFile, dfasdlFile, expectedFile)

            val expectedNodes = r._1
            val actualNodes   = r._2

            actualNodes.size should be(expectedNodes.size)
            compareXmlStructureNodes(expectedNodes, actualNodes)
          }

          it("should extract the correct data") {
            val expectedDataXml = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-02-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
            compareSequenceData("start_tag", expectedDataTree, dataTree)
            compareSequenceData("version", expectedDataTree, dataTree)
            compareSequenceData("name", expectedDataTree, dataTree)
            compareSequenceData("full_name", expectedDataTree, dataTree)
            compareSequenceData("organisation", expectedDataTree, dataTree)
            compareSequenceData("title", expectedDataTree, dataTree)
            compareSequenceData("photo", expectedDataTree, dataTree)
            compareSequenceData("phone_work", expectedDataTree, dataTree)
            compareSequenceData("phone_home", expectedDataTree, dataTree)
            compareSequenceData("address_work", expectedDataTree, dataTree)
            compareSequenceData("label_work", expectedDataTree, dataTree)
            compareSequenceData("address_home", expectedDataTree, dataTree)
            compareSequenceData("label_home", expectedDataTree, dataTree)
            compareSequenceData("email", expectedDataTree, dataTree)
            compareSequenceData("revision", expectedDataTree, dataTree)
            compareSequenceData("end_tag", expectedDataTree, dataTree)
          }
        }

        describe("with a simple description using stacked sequences") {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-03.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-03-expected-structure.xml"

            val r = prepareFileParserStructureComparison(dataFile, dfasdlFile, expectedFile)

            val expectedNodes = r._1
            val actualNodes   = r._2

            actualNodes.size should be(expectedNodes.size)
            compareXmlStructureNodes(expectedNodes, actualNodes)
          }

          it("should extract the correct data") {
            val expectedDataXml = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-03-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)
            dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
              ElementReference("MY-DFASDL", "entries")
            )
            val entryCount = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
            entryCount.rows.getOrElse(0L) should be(48L)

            compareSimpleDataNodes(expectedDataTree, dataTree)
            compareStackedSequenceData("value", "entries", expectedDataTree, dataTree)
          }
        }

        describe("with a simple description using 3-times stacked sequences") {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-04.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-04-expected-structure.xml"

            val r = prepareFileParserStructureComparison(dataFile, dfasdlFile, expectedFile)

            val expectedNodes = r._1
            val actualNodes   = r._2

            actualNodes.size should be(expectedNodes.size)
            compareXmlStructureNodes(expectedNodes, actualNodes)
          }

          it("should extract the correct data") {
            val expectedDataXml = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-04-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)

            val xpath = XPathFactory.newInstance().newXPath()
            val entries = xpath
              .evaluate(s"""//*[@class="id:entries"]""", expectedDataTree, XPathConstants.NODESET)
              .asInstanceOf[NodeList]
            withClue("No entries found in expected data tree!")(entries.getLength should be > 0)

            val maxSequenceRowsPerActor =
              system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

            for (vcardsCounter <- 0 until entries.getLength) {
              for (entryCounter <- 0 to 15) {
                val sequenceData = List(("columns", 0L),
                                        ("entries", entryCounter.toLong),
                                        ("vcards", vcardsCounter.toLong))

                withClue("The column-data should be correct!") {
                  val currentHash = calculateDataElementStorageHash("column-data", sequenceData)

                  dataTree ! DataTreeDocumentMessages.ReturnHashedData("column-data", currentHash)
                  val response = expectMsgType[DataTreeNodeMessages.Content]
                  response.data.size should be > 0
                  val xpathExpression = s"""//*[@class="id:column-data"]"""
                  val expectedData = xpath
                    .evaluate(xpathExpression, expectedDataTree, XPathConstants.NODESET)
                    .asInstanceOf[NodeList]
                  withClue(s"Node not found in data tree with xpath '$xpathExpression'!")(
                    expectedData should not be null
                  )
                  response
                    .data(((vcardsCounter * 16 + entryCounter) % maxSequenceRowsPerActor).toInt)
                    .data match {
                    case bs: ByteString =>
                      bs.utf8String should be(
                        expectedData.item(vcardsCounter * 16 + entryCounter).getTextContent
                      )
                    case otherData =>
                      otherData.toString should be(
                        expectedData.item(vcardsCounter * 16 + entryCounter).getTextContent
                      )
                  }
                }

                withClue("The column2-data should be correct!") {
                  val currentHash = calculateDataElementStorageHash("column2-data", sequenceData)

                  dataTree ! DataTreeDocumentMessages.ReturnHashedData("column2-data", currentHash)
                  val response = expectMsgType[DataTreeNodeMessages.Content]
                  response.data.size should be > 0
                  val xpathExpression = s"""//*[@class="id:column2-data"]"""
                  val expectedData = xpath
                    .evaluate(xpathExpression, expectedDataTree, XPathConstants.NODESET)
                    .asInstanceOf[NodeList]
                  withClue(s"Node not found in data tree with xpath '$xpathExpression'!")(
                    expectedData should not be null
                  )
                  response
                    .data(((vcardsCounter * 16 + entryCounter) % maxSequenceRowsPerActor).toInt)
                    .data match {
                    case bs: ByteString =>
                      bs.utf8String should be(
                        expectedData.item(vcardsCounter * 16 + entryCounter).getTextContent
                      )
                    case otherData =>
                      otherData.toString should be(
                        expectedData.item(vcardsCounter * 16 + entryCounter).getTextContent
                      )
                  }
                }
              }
            }
          }
        }

        describe(
          "with a simple description using 3-times stacked sequences without surrounding elements"
        ) {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-05.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-05-expected-structure.xml"

            val r = prepareFileParserStructureComparison(dataFile, dfasdlFile, expectedFile)

            val expectedNodes = r._1
            val actualNodes   = r._2

            actualNodes.size should be(expectedNodes.size)
            compareXmlStructureNodes(expectedNodes, actualNodes)
          }

          it("should extract the correct data") {
            val expectedDataXml = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-05-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)

            val xpath = XPathFactory.newInstance().newXPath()
            val entries = xpath
              .evaluate(s"""//*[@class="id:entries"]""", expectedDataTree, XPathConstants.NODESET)
              .asInstanceOf[NodeList]
            withClue("No entries found in expected data tree!")(entries.getLength should be > 0)

            val maxSequenceRowsPerActor =
              system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

            for (vcardsCounter <- 0 until entries.getLength) {
              for (entryCounter <- 0 to 15) {
                val sequenceData = List(("columns", 0L),
                                        ("entries", entryCounter.toLong),
                                        ("vcards", vcardsCounter.toLong))
                val currentHash = calculateDataElementStorageHash("column2-data", sequenceData)

                dataTree ! DataTreeDocumentMessages.ReturnHashedData("column2-data", currentHash)
                val response = expectMsgType[DataTreeNodeMessages.Content]
                response.data.size should be > 0
                val xpathExpression = s"""//*[@class="id:column2-data"]"""
                val expectedData = xpath
                  .evaluate(xpathExpression, expectedDataTree, XPathConstants.NODESET)
                  .asInstanceOf[NodeList]
                withClue(s"Node not found in data tree with xpath '$xpathExpression'!")(
                  expectedData should not be null
                )
                response
                  .data(((vcardsCounter * 16 + entryCounter) % maxSequenceRowsPerActor).toInt)
                  .data match {
                  case bs: ByteString =>
                    bs.utf8String should be(
                      expectedData.item(vcardsCounter * 16 + entryCounter).getTextContent
                    )
                  case otherData =>
                    otherData.toString should be(
                      expectedData.item(vcardsCounter * 16 + entryCounter).getTextContent
                    )
                }
              }
            }
          }
        }

        describe(
          "with a simple description using 2-times stacked and sibling sequences without surrounding elements"
        ) {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-06.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-06-expected-structure.xml"

            val r = prepareFileParserStructureComparison(dataFile, dfasdlFile, expectedFile)

            val expectedNodes = r._1
            val actualNodes   = r._2

            actualNodes.size should be(expectedNodes.size)
            compareXmlStructureNodes(expectedNodes, actualNodes)
          }

          it("should extract the correct data") {
            val expectedDataXml = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-06-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
            compareStackedSequenceData("start_tag", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("version", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("name", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("full_name", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("organisation", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("title", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("photo", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("phone_work", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("phone_home", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("address_work", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("label_work", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("address_home", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("label_home", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("email", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("revision", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("end_tag", "vcards2", expectedDataTree, dataTree)
          }
        }

        describe(
          "with a simple description using 2-times stacked and sibling sequences with surrounding elements"
        ) {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-07.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-07-expected-structure.xml"

            val r = prepareFileParserStructureComparison(dataFile, dfasdlFile, expectedFile)

            val expectedNodes = r._1
            val actualNodes   = r._2

            actualNodes.size should be(expectedNodes.size)

            compareXmlStructureNodes(expectedNodes, actualNodes)
          }

          it("should extract the correct data") {
            val expectedDataXml = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/complex-07-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
            compareStackedSequenceData("start_tag", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("version", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("name", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("full_name", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("organisation", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("title", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("photo", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("phone_work", "vcards", expectedDataTree, dataTree)
            compareStackedSequenceData("phone_home", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("address_work", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("label_work", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("address_home", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("label_home", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("email", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("revision", "vcards2", expectedDataTree, dataTree)
            compareStackedSequenceData("end_tag", "vcards2", expectedDataTree, dataTree)
          }
        }
      }

      describe("when given a file containing a single vcard") {
        val dataFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/choice-01.vcf"

        describe("with a simple description using a sequence and a choice") {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/choice-01.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/choice-01-expected-structure.xml"

            val r = prepareFileParserStructureComparison(dataFile, dfasdlFile, expectedFile)

            val expectedNodes = r._1
            val actualNodes   = r._2

            actualNodes.size should be(expectedNodes.size)

            compareXmlStructureNodes(expectedNodes, actualNodes)
          }

          it("should extract the correct data") {
            val expectedDataXml = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/choice-01-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)

            val xpath = XPathFactory.newInstance().newXPath()
            val elements = List(
              "start_tag",
              "version",
              "name",
              "full_name",
              "organisation",
              "title",
              "photo",
              "phone_work",
              "phone_home",
              "address_work",
              "label_work",
              "address_home",
              "label_home",
              "email",
              "revision",
              "end_tag"
            )

            elements.zipWithIndex.foreach { entry =>
              val id           = entry._1
              val row          = entry._2
              val sequenceData = List(("entries", row.toLong), ("vcards", 0L))
              val currentHash  = calculateDataElementStorageHash(id, sequenceData)

              dataTree ! DataTreeDocumentMessages.ReturnHashedData(id,
                                                                   currentHash,
                                                                   Option(row.toLong))
              val response = expectMsgType[DataTreeNodeMessages.Content]
              response.data.size should be(1)
              response.data.head.elementId should be(id)
              val node = xpath
                .evaluate(s"""//*[@class="id:$id"]""", expectedDataTree, XPathConstants.NODE)
                .asInstanceOf[Node]
              withClue(s"Node with class='id:$id' not found in data tree!")(
                node should not be null
              )
              response.data.head.data match {
                case bs: ByteString => bs.utf8String should be(node.getTextContent)
                case otherData      => otherData.toString should be(node.getTextContent)
              }
            }

          }
        }
      }
    }
  }
}
