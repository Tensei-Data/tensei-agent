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

import com.wegtam.tensei.agent.XmlActorSpec
import org.xml.sax.InputSource

class SimpleVCardTest extends XmlActorSpec {
  describe("FileParser") {
    describe("VCard") {
      describe("when given a simple vcard for one person") {
        val dataFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-01.vcf"

        describe("with a simple description") {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-01.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-01-expected-structure.xml"

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
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-01-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
          }
        }

        describe("with a simple description using formatted strings") {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-02.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-02-expected-structure.xml"

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
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-02-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
          }
        }

        describe("with a simple description using fancy formatted strings") {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-03.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-03-expected-structure.xml"

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
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-03-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
          }
        }

        describe("with a simple description using a formatted string within a fixed sequence") {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-04.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-04-expected-structure.xml"

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
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-04-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
            compareSequenceData("entry", expectedDataTree, dataTree)
          }
        }

        describe(
          "with a simple description using a formatted string within a sequence using a maximum"
        ) {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-05.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-05-expected-structure.xml"

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
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-05-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
            compareSequenceData("entry", expectedDataTree, dataTree)
          }
        }

        describe("with a simple description using a formatted string within an infinite sequence") {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-06.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-06-expected-structure.xml"

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
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-06-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
            compareSequenceData("entry", expectedDataTree, dataTree)
          }
        }

        describe(
          "with a simple description using a formatted string within an infinite sequence and an optional sibling"
        ) {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-07.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-07-expected-structure.xml"

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
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-07-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
            compareSequenceData("entry", expectedDataTree, dataTree)
          }
        }

        describe("with a simple description using a formatted string within 2 infinite sequences") {
          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-08.xml"

          it("should create the correct source structure") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-08-expected-structure.xml"

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
                  "/com/wegtam/tensei/agent/parsers/FileParsers/vCard/simple-08-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
            compareSequenceData("entry", expectedDataTree, dataTree)
            compareSequenceData("entry2", expectedDataTree, dataTree)
          }
        }
      }
    }
  }
}
