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

package com.wegtam.tensei.agent.parsers.FileParsers.CSV

import java.io.StringReader

import com.wegtam.tensei.agent.XmlActorSpec
import org.xml.sax.InputSource

class SimpleCSVTest extends XmlActorSpec {
  describe("FileParser") {
    describe("CSV") {
      describe("when given a simple csv with the most simple description") {
        val dataFile   = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01.csv"
        val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
        }
      }

      describe("when given a simple csv file using pipes with the most simple description") {
        val dataFile   = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-pipes.csv"
        val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-pipes.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-pipes-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-pipes-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
        }
      }

      describe("when given a simple csv file using quotes with the most simple description") {
        val dataFile   = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-quotes.csv"
        val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-quotes.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-quotes-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-quotes-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
        }
      }

      describe(
        "when given a simple csv file using quotes partially with the most simple description"
      ) {
        val dataFile =
          "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-quotes-partial.csv"
        val dfasdlFile =
          "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-quotes-partial.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-quotes-partial-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-quotes-partial-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
        }
      }

      describe("when given a simple csv file using semicolons with the most simple description") {
        val dataFile   = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-semicolon.csv"
        val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-semicolon.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-semicolon-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-semicolon-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
        }
      }

      describe("when given a simple csv file using tabs with the most simple description") {
        val dataFile   = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01.tsv"
        val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-tabs.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-tabs-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-01-tabs-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
        }
      }

      describe(
        "when given a simple csv file containing special characters with the most simple description"
      ) {
        val dataFile   = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-02.csv"
        val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-02.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-02-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-02-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
        }
      }

      describe("when given a simple csv file containing strings and numbers") {
        val dataFile   = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-03.csv"
        val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-03.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-03-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/simple-03-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
        }
      }
    }
  }
}
