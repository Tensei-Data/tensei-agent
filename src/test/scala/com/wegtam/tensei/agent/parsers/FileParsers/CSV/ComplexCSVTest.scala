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

import java.io.{ BufferedWriter, File, FileWriter, StringReader }

import com.wegtam.tensei.adt.ElementReference
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.XmlActorSpec
import org.xml.sax.InputSource

class ComplexCSVTest extends XmlActorSpec {
  describe("FileParser") {
    describe("CSV") {
      describe("when given a simple csv with a basic complex description") {
        val dataFile   = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-01.csv"
        val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-01.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-01-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-01-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
        }
      }

      describe("when given a simple csv with a basic complex description using a fixseq") {
        val dataFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-01.csv"
        val dfasdlFile =
          "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-01-with-fixseq.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-01-with-fixseq-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-01-with-fixseq-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
          compareSequenceData("firstname", expectedDataTree, dataTree)
          compareSequenceData("lastname", expectedDataTree, dataTree)
          compareSequenceData("email", expectedDataTree, dataTree)
          compareSequenceData("birthday", expectedDataTree, dataTree)
          compareSequenceData("phone", expectedDataTree, dataTree)
          compareSequenceData("division", expectedDataTree, dataTree)
        }
      }

      describe(
        "when given a simple csv including blank lines with a basic complex description using a seq"
      ) {
        val dataFile   = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-02.csv"
        val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-02-with-seq.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-02-with-seq-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-02-with-seq-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          withClue("It should parse all data including blank lines.") {
            dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
              ElementReference("MY-DFASDL", "account_list")
            )
            val parsedRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
            parsedRows.rows.getOrElse(0L) should be(10L)
          }

          compareSimpleDataNodes(expectedDataTree, dataTree)
          compareChoiceInSequence("account_list", expectedDataTree, dataTree)
        }
      }

      describe(
        "when given a simple csv including spaces with a basic complex description using a seq and the trim attribute"
      ) {
        val dataFile   = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-03.csv"
        val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-03-with-seq.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-03-with-seq-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/complex-03-with-seq-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
          compareSequenceData("firstname", expectedDataTree, dataTree)
          compareSequenceData("lastname", expectedDataTree, dataTree)
          compareSequenceData("email", expectedDataTree, dataTree)
          compareSequenceData("birthday", expectedDataTree, dataTree)
          compareSequenceData("phone", expectedDataTree, dataTree)
          compareSequenceData("division", expectedDataTree, dataTree)
        }
      }

      describe("when given a simple csv with a simple choice") {
        val dataFile   = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/choice-01.csv"
        val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/choice-01.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/choice-01-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/choice-01-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
        }
      }

      describe("when given a simple csv with a simple choice within a sequence") {
        val dataFile   = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/choice-02.csv"
        val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/choice-02.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/choice-02-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/choice-02-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
        }
      }

      describe("when given a simple csv with a stacked sequence using sequence stop signs") {
        val dataFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/sequence-stop-sign-01.csv"
        val dfasdlFile =
          "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/sequence-stop-sign-01.xml"

        it("should create the correct source structure with the last column empty") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/sequence-stop-sign-01-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/sequence-stop-sign-01-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
          dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
            ElementReference("MY-DFASDL", "columns")
          )
          val dataColumnCounter = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
          dataColumnCounter.rows.getOrElse(0L) should be(9)

          compareStackedSequenceData("data", "columns", expectedDataTree, dataTree)
        }
      }

      describe("when given a CSV with empty columns") {
        describe("with one empty column at the end of a line") {
          val dataFile = "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/empty-column-at-end.csv"
          val dfasdlFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/empty-column-at-end.xml"

          it("should create the correct source structure with the last column empty") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/empty-column-at-end-expected-structure.xml"

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
                  "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/empty-column-at-end-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
            compareSequenceData("entry-0", expectedDataTree, dataTree)
            compareSequenceData("entry-1", expectedDataTree, dataTree)
            compareSequenceData("entry-2", expectedDataTree, dataTree)
            compareSequenceData("entry-3", expectedDataTree, dataTree)
            compareSequenceData("entry-4", expectedDataTree, dataTree)
            compareSequenceData("entry-5", expectedDataTree, dataTree)
            compareSequenceData("entry-6", expectedDataTree, dataTree)
            compareSequenceData("entry-7", expectedDataTree, dataTree)
            compareSequenceData("entry-8", expectedDataTree, dataTree)
          }
        }

        describe("with one empty column at the end of a line and in DOS mode") {
          val tempFile = File.createTempFile("tensei-agent", "test")
          val bw       = new BufferedWriter(new FileWriter(tempFile))
          bw.write("0,211,Ozkan,Douglas,,647,EGZKSobTeknHCbLuHczvWmhTmCSGXD,OFFICE7152,")
          bw.write("\r\n")
          bw.write(
            "1,413,Suer,Candice,,314,OfOBVvpzNvHCebxyuxXFwsMju  JRU,OFFICE8586,(344) 999-2652"
          )
          bw.write("\r\n")
          bw.write("2,246,Somisetty,Jami,P,534,rAHWYkktOXAyPAYHlncZPG,,(984) 538-5366")
          bw.write("\r\n")
          bw.write(
            "3,248,Mazurek,Rosalinda,J,364,TJQqsUQQGqWG QleLheUoYlgRNVT,OFFICE8487,(860) 037-6897"
          )
          bw.write("\r\n")
          bw.close()
          val dataFile = tempFile.getAbsolutePath.replace("\\", "/")

          val dfasdlFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/empty-column-at-end.xml"

          it("should create the correct source structure with the last column empty") {
            val expectedFile =
              "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/empty-column-at-end-expected-structure.xml"

            val r =
              prepareFileParserStructureComparisonForTempFile(dataFile, dfasdlFile, expectedFile)

            val expectedNodes = r._1
            val actualNodes   = r._2

            actualNodes.size should be(expectedNodes.size)
            compareXmlStructureNodes(expectedNodes, actualNodes)
          }

          it("should extract the correct data") {
            val expectedDataXml = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/com/wegtam/tensei/agent/parsers/FileParsers/CSV/empty-column-at-end-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            val dataTree = prepareFileParserDataComparisonForTempFile(dataFile, dfasdlFile)

            compareSimpleDataNodes(expectedDataTree, dataTree)
            compareSequenceData("entry-0", expectedDataTree, dataTree)
            compareSequenceData("entry-1", expectedDataTree, dataTree)
            compareSequenceData("entry-2", expectedDataTree, dataTree)
            compareSequenceData("entry-3", expectedDataTree, dataTree)
            compareSequenceData("entry-4", expectedDataTree, dataTree)
            compareSequenceData("entry-5", expectedDataTree, dataTree)
            compareSequenceData("entry-6", expectedDataTree, dataTree)
            compareSequenceData("entry-7", expectedDataTree, dataTree)
            compareSequenceData("entry-8", expectedDataTree, dataTree)
          }
        }
      }
    }
  }
}
