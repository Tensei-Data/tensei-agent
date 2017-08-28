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

package com.wegtam.tensei.agent.parsers.FileParsers.Text

import java.io.StringReader

import com.wegtam.tensei.agent.XmlActorSpec
import org.xml.sax.InputSource

class SimpleTextTest extends XmlActorSpec {
  describe("FileParser") {
    describe("Text") {
      describe("when given a simple text file") {
        val dataFile   = "/com/wegtam/tensei/agent/parsers/FileParsers/Text/text-01.txt"
        val dfasdlFile = "/com/wegtam/tensei/agent/parsers/FileParsers/Text/text-01.xml"

        it("should create the correct source structure") {
          val expectedFile =
            "/com/wegtam/tensei/agent/parsers/FileParsers/Text/text-01-expected-structure.xml"

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
                "/com/wegtam/tensei/agent/parsers/FileParsers/Text/text-01-expected-data.xml"
              )
            )
            .mkString
          val expectedDataTree =
            createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

          val dataTree = prepareFileParserDataComparison(dataFile, dfasdlFile)

          compareSimpleDataNodes(expectedDataTree, dataTree)
          compareSequenceData("content", expectedDataTree, dataTree)
        }
      }
    }
  }
}
