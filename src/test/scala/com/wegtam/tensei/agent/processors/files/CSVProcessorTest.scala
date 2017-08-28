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

package com.wegtam.tensei.agent.processors.files

import java.io.File

import akka.testkit.{ TestActorRef, TestFSMRef }
import com.wegtam.tensei.agent.Parser.{ ParserCompletedStatus, ParserMessages }
import com.wegtam.tensei.agent.Processor.ProcessorMessages
import com.wegtam.tensei.agent.Processor.ProcessorMessages.StartProcessingMessage
import com.wegtam.tensei.agent._
import com.wegtam.tensei.agent.adt._
import com.wegtam.tensei.adt._

class CSVProcessorTest extends XmlActorSpec with XmlTestHelpers {
  describe("Processor") {
    describe("Files") {
      describe("processing simple-01.csv") {
        val dataFile = "/com/wegtam/tensei/agent/processors/files/simple-01.csv"

        describe("using simple-01.xml dfasdl") {
          val dfasdlFile = "/com/wegtam/tensei/agent/processors/files/simple-01.xml"

          describe("using the same target dfasdl") {
            it("should copy the file") {
              val sourceFilePath = getClass.getResource(dataFile).toURI
              val targetFilePath = File.createTempFile("CsvProcessor", "test").toURI
              val xml =
                scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
              val sourceDfasdl = DFASDL("SOURCE", xml)
              val targetDfasdl = DFASDL("TARGET", xml)

              val mappings = List(
                MappingTransformation(List(ElementReference(sourceDfasdl.id, "firstname")),
                                      List(ElementReference(targetDfasdl.id, "firstname")),
                                      List()),
                MappingTransformation(List(ElementReference(sourceDfasdl.id, "lastname")),
                                      List(ElementReference(targetDfasdl.id, "lastname")),
                                      List()),
                MappingTransformation(List(ElementReference(sourceDfasdl.id, "email")),
                                      List(ElementReference(targetDfasdl.id, "email")),
                                      List()),
                MappingTransformation(List(ElementReference(sourceDfasdl.id, "integer")),
                                      List(ElementReference(targetDfasdl.id, "integer")),
                                      List()),
                MappingTransformation(List(ElementReference(sourceDfasdl.id, "float1")),
                                      List(ElementReference(targetDfasdl.id, "float1")),
                                      List()),
                MappingTransformation(List(ElementReference(sourceDfasdl.id, "float2")),
                                      List(ElementReference(targetDfasdl.id, "float2")),
                                      List())
              )
              val recipe = Recipe("ID", Recipe.MapAllToAll, mappings)
              val cookbook =
                Cookbook("COOKBOOK", List(sourceDfasdl), Option(targetDfasdl), List(recipe))
              val source =
                ConnectionInformation(sourceFilePath,
                                      Option(DFASDLReference(cookbook.id, sourceDfasdl.id)))
              val target =
                ConnectionInformation(targetFilePath,
                                      Option(DFASDLReference(cookbook.id, targetDfasdl.id)))

              val agentStartTransformationMsg =
                AgentStartTransformationMessage(List(source), target, cookbook, Option("1-2-3-4"))

              val dataTree = TestActorRef(
                DataTreeDocument.props(sourceDfasdl, Option("CSVProcessorTest"), Set.empty[String])
              )

              val parser = TestFSMRef(new Parser(Option("CSVProcessorTest")))
              parser ! ParserMessages.StartParsing(agentStartTransformationMsg,
                                                   Map(sourceDfasdl.hashCode() -> dataTree))

              val parserResponse = expectMsgType[ParserCompletedStatus]
              parserResponse.statusMessages.foreach(
                status => status should be(ParserStatus.COMPLETED)
              )

              val processor = TestFSMRef(new Processor(Option("CSVProcessorTest")))
              processor ! StartProcessingMessage(agentStartTransformationMsg, List(dataTree))

              expectMsg(ProcessorMessages.Completed)

              val processedData =
                scala.io.Source.fromFile(target.uri.getSchemeSpecificPart).mkString
              val expectedData = scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream(
                    "/com/wegtam/tensei/agent/processors/files/simple-01-target.csv"
                  )
                )
                .mkString

              processedData should be(expectedData)
            }
          }
        }
      }
    }
  }
}
