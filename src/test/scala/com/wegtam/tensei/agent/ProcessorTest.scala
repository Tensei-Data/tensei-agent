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

package com.wegtam.tensei.agent

import java.io.File

import akka.testkit.{ TestActorRef, TestFSMRef }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.Processor.ProcessorMessages
import com.wegtam.tensei.agent.Processor.ProcessorMessages.StartProcessingMessage
import com.wegtam.tensei.agent.adt._
import com.wegtam.tensei.agent.parsers.{ BaseParserMessages, FileParser }

class ProcessorTest extends ActorSpec with XmlTestHelpers {
  describe("Processor") {
    describe("when given a simple data tree and a simple source and target dfasdl") {
      val sourceData =
        getClass.getResource("/com/wegtam/tensei/agent/processors/simple-dfasdl-data.csv").toURI
      val dfasdl = DFASDL(
        "SIMPLE-DFASDL",
        scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/processors/simple-dfasdl.xml")
          )
          .mkString
      )
      val sourceElements = List(
        ElementReference(dfasdl.id, "firstname"),
        ElementReference(dfasdl.id, "lastname"),
        ElementReference(dfasdl.id, "email"),
        ElementReference(dfasdl.id, "birthday")
      )
      val targetElements = List(
        ElementReference(dfasdl.id, "firstname"),
        ElementReference(dfasdl.id, "lastname"),
        ElementReference(dfasdl.id, "email"),
        ElementReference(dfasdl.id, "birthday")
      )
      val mapping  = MappingTransformation(sourceElements, targetElements)
      val recipe   = new Recipe("COPY-COLUMNS", Recipe.MapOneToOne, List(mapping))
      val cookbook = Cookbook("COOKBOOK", List(dfasdl), Option(dfasdl), List(recipe))
      val source =
        ConnectionInformation(sourceData, Option(DFASDLReference(cookbook.id, dfasdl.id)))
      val targetData = File.createTempFile("ProcessorTest", "tmpData").toURI
      val target =
        ConnectionInformation(targetData, Option(DFASDLReference(cookbook.id, dfasdl.id)))

      it("should process the data correctly") {
        val dataTree =
          TestActorRef(DataTreeDocument.props(dfasdl, Option("ProcessorTest"), Set.empty[String]))
        val fileParser =
          TestActorRef(FileParser.props(source, cookbook, dataTree, Option("ProcessorTest")))

        fileParser ! BaseParserMessages.SubParserInitialize
        expectMsg(BaseParserMessages.SubParserInitialized)
        fileParser ! BaseParserMessages.Start
        val response = expectMsgType[ParserStatusMessage]
        response.status should be(ParserStatus.COMPLETED)
        fileParser ! BaseParserMessages.Stop

        val processor = TestFSMRef(new Processor(Option("ProcessorTest")))

        val msg =
          new AgentStartTransformationMessage(List(source), target, cookbook, Option("1-2-3-4"))
        val pmsg = StartProcessingMessage(msg, List(dataTree))

        processor ! pmsg

        expectMsg(ProcessorMessages.Completed)

        val actualData = scala.io.Source.fromURI(targetData).mkString
        val expectedData = scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/processors/simple-dfasdl-data-expected-target.csv"
            )
          )
          .mkString

        actualData shouldEqual expectedData
      }
    }

    describe("when processing a simple data source into a simple target") {
      describe("having source fields that are too long for the target") {
        it("should truncate the appropriate target fields") {
          val sourceData =
            getClass.getResource("/com/wegtam/tensei/agent/processors/long-column.csv").toURI
          val sourceDfasdl =
            DFASDL("SOURCE-01",
                   scala.io.Source
                     .fromInputStream(
                       getClass.getResourceAsStream(
                         "/com/wegtam/tensei/agent/processors/long-column-source.xml"
                       )
                     )
                     .mkString)
          val targetDfasdl =
            DFASDL("TARGET",
                   scala.io.Source
                     .fromInputStream(
                       getClass.getResourceAsStream(
                         "/com/wegtam/tensei/agent/processors/long-column-target.xml"
                       )
                     )
                     .mkString)
          val sourceElements = List(
            ElementReference(sourceDfasdl.id, "birthday"),
            ElementReference(sourceDfasdl.id, "notes")
          )
          val targetElements = List(
            ElementReference(targetDfasdl.id, "birthday"),
            ElementReference(targetDfasdl.id, "notes")
          )
          val mapping = MappingTransformation(sourceElements, targetElements)
          val recipe  = new Recipe("COPY-COLUMNS", Recipe.MapOneToOne, List(mapping))
          val cookbook =
            Cookbook("COOKBOOK", List(sourceDfasdl), Option(targetDfasdl), List(recipe))
          val source = ConnectionInformation(sourceData,
                                             Option(DFASDLReference(cookbook.id, sourceDfasdl.id)))
          val targetData = File.createTempFile("ProcessorTest", "tmpData").toURI
          val target = ConnectionInformation(targetData,
                                             Option(DFASDLReference(cookbook.id, targetDfasdl.id)))

          val dataTree = TestActorRef(
            DataTreeDocument.props(sourceDfasdl, Option("ProcessorTest"), Set.empty[String])
          )
          val fileParser =
            TestActorRef(FileParser.props(source, cookbook, dataTree, Option("ProcessorTest")))

          fileParser ! BaseParserMessages.SubParserInitialize
          expectMsg(BaseParserMessages.SubParserInitialized)
          fileParser ! BaseParserMessages.Start
          val response = expectMsgType[ParserStatusMessage]
          response.status should be(ParserStatus.COMPLETED)
          fileParser ! BaseParserMessages.Stop

          val processor = TestFSMRef(new Processor(Option("ProcessorTest")))

          val msg =
            new AgentStartTransformationMessage(List(source), target, cookbook, Option("1-2-3-4"))
          val pmsg = StartProcessingMessage(msg, List(dataTree))

          processor ! pmsg

          expectMsg(ProcessorMessages.Completed)

          val actualData = scala.io.Source.fromURI(targetData).mkString
          val expectedData = scala.io.Source
            .fromInputStream(
              getClass.getResourceAsStream(
                "/com/wegtam/tensei/agent/processors/long-column-expected-target.csv"
              )
            )
            .mkString

          actualData shouldEqual expectedData
        }
      }
    }

    describe("when given a file that contains no data") {
      it("should work") {
        val sourceData =
          getClass.getResource("/com/wegtam/tensei/agent/processors/empty.csv").toURI
        val sourceDfasdl = DFASDL(
          "SOURCE-01",
          scala.io.Source
            .fromInputStream(
              getClass
                .getResourceAsStream("/com/wegtam/tensei/agent/processors/long-column-source.xml")
            )
            .mkString
        )
        val targetDfasdl = DFASDL(
          "TARGET",
          scala.io.Source
            .fromInputStream(
              getClass
                .getResourceAsStream("/com/wegtam/tensei/agent/processors/long-column-target.xml")
            )
            .mkString
        )
        val sourceElements = List(
          ElementReference(sourceDfasdl.id, "birthday"),
          ElementReference(sourceDfasdl.id, "notes")
        )
        val targetElements = List(
          ElementReference(targetDfasdl.id, "birthday"),
          ElementReference(targetDfasdl.id, "notes")
        )
        val mapping  = MappingTransformation(sourceElements, targetElements)
        val recipe   = new Recipe("COPY-COLUMNS", Recipe.MapOneToOne, List(mapping))
        val cookbook = Cookbook("COOKBOOK", List(sourceDfasdl), Option(targetDfasdl), List(recipe))
        val source =
          ConnectionInformation(sourceData, Option(DFASDLReference(cookbook.id, sourceDfasdl.id)))
        val targetData = File.createTempFile("ProcessorTest", "tmpData").toURI
        val target =
          ConnectionInformation(targetData, Option(DFASDLReference(cookbook.id, targetDfasdl.id)))

        val dataTree = TestActorRef(
          DataTreeDocument.props(sourceDfasdl, Option("ProcessorTest"), Set.empty[String])
        )
        val fileParser =
          TestActorRef(FileParser.props(source, cookbook, dataTree, Option("ProcessorTest")))

        fileParser ! BaseParserMessages.SubParserInitialize
        expectMsg(BaseParserMessages.SubParserInitialized)
        fileParser ! BaseParserMessages.Start
        val response = expectMsgType[ParserStatusMessage]
        response.status should be(ParserStatus.COMPLETED)
        fileParser ! BaseParserMessages.Stop

        val processor = TestFSMRef(new Processor(Option("ProcessorTest")))

        val msg =
          new AgentStartTransformationMessage(List(source), target, cookbook, Option("1-2-3-4"))
        val pmsg = StartProcessingMessage(msg, List(dataTree))

        processor ! pmsg

        expectMsg(ProcessorMessages.Completed)

        val actualData = scala.io.Source.fromURI(targetData).mkString
        val expectedData = scala.io.Source
          .fromInputStream(
            getClass
              .getResourceAsStream("/com/wegtam/tensei/agent/processors/empty-expected-target.csv")
          )
          .mkString

        actualData shouldEqual expectedData
      }
    }
  }
}
