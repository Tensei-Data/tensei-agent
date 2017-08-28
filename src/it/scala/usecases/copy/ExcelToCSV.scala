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

package usecases.copy

import java.io.{ File, InputStream }
import java.net.URI

import akka.testkit.{ TestActorRef, TestFSMRef }
import com.wegtam.tensei.adt.Recipe.{ MapAllToAll, MapOneToOne }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages.{
  GetSequenceRowCount,
  SequenceRowCount
}
import com.wegtam.tensei.agent.Parser.{ ParserCompletedStatus, ParserMessages }
import com.wegtam.tensei.agent.Processor.ProcessorMessages
import com.wegtam.tensei.agent._
import com.wegtam.tensei.agent.adt.ParserStatus

import scala.concurrent.duration.{ FiniteDuration, SECONDS }

class ExcelToCSV extends XmlActorSpec with XmlTestHelpers {
  val agentRunIdentifier = Option("ExcelToCSVTest")

  describe("Use case Excel to CSV") {
    describe("when given an Excel file should migrate into CSV") {
      it(
        "should create a CSV from the Excel file and migrate the content to another CSV dependent on the specification"
      ) {

        val sourceURI = getClass.getResource("/usecases/copy/excel/minimal.xls").toURI
        val targetFilePath =
          File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

        lazy val sourceDfasdl: DFASDL = {
          val in: InputStream = getClass.getResourceAsStream("/usecases/copy/excel/minimal.xml")
          val xml             = scala.io.Source.fromInputStream(in).mkString
          DFASDL("Source", xml)
        }

        lazy val targetDfasdl = {
          val in: InputStream =
            getClass.getResourceAsStream("/usecases/copy/excel/minimal-target.xml")
          val xml = scala.io.Source.fromInputStream(in).mkString
          DFASDL("Target", xml)
        }

        val cookbook = Cookbook(
          "MY-COOKBOOK",
          List(sourceDfasdl),
          Option(targetDfasdl),
          List(
            new Recipe(
              "MAP-HEADER",
              MapOneToOne,
              List(
                new MappingTransformation(
                  List(ElementReference(sourceDfasdl.id, "header")),
                  List(ElementReference(targetDfasdl.id, "header")),
                  List(
                    new TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                                  new TransformerOptions(classOf[String],
                                                                         classOf[String],
                                                                         List.empty))
                  )
                )
              )
            ),
            new Recipe(
              "MAP-DATA",
              MapAllToAll,
              List(
                new MappingTransformation(List(ElementReference(sourceDfasdl.id, "age")),
                                          List(ElementReference(targetDfasdl.id, "age"))),
                new MappingTransformation(List(ElementReference(sourceDfasdl.id, "price")),
                                          List(ElementReference(targetDfasdl.id, "price"))),
                new MappingTransformation(List(ElementReference(sourceDfasdl.id, "birthdate")),
                                          List(ElementReference(targetDfasdl.id, "birthdate"))),
                new MappingTransformation(List(ElementReference(sourceDfasdl.id, "birthtime")),
                                          List(ElementReference(targetDfasdl.id, "birthtime"))),
                new MappingTransformation(
                  List(ElementReference(sourceDfasdl.id, "firstname"),
                       ElementReference(sourceDfasdl.id, "name")),
                  List(ElementReference(targetDfasdl.id, "name")),
                  List(
                    new TransformationDescription("com.wegtam.tensei.agent.transformers.Concat",
                                                  new TransformerOptions(classOf[String],
                                                                         classOf[String],
                                                                         List(("separator", " "))))
                  )
                )
              )
            )
          )
        )

        val sourceConnection =
          ConnectionInformation(uri = sourceURI,
                                dfasdlRef = Option(DFASDLReference(cookbook.id, sourceDfasdl.id)),
                                languageTag = Option("de_DE"))
        val targetConnection =
          ConnectionInformation(uri = new URI(s"file:$targetFilePath"),
                                dfasdlRef = Option(DFASDLReference(cookbook.id, targetDfasdl.id)))

        val agentStartTransformationMessage =
          AgentStartTransformationMessage(List(sourceConnection),
                                          targetConnection,
                                          cookbook,
                                          agentRunIdentifier)

        val dataTree =
          TestActorRef(DataTreeDocument.props(sourceDfasdl, agentRunIdentifier, Set.empty[String]))
        val parser = TestFSMRef(new Parser(agentRunIdentifier))

        parser ! ParserMessages.StartParsing(agentStartTransformationMessage,
                                             Map(sourceDfasdl.hashCode() -> dataTree))
        val parserResponse = expectMsgType[ParserCompletedStatus](FiniteDuration(20, SECONDS))
        parserResponse.statusMessages.foreach(status => status should be(ParserStatus.COMPLETED))

        dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "people"))
        val count = expectMsgType[SequenceRowCount](FiniteDuration(40, SECONDS))
        count.rows.getOrElse(0L) should be(3)

        val processor = TestFSMRef(new Processor(agentRunIdentifier))
        processor ! ProcessorMessages.StartProcessingMessage(agentStartTransformationMessage,
                                                             List(dataTree))
        expectMsg(FiniteDuration(60, SECONDS), ProcessorMessages.Completed)

        val expectedData = scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream("/usecases/copy/excel/minimal-expected.csv")
          )
          .mkString
        val actualData = scala.io.Source.fromFile(targetFilePath).mkString
        actualData should be(expectedData)
      }
    }
  }
}
