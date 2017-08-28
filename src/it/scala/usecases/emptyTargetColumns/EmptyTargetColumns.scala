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

package usecases.emptyTargetColumns

import java.io.File
import java.net.URI

import akka.testkit.{ TestActorRef, TestFSMRef }
import com.wegtam.scalatest.tags.{ DbTest, DbTestH2 }
import com.wegtam.tensei.adt.Recipe.MapOneToOne
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.Parser.{ ParserCompletedStatus, ParserMessages }
import com.wegtam.tensei.agent.Processor.ProcessorMessages
import com.wegtam.tensei.agent.adt.ParserStatus
import com.wegtam.tensei.agent.{ DataTreeDocument, Parser, Processor, XmlActorSpec }

import scala.collection.mutable.ListBuffer

class EmptyTargetColumns extends XmlActorSpec {
  val agentRunIdentifier = Option("EmptyTargetColumnsTest")

  describe("Use cases") {
    describe("if not all target data columns are mapped") {
      describe("when writing into a file") {
        describe("and target columns have default values") {
          it("should write the default values to the not mapped columns") {
            val data           = getClass.getResource("/usecases/emptyTargetColumns/source.csv").toURI
            val targetFilePath = File.createTempFile("EmptyTargetColumns", "test").toURI
            val sourceDfasdl = DFASDL(
              "XML-SOURCE-DATA",
              scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream("/usecases/emptyTargetColumns/source-dfasdl.xml")
                )
                .mkString
            )
            val targetDfasdl = DFASDL(
              "DB-TARGET-DATA",
              scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream(
                    "/usecases/emptyTargetColumns/target-with-defaults-dfasdl.xml"
                  )
                )
                .mkString
            )

            val customersRecipe = Recipe(
              "MapColumns",
              MapOneToOne,
              List(
                MappingTransformation(
                  createElementReferenceList(sourceDfasdl, List("birthday")),
                  createElementReferenceList(targetDfasdl, List("birthday"))
                ),
                MappingTransformation(
                  createElementReferenceList(sourceDfasdl, List("firstname", "firstname")),
                  createElementReferenceList(targetDfasdl, List("firstname", "lastname")),
                  List(
                    TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                              TransformerOptions(classOf[String], classOf[String]))
                  )
                )
              )
            )

            val cookbook = Cookbook("EmptyColumnsTest",
                                    List(sourceDfasdl),
                                    Option(targetDfasdl),
                                    List(customersRecipe))

            val sourceConnection =
              ConnectionInformation(data, Option(DFASDLReference(cookbook.id, sourceDfasdl.id)))
            val targetConnection =
              ConnectionInformation(targetFilePath,
                                    Option(DFASDLReference(cookbook.id, targetDfasdl.id)))

            val agentStartTransformationMessage =
              AgentStartTransformationMessage(List(sourceConnection),
                                              targetConnection,
                                              cookbook,
                                              agentRunIdentifier)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("Xml2DbTest"), Set.empty[String])
            )
            val parser = TestFSMRef(new Parser(agentRunIdentifier))

            parser ! ParserMessages.StartParsing(agentStartTransformationMessage,
                                                 Map(sourceDfasdl.hashCode() -> dataTree))
            val parserResponse = expectMsgType[ParserCompletedStatus]
            parserResponse.statusMessages.foreach(
              status => status should be(ParserStatus.COMPLETED)
            )

            val processor = TestFSMRef(new Processor(agentRunIdentifier))
            processor ! ProcessorMessages.StartProcessingMessage(agentStartTransformationMessage,
                                                                 List(dataTree))
            expectMsg(ProcessorMessages.Completed)

            val actualData = scala.io.Source.fromURI(targetFilePath).mkString
            val expectedData = scala.io.Source
              .fromURI(
                getClass
                  .getResource("/usecases/emptyTargetColumns/expected-target-with-defaults.csv")
                  .toURI
              )
              .mkString

            withClue(s"The file $targetFilePath should have the proper content!")(
              actualData should be(expectedData)
            )
          }
        }

        describe("and target columns don't have default values") {
          it("should write an empty string to the not mapped columns") {
            val data           = getClass.getResource("/usecases/emptyTargetColumns/source.csv").toURI
            val targetFilePath = File.createTempFile("EmptyTargetColumns", "test").toURI
            val sourceDfasdl = DFASDL(
              "XML-SOURCE-DATA",
              scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream("/usecases/emptyTargetColumns/source-dfasdl.xml")
                )
                .mkString
            )
            val targetDfasdl = DFASDL(
              "DB-TARGET-DATA",
              scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream("/usecases/emptyTargetColumns/target-dfasdl.xml")
                )
                .mkString
            )

            val customersRecipe = Recipe(
              "MapColumns",
              MapOneToOne,
              List(
                MappingTransformation(
                  createElementReferenceList(sourceDfasdl, List("birthday")),
                  createElementReferenceList(targetDfasdl, List("birthday"))
                ),
                MappingTransformation(
                  createElementReferenceList(sourceDfasdl, List("firstname", "firstname")),
                  createElementReferenceList(targetDfasdl, List("firstname", "lastname")),
                  List(
                    TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                              TransformerOptions(classOf[String], classOf[String]))
                  )
                )
              )
            )

            val cookbook = Cookbook("EmptyColumnsTest",
                                    List(sourceDfasdl),
                                    Option(targetDfasdl),
                                    List(customersRecipe))

            val sourceConnection =
              ConnectionInformation(data, Option(DFASDLReference(cookbook.id, sourceDfasdl.id)))
            val targetConnection =
              ConnectionInformation(targetFilePath,
                                    Option(DFASDLReference(cookbook.id, targetDfasdl.id)))

            val agentStartTransformationMessage =
              AgentStartTransformationMessage(List(sourceConnection),
                                              targetConnection,
                                              cookbook,
                                              agentRunIdentifier)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("Xml2DbTest"), Set.empty[String])
            )
            val parser = TestFSMRef(new Parser(agentRunIdentifier))

            parser ! ParserMessages.StartParsing(agentStartTransformationMessage,
                                                 Map(sourceDfasdl.hashCode() -> dataTree))
            val parserResponse = expectMsgType[ParserCompletedStatus]
            parserResponse.statusMessages.foreach(
              status => status should be(ParserStatus.COMPLETED)
            )

            val processor = TestFSMRef(new Processor(agentRunIdentifier))
            processor ! ProcessorMessages.StartProcessingMessage(agentStartTransformationMessage,
                                                                 List(dataTree))
            expectMsg(ProcessorMessages.Completed)

            val actualData = scala.io.Source.fromURI(targetFilePath).mkString
            val expectedData = scala.io.Source
              .fromURI(
                getClass.getResource("/usecases/emptyTargetColumns/expected-target.csv").toURI
              )
              .mkString

            withClue(s"The file $targetFilePath should have the proper content!")(
              actualData should be(expectedData)
            )
          }
        }
      }

      describe("when writing into a database") {
        describe("and target columns have default values") {
          it("should write the default values to the not mapped columns", DbTest, DbTestH2) {
            val data = getClass.getResource("/usecases/emptyTargetColumns/source.csv").toURI
            val sourceDfasdl = DFASDL(
              "XML-SOURCE-DATA",
              scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream("/usecases/emptyTargetColumns/source-dfasdl.xml")
                )
                .mkString
            )
            val targetDfasdl = DFASDL(
              "DB-TARGET-DATA",
              scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream(
                    "/usecases/emptyTargetColumns/target-with-defaults-dfasdl.xml"
                  )
                )
                .mkString
            )

            val customersRecipe = Recipe(
              "MapColumns",
              MapOneToOne,
              List(
                MappingTransformation(
                  createElementReferenceList(sourceDfasdl, List("birthday")),
                  createElementReferenceList(targetDfasdl, List("birthday"))
                ),
                MappingTransformation(
                  createElementReferenceList(sourceDfasdl, List("firstname", "lastname")),
                  createElementReferenceList(targetDfasdl, List("firstname", "lastname")),
                  List(
                    TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                              TransformerOptions(classOf[String], classOf[String]))
                  )
                )
              )
            )

            val cookbook = Cookbook("EmptyColumnsTest",
                                    List(sourceDfasdl),
                                    Option(targetDfasdl),
                                    List(customersRecipe))

            val sourceConnection =
              ConnectionInformation(data, Option(DFASDLReference(cookbook.id, sourceDfasdl.id)))
            val targetDatabase = java.sql.DriverManager.getConnection("jdbc:h2:mem:emptyColumns1")
            val targetConnection =
              ConnectionInformation(new URI(targetDatabase.getMetaData.getURL),
                                    Option(DFASDLReference(cookbook.id, targetDfasdl.id)))

            val agentStartTransformationMessage =
              AgentStartTransformationMessage(List(sourceConnection),
                                              targetConnection,
                                              cookbook,
                                              agentRunIdentifier)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("Xml2DbTest"), Set.empty[String])
            )
            val parser = TestFSMRef(new Parser(agentRunIdentifier))

            parser ! ParserMessages.StartParsing(agentStartTransformationMessage,
                                                 Map(sourceDfasdl.hashCode() -> dataTree))
            val parserResponse = expectMsgType[ParserCompletedStatus]
            parserResponse.statusMessages.foreach(
              status => status should be(ParserStatus.COMPLETED)
            )

            val processor = TestFSMRef(new Processor(agentRunIdentifier))
            processor ! ProcessorMessages.StartProcessingMessage(agentStartTransformationMessage,
                                                                 List(dataTree))
            expectMsg(ProcessorMessages.Completed)

            val actualData = {
              val stm     = targetDatabase.createStatement()
              val results = stm.executeQuery("SELECT * FROM ROWS")
              val rows    = new ListBuffer[String]
              while (results.next()) {
                rows += s"${results.getString("firstname")},${results.getString("lastname")},${results
                  .getDate("birthday")}"
              }
              rows.toList.mkString(";")
            }
            val expectedData =
              """John,Doe,1879-03-14;John,Doe,1826-09-17;John,Doe,1777-04-30;John,Doe,1808-07-25;John,Doe,1646-07-01"""

            withClue(s"The database table should have the proper content!")(
              actualData should be(expectedData)
            )

            val dst = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:emptyColumns1")
            dst.createStatement().execute("SHUTDOWN")
            dst.close()
          }
        }

        describe("and target columns don't have default values") {
          it("should write an empty string to the not mapped columns", DbTest, DbTestH2) {
            val data = getClass.getResource("/usecases/emptyTargetColumns/source.csv").toURI
            val sourceDfasdl = DFASDL(
              "XML-SOURCE-DATA",
              scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream("/usecases/emptyTargetColumns/source-dfasdl.xml")
                )
                .mkString
            )
            val targetDfasdl = DFASDL(
              "DB-TARGET-DATA",
              scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream("/usecases/emptyTargetColumns/target-dfasdl.xml")
                )
                .mkString
            )

            val customersRecipe = Recipe(
              "MapColumns",
              MapOneToOne,
              List(
                MappingTransformation(
                  createElementReferenceList(sourceDfasdl, List("birthday")),
                  createElementReferenceList(targetDfasdl, List("birthday"))
                ),
                MappingTransformation(
                  createElementReferenceList(sourceDfasdl, List("firstname", "lastname")),
                  createElementReferenceList(targetDfasdl, List("firstname", "lastname")),
                  List(
                    TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                              TransformerOptions(classOf[String], classOf[String]))
                  )
                )
              )
            )

            val cookbook = Cookbook("EmptyColumnsTest",
                                    List(sourceDfasdl),
                                    Option(targetDfasdl),
                                    List(customersRecipe))

            val sourceConnection =
              ConnectionInformation(data, Option(DFASDLReference(cookbook.id, sourceDfasdl.id)))
            val targetDatabase = java.sql.DriverManager.getConnection("jdbc:h2:mem:emptyColumns2")
            val targetConnection =
              ConnectionInformation(new URI(targetDatabase.getMetaData.getURL),
                                    Option(DFASDLReference(cookbook.id, targetDfasdl.id)))

            val agentStartTransformationMessage =
              AgentStartTransformationMessage(List(sourceConnection),
                                              targetConnection,
                                              cookbook,
                                              agentRunIdentifier)

            val dataTree = TestActorRef(
              DataTreeDocument.props(sourceDfasdl, Option("Xml2DbTest"), Set.empty[String])
            )
            val parser = TestFSMRef(new Parser(agentRunIdentifier))

            parser ! ParserMessages.StartParsing(agentStartTransformationMessage,
                                                 Map(sourceDfasdl.hashCode() -> dataTree))
            val parserResponse = expectMsgType[ParserCompletedStatus]
            parserResponse.statusMessages.foreach(
              status => status should be(ParserStatus.COMPLETED)
            )

            val processor = TestFSMRef(new Processor(agentRunIdentifier))
            processor ! ProcessorMessages.StartProcessingMessage(agentStartTransformationMessage,
                                                                 List(dataTree))
            expectMsg(ProcessorMessages.Completed)

            val actualData = {
              val stm     = targetDatabase.createStatement()
              val results = stm.executeQuery("SELECT * FROM ROWS")
              val rows    = new ListBuffer[String]
              while (results.next()) {
                rows += s"${results.getString("firstname")},${results.getString("lastname")},${results
                  .getDate("birthday")}"
              }
              rows.toList.mkString(";")
            }
            val expectedData =
              """null,null,1879-03-14;null,null,1826-09-17;null,null,1777-04-30;null,null,1808-07-25;null,null,1646-07-01"""

            withClue(s"The database table should have the proper content!")(
              actualData should be(expectedData)
            )

            val dst = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:emptyColumns2")
            dst.createStatement().execute("SHUTDOWN")
            dst.close()
          }
        }
      }
    }
  }
}
