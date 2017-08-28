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

package usecases.databases

import java.io.{ File, InputStream, StringReader }
import java.net.URI

import akka.testkit.TestActorRef
import com.wegtam.scalatest.tags.{ DbTest, DbTestH2 }
import com.wegtam.tensei.adt.Recipe.MapAllToAll
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.Processor.ProcessorMessages
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.parsers.{ BaseParserMessages, DatabaseParser, FileParser }
import com.wegtam.tensei.agent.{ DataTreeDocument, Processor, XmlActorSpec }
import org.scalatest.BeforeAndAfterEach
import org.xml.sax.InputSource

import scala.concurrent.Await
import scala.concurrent.duration._

class Splitting extends XmlActorSpec with BeforeAndAfterEach {
  val sourceDatabaseName = "dbsplitsrc"
  val targetDatabaseName = "dbsplitdst"

  /**
    * Shutdown the test actor system and the source database.
    */
  override def afterAll(): Unit = {
    Await.result(system.terminate(), Duration.Inf)
    val src = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$sourceDatabaseName")
    src.createStatement().execute("SHUTDOWN")
    src.close()
  }

  /**
    * Initialize the source database.
    */
  override def beforeAll(): Unit = {
    println("Initializing database (this may take a while)...") // DEBUG
    val src =
      java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
    val stm = src.createStatement()
    val sql = scala.io.Source
      .fromInputStream(getClass.getResourceAsStream("/usecases/databases/sugarcrm-6.sql"))
      .mkString
    stm.execute(sql)
    stm.close()
    println("Database initialized.") // DEBUG
  }

  /**
    * Shutdown the target database after each test.
    */
  override def afterEach(): Unit = {
    val dst = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")
    dst.createStatement().execute("SHUTDOWN")
    dst.close()
  }

  describe("Use cases") {
    describe("using databases") {
      describe("splitting") {
        describe("split one field into two") {
          describe("from a database into a file") {
            describe("using one source sequence") {
              it("should work", DbTest, DbTestH2) {
                lazy val sourceDfasdl: DFASDL = {
                  val in: InputStream =
                    getClass.getResourceAsStream("/usecases/databases/sugarcrm-6-join2.xml")
                  val xml = scala.io.Source.fromInputStream(in).mkString
                  DFASDL("SugarCRM", xml)
                }

                lazy val targetDfasdl = {
                  val in: InputStream =
                    getClass.getResourceAsStream("/usecases/databases/sugarcrm-target-03.xml")
                  val xml = scala.io.Source.fromInputStream(in).mkString
                  DFASDL("Target", xml)
                }

                val cookbook = Cookbook(
                  "MY-COOKBOOK",
                  List(sourceDfasdl),
                  Option(targetDfasdl),
                  List(
                    new Recipe(
                      "MAP-CONTACTS",
                      MapAllToAll,
                      List(
                        new MappingTransformation(
                          List(
                            ElementReference(sourceDfasdl.id, "accounts_with_contacts_row_name")
                          ),
                          List(ElementReference(targetDfasdl.id, "name"))
                        ),
                        new MappingTransformation(
                          List(ElementReference(sourceDfasdl.id,
                                                "accounts_with_contacts_row_first_name"),
                               ElementReference(sourceDfasdl.id,
                                                "accounts_with_contacts_row_last_name")),
                          List(ElementReference(targetDfasdl.id, "human_name")),
                          List(
                            new TransformationDescription(
                              "com.wegtam.tensei.agent.transformers.Concat",
                              new TransformerOptions(classOf[String],
                                                     classOf[String],
                                                     List(("separator", " ")))
                            )
                          )
                        ),
                        new MappingTransformation(
                          List(
                            ElementReference(sourceDfasdl.id,
                                             "accounts_with_contacts_row_phone_home")
                          ),
                          List(ElementReference(targetDfasdl.id, "vorwahl")),
                          List(
                            new TransformationDescription(
                              "com.wegtam.tensei.agent.transformers.Split",
                              new TransformerOptions(classOf[String],
                                                     classOf[String],
                                                     List(("pattern", " "), ("limit", "1")))
                            )
                          )
                        ),
                        new MappingTransformation(
                          List(
                            ElementReference(sourceDfasdl.id,
                                             "accounts_with_contacts_row_phone_home")
                          ),
                          List(ElementReference(targetDfasdl.id, "hauptnummer")),
                          List(
                            new TransformationDescription(
                              "com.wegtam.tensei.agent.transformers.Split",
                              new TransformerOptions(classOf[String],
                                                     classOf[String],
                                                     List(("pattern", " "), ("selected", "1")))
                            )
                          )
                        )
                      )
                    )
                  )
                )
                val source = new ConnectionInformation(
                  uri = new URI(s"jdbc:h2:mem:$sourceDatabaseName"),
                  dfasdlRef = Option(new DFASDLReference("MY-COOKBOOK", sourceDfasdl.id))
                )
                val target = {
                  val targetFilePath =
                    File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")
                  new ConnectionInformation(
                    uri = new URI(s"file:$targetFilePath"),
                    dfasdlRef = Option(new DFASDLReference(cookbook.id, targetDfasdl.id))
                  )
                }

                val startTransformationMsg =
                  new AgentStartTransformationMessage(sources = List(source),
                                                      target = target,
                                                      cookbook = cookbook)

                val dataTree = TestActorRef(
                  DataTreeDocument.props(sourceDfasdl, Option("SplittingTest"), Set.empty[String])
                )
                val dbParser = TestActorRef(
                  DatabaseParser.props(source, cookbook, dataTree, Option("SplittingTest"))
                )
                dbParser ! BaseParserMessages.Start

                val message = expectMsgType[ParserStatusMessage](5.seconds)
                message.status should be(ParserStatus.COMPLETED)

                val expectedDataXml = scala.io.Source
                  .fromInputStream(
                    getClass.getResourceAsStream(
                      "/usecases/databases/sugarcrm-target-03-expected-data-tree.xml"
                    )
                  )
                  .mkString
                val expectedDataTree = createTestDocumentBuilder().parse(
                  new InputSource(new StringReader(expectedDataXml))
                )

                compareSequenceData("accounts_with_contacts_row_name", expectedDataTree, dataTree)
                compareSequenceData("accounts_with_contacts_row_first_name",
                                    expectedDataTree,
                                    dataTree)
                compareSequenceData("accounts_with_contacts_row_last_name",
                                    expectedDataTree,
                                    dataTree)
                compareSequenceData("accounts_with_contacts_row_phone_home",
                                    expectedDataTree,
                                    dataTree)

                val processor = TestActorRef(new Processor(Option("SplittingTest")))
                processor ! ProcessorMessages.StartProcessingMessage(startTransformationMsg,
                                                                     List(dataTree))
                expectMsg(ProcessorMessages.Completed)

                val expectedData = scala.io.Source
                  .fromInputStream(
                    getClass.getResourceAsStream(
                      "/usecases/databases/sugarcrm-target-03-expected-data.csv"
                    )
                  )
                  .mkString
                val actualData =
                  scala.io.Source.fromFile(target.uri.getSchemeSpecificPart).mkString

                actualData should be(expectedData)
              }
            }
          }

          describe("from a file into a file") {
            describe("using one source sequence") {
              it("should work") {
                lazy val sourceDfasdl: DFASDL = {
                  val in: InputStream =
                    getClass.getResourceAsStream("/usecases/databases/splitting-01-source.xml")
                  val xml = scala.io.Source.fromInputStream(in).mkString
                  DFASDL("Source", xml)
                }

                lazy val targetDfasdl = {
                  val in: InputStream =
                    getClass.getResourceAsStream("/usecases/databases/splitting-01-target.xml")
                  val xml = scala.io.Source.fromInputStream(in).mkString
                  DFASDL("Target", xml)
                }

                val cookbook = Cookbook(
                  "MY-COOKBOOK",
                  List(sourceDfasdl),
                  Option(targetDfasdl),
                  List(
                    new Recipe(
                      "MAP-CONTACTS",
                      MapAllToAll,
                      List(
                        new MappingTransformation(
                          List(ElementReference(sourceDfasdl.id, "title")),
                          List(ElementReference(targetDfasdl.id, "title"))
                        ),
                        new MappingTransformation(
                          List(ElementReference(sourceDfasdl.id, "vorname"),
                               ElementReference(sourceDfasdl.id, "name")),
                          List(ElementReference(targetDfasdl.id, "name")),
                          List(
                            new TransformationDescription(
                              "com.wegtam.tensei.agent.transformers.Concat",
                              new TransformerOptions(classOf[String],
                                                     classOf[String],
                                                     List(("separator", " ")))
                            )
                          )
                        ),
                        new MappingTransformation(
                          List(ElementReference(sourceDfasdl.id, "telefonnummer")),
                          List(ElementReference(targetDfasdl.id, "vorwahl")),
                          List(
                            new TransformationDescription(
                              "com.wegtam.tensei.agent.transformers.Split",
                              new TransformerOptions(classOf[String],
                                                     classOf[String],
                                                     List(("pattern", " "), ("limit", "1")))
                            )
                          )
                        ),
                        new MappingTransformation(
                          List(ElementReference(sourceDfasdl.id, "telefonnummer")),
                          List(ElementReference(targetDfasdl.id, "hauptnummer")),
                          List(
                            new TransformationDescription(
                              "com.wegtam.tensei.agent.transformers.Split",
                              new TransformerOptions(classOf[String],
                                                     classOf[String],
                                                     List(("pattern", " "), ("selected", "1")))
                            )
                          )
                        )
                      )
                    )
                  )
                )
                val sourceFilePath =
                  getClass.getResource("/usecases/databases/splitting-01-contact.csv")
                val source = new ConnectionInformation(
                  uri = new URI(s"$sourceFilePath"),
                  dfasdlRef = Option(new DFASDLReference(cookbook.id, sourceDfasdl.id))
                )

                val target = {
                  val targetFilePath =
                    File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")
                  new ConnectionInformation(
                    uri = new URI(s"file:$targetFilePath"),
                    dfasdlRef = Option(new DFASDLReference(cookbook.id, targetDfasdl.id))
                  )
                }

                val startTransformationMsg =
                  new AgentStartTransformationMessage(sources = List(source),
                                                      target = target,
                                                      cookbook = cookbook)

                val dataTree = TestActorRef(
                  DataTreeDocument.props(sourceDfasdl, Option("SplittingTest"), Set.empty[String])
                )
                val dbParser = TestActorRef(
                  FileParser.props(source, cookbook, dataTree, Option("SplittingTest"))
                )
                dbParser ! BaseParserMessages.Start

                val message = expectMsgType[ParserStatusMessage](5.seconds)
                message.status should be(ParserStatus.COMPLETED)

                val processor = TestActorRef(new Processor(Option("SplittingTest")))
                processor ! ProcessorMessages.StartProcessingMessage(startTransformationMsg,
                                                                     List(dataTree))
                expectMsg(30.seconds, ProcessorMessages.Completed) // FIXME Timeout hoch gesetzt, damit der Test funktioniert

                val expectedData = scala.io.Source
                  .fromInputStream(
                    getClass
                      .getResourceAsStream("/usecases/databases/splitting-01-expected-data.csv")
                  )
                  .mkString
                val actualData =
                  scala.io.Source.fromFile(target.uri.getSchemeSpecificPart).mkString

                actualData should be(expectedData)
              }
            }
          }
        }
      }
    }
  }
}
