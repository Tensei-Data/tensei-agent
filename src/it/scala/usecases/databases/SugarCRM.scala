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
import com.wegtam.tensei.adt.Recipe.MapOneToOne
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.Processor.ProcessorMessages.{ Completed, StartProcessingMessage }
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.parsers.{ BaseParserMessages, DatabaseParser }
import com.wegtam.tensei.agent.{ DataTreeDocument, Processor, XmlActorSpec }
import org.scalatest.BeforeAndAfterEach
import org.xml.sax.InputSource

import scala.concurrent.Await
import scala.concurrent.duration._

class SugarCRM extends XmlActorSpec with BeforeAndAfterEach {
  val sourceDatabaseName = "sugarcrmsrc"

  val targetDatabaseName = "sugarcrmdst"

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
    println("Initializing database (this may take a while)...")
    val removeSrc = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$sourceDatabaseName")
    removeSrc.createStatement().execute("SHUTDOWN")
    removeSrc.close()
    val src =
      java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
    val stm = src.createStatement()
    val sql = scala.io.Source
      .fromInputStream(getClass.getResourceAsStream("/usecases/databases/sugarcrm-6.sql"))
      .mkString
    stm.execute(sql)
    stm.close()
    println("Database initialized.")
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
      describe("SugarCRM") {
        describe("using some real world data") {
          describe("copying parts of the accounts table") {
            it("should work", DbTest, DbTestH2) {
              lazy val sourceDfasdl: DFASDL = {
                val in: InputStream =
                  getClass.getResourceAsStream("/usecases/databases/sugarcrm-6.xml")
                val xml = scala.io.Source.fromInputStream(in).mkString
                DFASDL("SugarCRM", xml)
              }

              lazy val targetDfasdl = {
                val in: InputStream =
                  getClass.getResourceAsStream("/usecases/databases/sugarcrm-target-01.xml")
                val xml = scala.io.Source.fromInputStream(in).mkString
                DFASDL("Target", xml)
              }

              val cookbook = Cookbook(
                "MY-COOKBOOK",
                List(sourceDfasdl),
                Option(targetDfasdl),
                List(
                  new Recipe(
                    "MAP-ACCOUNTS",
                    MapOneToOne,
                    List(
                      new MappingTransformation(
                        createElementReferenceList(sourceDfasdl,
                                                   List("accounts_row_id",
                                                        "accounts_row_name",
                                                        "accounts_row_date_entered",
                                                        "accounts_row_description")),
                        createElementReferenceList(targetDfasdl,
                                                   List("id",
                                                        "name",
                                                        "date_entered",
                                                        "description"))
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

              val startTransformationMsg = new AgentStartTransformationMessage(sources =
                                                                                 List(source),
                                                                               target = target,
                                                                               cookbook = cookbook)

              val dataTree = TestActorRef(
                DataTreeDocument.props(sourceDfasdl, Option("SugarCRMTest"), Set.empty[String])
              )
              val dbParser = TestActorRef(
                DatabaseParser.props(source, cookbook, dataTree, Option("SugarCRMTest"))
              )
              dbParser ! BaseParserMessages.Start

              val message = expectMsgType[ParserStatusMessage](10.seconds)
              message.status should be(ParserStatus.COMPLETED)

              val processor = TestActorRef(new Processor(Option("SugarCRMTest")))
              processor ! new StartProcessingMessage(startTransformationMsg, List(dataTree))
              expectMsg(30.seconds, Completed)

              val expectedData = scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream(
                    "/usecases/databases/sugarcrm-target-01-expected-data.csv"
                  )
                )
                .mkString
              val actualData = scala.io.Source.fromFile(target.uri.getSchemeSpecificPart).mkString

              actualData should be(expectedData)
            }
          }

          describe("using data from a join command") {
            it("should work", DbTest, DbTestH2) {
              lazy val sourceDfasdl: DFASDL = {
                val in: InputStream =
                  getClass.getResourceAsStream("/usecases/databases/sugarcrm-6-join.xml")
                val xml = scala.io.Source.fromInputStream(in).mkString
                DFASDL("SugarCRM", xml)
              }

              lazy val targetDfasdl = {
                val in: InputStream =
                  getClass.getResourceAsStream("/usecases/databases/sugarcrm-target-02.xml")
                val xml = scala.io.Source.fromInputStream(in).mkString
                DFASDL("Target", xml)
              }

              val cookbook = Cookbook(
                "MY-COOKBOOK",
                List(sourceDfasdl),
                Option(targetDfasdl),
                List(
                  new Recipe(
                    "MAP-ACCOUNTS",
                    MapOneToOne,
                    List(
                      new MappingTransformation(
                        createElementReferenceList(
                          sourceDfasdl,
                          List(
                            "accounts_with_contacts_row_first_name",
                            "accounts_with_contacts_row_last_name",
                            "accounts_with_contacts_row_name",
                            "accounts_with_contacts_row_birthdate"
                          )
                        ),
                        createElementReferenceList(targetDfasdl,
                                                   List("name",
                                                        "surname",
                                                        "companyname",
                                                        "birthdate"))
                      )
                    )
                  )
                )
              )
              val source = new ConnectionInformation(
                uri = new URI(s"jdbc:h2:mem:$sourceDatabaseName"),
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

              val startTransformationMsg = new AgentStartTransformationMessage(sources =
                                                                                 List(source),
                                                                               target = target,
                                                                               cookbook = cookbook)

              val dataTree = TestActorRef(
                DataTreeDocument.props(sourceDfasdl, Option("SugarCRMTest"), Set.empty[String])
              )
              val dbParser = TestActorRef(
                DatabaseParser.props(source, cookbook, dataTree, Option("SugarCRMTest"))
              )
              dbParser ! BaseParserMessages.Start

              val message = expectMsgType[ParserStatusMessage](5.seconds)
              message.status should be(ParserStatus.COMPLETED)

              val expectedDataXml = scala.io.Source
                .fromInputStream(
                  getClass
                    .getResourceAsStream("/usecases/databases/sugarcrm-6-join-expected-data.xml")
                )
                .mkString
              val expectedDataTree = createTestDocumentBuilder(useSchema = false)
                .parse(new InputSource(new StringReader(expectedDataXml)))

              List(
                "accounts_with_contacts_row_name",
                "accounts_with_contacts_row_account_type",
                "accounts_with_contacts_row_salutation",
                "accounts_with_contacts_row_first_name",
                "accounts_with_contacts_row_last_name",
                "accounts_with_contacts_row_birthdate"
              ).foreach { id =>
                compareSequenceData(id, expectedDataTree, dataTree)
              }

              val processor = TestActorRef(new Processor(Option("SugarCRMTest")))
              processor ! new StartProcessingMessage(startTransformationMsg, List(dataTree))
              expectMsg(Completed)

              val expectedData = scala.io.Source
                .fromInputStream(
                  getClass
                    .getResourceAsStream("/usecases/databases/sugarcrm-6-join-expected-data.csv")
                )
                .mkString
              val actualData = scala.io.Source.fromFile(target.uri.getSchemeSpecificPart).mkString

              actualData should be(expectedData)
            }
          }

          describe("using data from a join command and store the data into another database") {
            it("should work", DbTest, DbTestH2) {
              lazy val sourceDfasdl: DFASDL = {
                val in: InputStream =
                  getClass.getResourceAsStream("/usecases/databases/sugarcrm-6-join.xml")
                val xml = scala.io.Source.fromInputStream(in).mkString
                DFASDL("SugarCRM", xml)
              }

              lazy val targetDfasdl = {
                val in: InputStream =
                  getClass.getResourceAsStream("/usecases/databases/sugarcrm-target-02.xml")
                val xml = scala.io.Source.fromInputStream(in).mkString
                DFASDL("Target", xml)
              }

              val cookbook = Cookbook(
                "MY-COOKBOOK",
                List(sourceDfasdl),
                Option(targetDfasdl),
                List(
                  new Recipe(
                    "MAP-ACCOUNTS",
                    MapOneToOne,
                    List(
                      new MappingTransformation(
                        createElementReferenceList(
                          sourceDfasdl,
                          List(
                            "accounts_with_contacts_row_first_name",
                            "accounts_with_contacts_row_last_name",
                            "accounts_with_contacts_row_name",
                            "accounts_with_contacts_row_birthdate"
                          )
                        ),
                        createElementReferenceList(targetDfasdl,
                                                   List("name",
                                                        "surname",
                                                        "companyname",
                                                        "birthdate"))
                      )
                    )
                  )
                )
              )
              val source = new ConnectionInformation(
                uri = new URI(s"jdbc:h2:mem:$sourceDatabaseName"),
                dfasdlRef = Option(new DFASDLReference(cookbook.id, sourceDfasdl.id))
              )
              val target = new ConnectionInformation(
                uri = new URI(s"jdbc:h2:mem:$targetDatabaseName;DB_CLOSE_DELAY=-1"),
                dfasdlRef = Option(new DFASDLReference(cookbook.id, targetDfasdl.id))
              )

              val startTransformationMsg = new AgentStartTransformationMessage(sources =
                                                                                 List(source),
                                                                               target = target,
                                                                               cookbook = cookbook)

              val dataTree = TestActorRef(
                DataTreeDocument.props(sourceDfasdl, Option("SugarCRMTest"), Set.empty[String])
              )
              val dbParser = TestActorRef(
                DatabaseParser.props(source, cookbook, dataTree, Option("SugarCRMTest"))
              )
              dbParser ! BaseParserMessages.Start

              val message = expectMsgType[ParserStatusMessage](5.seconds)
              message.status should be(ParserStatus.COMPLETED)

              val expectedDataXml = scala.io.Source
                .fromInputStream(
                  getClass
                    .getResourceAsStream("/usecases/databases/sugarcrm-6-join-expected-data.xml")
                )
                .mkString
              val expectedDataTree = createTestDocumentBuilder(useSchema = false)
                .parse(new InputSource(new StringReader(expectedDataXml)))

              List(
                "accounts_with_contacts_row_name",
                "accounts_with_contacts_row_account_type",
                "accounts_with_contacts_row_salutation",
                "accounts_with_contacts_row_first_name",
                "accounts_with_contacts_row_last_name",
                "accounts_with_contacts_row_birthdate"
              ).foreach { id =>
                compareSequenceData(id, expectedDataTree, dataTree)
              }

              val processor = TestActorRef(new Processor(Option("SugarCRMTest")))
              processor ! new StartProcessingMessage(startTransformationMsg, List(dataTree))
              expectMsg(Completed)

              val connection = java.sql.DriverManager.getConnection(target.uri.toString)
              val statement  = connection.createStatement()
              val results    = statement.executeQuery("SELECT * FROM ACCOUNTS")
              withClue("Data should have been written to the database!") {
                results.next() should be(true)
                results.getString("name") should be("Hilary")
                results.getString("surname") should be("Shealy")
                results.getString("companyname") should be("Constrata Trust LLC")
                results.getDate("birthdate") should be(null)
                results.next() should be(true)
                results.getString("name") should be("Deidra")
                results.getString("surname") should be("Salem")
                results.getString("companyname") should be("Tracker Com LP")
                results.getDate("birthdate") should be(null)
                results.next() should be(true)
                results.getString("name") should be("Everett")
                results.getString("surname") should be("Osteen")
                results.getString("companyname") should be("Rhyme & Reason Inc")
                results.getDate("birthdate") should be(null)
                results.next() should be(false)
              }
            }
          }
        }
      }
    }
  }
}
