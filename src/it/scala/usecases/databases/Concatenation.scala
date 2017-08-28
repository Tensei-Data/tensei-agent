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

import akka.testkit.{ TestActorRef, TestFSMRef }
import com.wegtam.scalatest.tags.{ DbTest, DbTestH2 }
import com.wegtam.tensei.adt.Recipe.MapAllToAll
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.Processor.ProcessorMessages.{ Completed, StartProcessingMessage }
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.parsers.{ BaseParserMessages, DatabaseParser }
import com.wegtam.tensei.agent.{ DataTreeDocument, Processor, ProcessorState, XmlActorSpec }
import org.scalatest.BeforeAndAfterEach
import org.xml.sax.InputSource

import scala.concurrent.Await
import scala.concurrent.duration._

class Concatenation extends XmlActorSpec with BeforeAndAfterEach {
  val sourceDatabaseName = "dbconcatsrc"

  val targetDatabaseName = "dbconcatdst"

  lazy val sourceDatabaseConnection =
    java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$sourceDatabaseName")

  /**
    * Shutdown the test actor system and the source database.
    */
  override def afterAll(): Unit = {
    Await.result(system.terminate(), Duration.Inf)
    sourceDatabaseConnection.createStatement().execute("SHUTDOWN")
    sourceDatabaseConnection.close()
  }

  /**
    * Initialize the source database.
    */
  override def beforeAll(): Unit = {
    println("Initializing database (this may take a while)...")
    val stm = sourceDatabaseConnection.createStatement()
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
      describe("concatenation") {
        describe("concatenate two fields into one") {
          describe("from a database into a file") {
            describe("using one source sequence") {
              describe(s"with recipe mode $MapAllToAll") {
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
                        "MAP-CONTACTS",
                        MapAllToAll,
                        List(
                          new MappingTransformation(
                            List(ElementReference(sourceDfasdl.id, "contacts_row_id")),
                            List(ElementReference(targetDfasdl.id, "id"))
                          ),
                          new MappingTransformation(
                            List(ElementReference(sourceDfasdl.id, "contacts_row_date_entered")),
                            List(ElementReference(targetDfasdl.id, "date_entered"))
                          ),
                          new MappingTransformation(
                            List(ElementReference(sourceDfasdl.id, "contacts_row_description")),
                            List(ElementReference(targetDfasdl.id, "description"))
                          ),
                          new MappingTransformation(
                            List(ElementReference(sourceDfasdl.id, "contacts_row_first_name"),
                                 ElementReference(sourceDfasdl.id, "contacts_row_last_name")),
                            List(ElementReference(targetDfasdl.id, "name")),
                            List(
                              new TransformationDescription(
                                "com.wegtam.tensei.agent.transformers.Concat",
                                new TransformerOptions(classOf[String],
                                                       classOf[String],
                                                       List(("separator", " ")))
                              )
                            )
                          )
                        )
                      )
                    )
                  )
                  val source = new ConnectionInformation(
                    uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
                    dfasdlRef = Option(new DFASDLReference("MY-COOKBOOK", sourceDfasdl.id))
                  )
                  val target = {
                    val targetFilePath = File
                      .createTempFile("xmlActorTest", "test")
                      .getAbsolutePath
                      .replace("\\", "/")
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
                    DataTreeDocument.props(sourceDfasdl,
                                           Option("ConcatenationTest"),
                                           Set.empty[String])
                  )
                  val dbParser = TestActorRef(
                    DatabaseParser.props(source, cookbook, dataTree, Option("ConcatenationTest"))
                  )
                  dbParser ! BaseParserMessages.Start

                  val message = expectMsgType[ParserStatusMessage](5.seconds)
                  message.status should be(ParserStatus.COMPLETED)

                  val expectedDataXml = scala.io.Source
                    .fromInputStream(
                      getClass.getResourceAsStream(
                        "/usecases/databases/concatenation-01-expected-data.xml"
                      )
                    )
                    .mkString
                  val expectedDataTree = createTestDocumentBuilder(useSchema = false).parse(
                    new InputSource(new StringReader(expectedDataXml))
                  ) // FIXME Workaround for invalid datetime format.

                  val idsToCheck = List(
                    "accounts_bugs_row_account_id",
                    "accounts_bugs_row_bug_id",
                    "accounts_bugs_row_date_modified",
                    "accounts_bugs_row_deleted",
                    "accounts_bugs_row_id",
                    "accounts_contacts_row_account_id",
                    "accounts_contacts_row_contact_id",
                    "accounts_contacts_row_date_modified",
                    "accounts_contacts_row_deleted",
                    "accounts_contacts_row_id",
                    "accounts_row_account_type",
                    "accounts_row_annual_revenue",
                    "accounts_row_assigned_user_id",
                    "accounts_row_billing_address_city",
                    "accounts_row_billing_address_country",
                    "accounts_row_billing_address_postalcode",
                    "accounts_row_billing_address_state",
                    "accounts_row_billing_address_street",
                    "accounts_row_campaign_id",
                    "accounts_row_created_by",
                    "accounts_row_date_entered",
                    "accounts_row_date_modified",
                    "accounts_row_deleted",
                    "accounts_row_description",
                    "accounts_row_employees",
                    "accounts_row_id",
                    "accounts_row_industry",
                    "accounts_row_modified_user_id",
                    "accounts_row_name",
                    "accounts_row_ownership",
                    "accounts_row_parent_id",
                    "accounts_row_phone_alternate",
                    "accounts_row_phone_fax",
                    "accounts_row_phone_office",
                    "accounts_row_rating",
                    "accounts_row_shipping_address_city",
                    "accounts_row_shipping_address_country",
                    "accounts_row_shipping_address_postalcode",
                    "accounts_row_shipping_address_state",
                    "accounts_row_shipping_address_street",
                    "accounts_row_sic_code",
                    "accounts_row_ticker_symbol",
                    "accounts_row_website",
                    "contacts_row_alt_address_city",
                    "contacts_row_alt_address_country",
                    "contacts_row_alt_address_postalcode",
                    "contacts_row_alt_address_state",
                    "contacts_row_alt_address_street",
                    "contacts_row_assigned_user_id",
                    "contacts_row_assistant",
                    "contacts_row_assistant_phone",
                    "contacts_row_birthdate",
                    "contacts_row_campaign_id",
                    "contacts_row_created_by",
                    "contacts_row_date_entered",
                    "contacts_row_date_modified",
                    "contacts_row_deleted",
                    "contacts_row_department",
                    "contacts_row_description",
                    "contacts_row_do_not_call",
                    "contacts_row_first_name",
                    "contacts_row_id",
                    "contacts_row_last_name",
                    "contacts_row_lead_source",
                    "contacts_row_modified_user_id",
                    "contacts_row_phone_fax",
                    "contacts_row_phone_home",
                    "contacts_row_phone_mobile",
                    "contacts_row_phone_other",
                    "contacts_row_phone_work",
                    "contacts_row_primary_address_city",
                    "contacts_row_primary_address_country",
                    "contacts_row_primary_address_postalcode",
                    "contacts_row_primary_address_state",
                    "contacts_row_primary_address_street",
                    "contacts_row_reports_to_id",
                    "contacts_row_salutation",
                    "contacts_row_title"
                  )
                  idsToCheck.foreach(id => compareSequenceData(id, expectedDataTree, dataTree))

                  val processor = TestFSMRef(new Processor(Option("ConcatenationTest")))
                  processor.stateName should be(ProcessorState.Idle)

                  processor ! StartProcessingMessage(startTransformationMsg, List(dataTree))
                  expectMsg(FiniteDuration(90, SECONDS), Completed)

                  val expectedData = scala.io.Source
                    .fromInputStream(
                      getClass.getResourceAsStream(
                        "/usecases/databases/sugarcrm-target-01-concat-expected-data.csv"
                      )
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
}
