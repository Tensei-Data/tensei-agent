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

import java.io.InputStream
import java.net.URI

import akka.testkit.{ TestActorRef, TestFSMRef }
import com.wegtam.scalatest.tags.{ DbTest, DbTestH2 }
import com.wegtam.tensei.adt.Recipe.{ MapAllToAll, MapOneToOne }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.Processor.ProcessorMessages.{ Completed, StartProcessingMessage }
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.parsers.{ BaseParserMessages, DatabaseParser }
import com.wegtam.tensei.agent.{ DataTreeDocument, Processor, ProcessorState, XmlActorSpec }
import org.scalatest.BeforeAndAfterEach

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Test different scenarios which should be expressable in both modes:
  * `MapOneToOne` and `MapAllToAll`
  */
class MappingModeAssociation extends XmlActorSpec with BeforeAndAfterEach {

  val sourceDatabaseName = "dbassociationsrc"

  val targetDatabaseName = "dbassociationt"

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
    //println("Initializing database (this may take a while)...")
    val stm = sourceDatabaseConnection.createStatement()
    val sql = scala.io.Source
      .fromInputStream(getClass.getResourceAsStream("/usecases/databases/associationdb.sql"))
      .mkString
    stm.execute(sql)
    stm.close()
    //println("Database initialized.")
  }

  /**
    * Shutdown the target database after each test.
    */
  override def afterEach(): Unit = {
    val dst = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")
    dst.createStatement().execute("SHUTDOWN")
    dst.close()
  }

  describe("MappingModeAssociation") {
    describe("by mapping all elements from the source into all elements in the target") {

      describe("in the correct mapping order") {
        it("should migrate correctly", DbTest, DbTestH2) {
          lazy val sourceDfasdl: DFASDL = {
            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/association.xml")
            val xml = scala.io.Source.fromInputStream(in).mkString
            DFASDL("Association", xml)
          }

          lazy val targetDfasdl = {
            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/association-target.xml")
            val xml = scala.io.Source.fromInputStream(in).mkString
            DFASDL("Target", xml)
          }

          val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

          // Map-One-To-One
          val recipe_one_to_one = new Recipe(
            "Map-OTO",
            MapOneToOne,
            List(
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_id"),
                  ElementReference(sourceDfasdl.id, "accounts_row_name"),
                  ElementReference(sourceDfasdl.id, "accounts_row_vorname"),
                  ElementReference(sourceDfasdl.id, "accounts_row_date_entered"),
                  ElementReference(sourceDfasdl.id, "accounts_row_birthday"),
                  ElementReference(sourceDfasdl.id, "accounts_row_description"),
                  ElementReference(sourceDfasdl.id, "accounts_row_deleted"),
                  ElementReference(sourceDfasdl.id, "accounts_row_phone_office"),
                  ElementReference(sourceDfasdl.id, "accounts_row_website")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_id"),
                  ElementReference(targetDfasdl.id, "accounts_row_name"),
                  ElementReference(targetDfasdl.id, "accounts_row_vorname"),
                  ElementReference(targetDfasdl.id, "accounts_row_date_entered"),
                  ElementReference(targetDfasdl.id, "accounts_row_birthday"),
                  ElementReference(targetDfasdl.id, "accounts_row_description"),
                  ElementReference(targetDfasdl.id, "accounts_row_deleted"),
                  ElementReference(targetDfasdl.id, "accounts_row_phone_office"),
                  ElementReference(targetDfasdl.id, "accounts_row_website")
                )
              )
            )
          )

          // Map-All-To-All
          val recipe_all_to_all = new Recipe(
            "Map-ATA",
            MapAllToAll,
            List(
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_id")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_id"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_name")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_name"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_vorname")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_vorname"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_date_entered")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_date_entered"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_birthday")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_birthday"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_description")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_description"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_deleted")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_deleted"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_phone_office")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_phone_office"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_website")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_website"))
              )
            )
          )

          val cookbook = Cookbook("MY-COOKBOOK",
                                  List(sourceDfasdl),
                                  Option(targetDfasdl),
                                  List(recipe_one_to_one, recipe_all_to_all))

          val sourceConnection = new ConnectionInformation(
            uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
            dfasdlRef = Option(new DFASDLReference(cookbook.id, sourceDfasdl.id))
          )
          val targetConnection = new ConnectionInformation(
            uri = new URI(targetDb.getMetaData.getURL),
            dfasdlRef = Option(new DFASDLReference(cookbook.id, targetDfasdl.id))
          )

          val startTransformationMsg = new AgentStartTransformationMessage(
            sources = List(sourceConnection),
            target = targetConnection,
            cookbook = cookbook
          )

          val dataTree = TestActorRef(
            DataTreeDocument.props(sourceDfasdl, Option("AssociationTest"), Set.empty[String])
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(sourceConnection, cookbook, dataTree, Option("AssociationTest"))
          )
          dbParser ! BaseParserMessages.Start

          val message = expectMsgType[ParserStatusMessage](5.seconds)
          message.status should be(ParserStatus.COMPLETED)

          val processor = TestFSMRef(new Processor(Option("AssociationTest")))
          processor.stateName should be(ProcessorState.Idle)

          processor ! StartProcessingMessage(startTransformationMsg, List(dataTree))
          expectMsg(FiniteDuration(90, SECONDS), Completed)

          val sourceData   = new mutable.HashMap[Long, List[String]]()
          val statement    = sourceDatabaseConnection.createStatement()
          val expectedData = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id;")
          while (expectedData.next()) {
            val id = expectedData.getInt("id")
            val otherData = List[String](
              id.toString,
              expectedData.getString("name"),
              expectedData.getString("vorname"),
              expectedData.getTimestamp("date_entered").toString,
              expectedData.getDate("birthday").toString,
              expectedData.getString("description"),
              expectedData.getInt("deleted").toString,
              expectedData.getString("phone_office"),
              expectedData.getString("website")
            )
            sourceData.put(id.toLong, otherData)
          }

          val targetStatement = targetDb.createStatement()
          val one_to_one_data = targetStatement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id;")
          while (one_to_one_data.next()) {
            val sourceLine = sourceData.get(one_to_one_data.getInt("id").toLong)

            withClue("compare `id` in oto") {
              one_to_one_data.getInt("id").toString should be(sourceLine.get.head)
            }
            withClue("compare `name` in oto") {
              one_to_one_data.getString("name") should be(sourceLine.get(1))
            }
            withClue("compare `vorname` in oto") {
              one_to_one_data.getString("vorname") should be(sourceLine.get(2))
            }
            withClue("compare `date_entered` in oto") {
              one_to_one_data.getTimestamp("date_entered").toString should be(sourceLine.get(3))
            }
            withClue("compare `birthday` in oto") {
              one_to_one_data.getDate("birthday").toString should be(sourceLine.get(4))
            }
            withClue("compare `description` in oto") {
              one_to_one_data.getString("description") should be(sourceLine.get(5))
            }
            withClue("compare `deleted` in oto") {
              one_to_one_data.getInt("deleted").toString should be(sourceLine.get(6))
            }
            withClue("compare `phone_office` in oto") {
              one_to_one_data.getString("phone_office") should be(sourceLine.get(7))
            }
            withClue("compare `website` in oto") {
              one_to_one_data.getString("website") should be(sourceLine.get(8))
            }
          }

          val all_to_all_data =
            targetStatement.executeQuery("SELECT * FROM ACCOUNTS_ATA ORDER BY id;")
          while (all_to_all_data.next()) {
            val sourceLine = sourceData.get(all_to_all_data.getInt("id").toLong)

            withClue("compare `id` in ata") {
              all_to_all_data.getInt("id").toString should be(sourceLine.get.head)
            }
            withClue("compare `name` in ata") {
              all_to_all_data.getString("name") should be(sourceLine.get(1))
            }
            withClue("compare `vorname` in ata") {
              all_to_all_data.getString("vorname") should be(sourceLine.get(2))
            }
            withClue("compare `date_entered` in ata") {
              all_to_all_data.getTimestamp("date_entered").toString should be(sourceLine.get(3))
            }
            withClue("compare `birthday` in ata") {
              all_to_all_data.getDate("birthday").toString should be(sourceLine.get(4))
            }
            withClue("compare `description` in ata") {
              all_to_all_data.getString("description") should be(sourceLine.get(5))
            }
            withClue("compare `deleted` in ata") {
              all_to_all_data.getInt("deleted").toString should be(sourceLine.get(6))
            }
            withClue("compare `phone_office` in ata") {
              all_to_all_data.getString("phone_office") should be(sourceLine.get(7))
            }
            withClue("compare `website` in ata") {
              all_to_all_data.getString("website") should be(sourceLine.get(8))
            }
          }
        }
      }

      describe("in random mapping order") {
        it("should migrate correctly", DbTest, DbTestH2) {
          lazy val sourceDfasdl: DFASDL = {
            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/association.xml")
            val xml = scala.io.Source.fromInputStream(in).mkString
            DFASDL("Association", xml)
          }

          lazy val targetDfasdl = {
            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/association-target.xml")
            val xml = scala.io.Source.fromInputStream(in).mkString
            DFASDL("Target", xml)
          }

          val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

          // Map-One-To-One
          val recipe_one_to_one = new Recipe(
            "Map-OTO",
            MapOneToOne,
            List(
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_id"),
                  ElementReference(sourceDfasdl.id, "accounts_row_website"),
                  ElementReference(sourceDfasdl.id, "accounts_row_date_entered"),
                  ElementReference(sourceDfasdl.id, "accounts_row_vorname"),
                  ElementReference(sourceDfasdl.id, "accounts_row_deleted"),
                  ElementReference(sourceDfasdl.id, "accounts_row_phone_office"),
                  ElementReference(sourceDfasdl.id, "accounts_row_birthday"),
                  ElementReference(sourceDfasdl.id, "accounts_row_description"),
                  ElementReference(sourceDfasdl.id, "accounts_row_name")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_id"),
                  ElementReference(targetDfasdl.id, "accounts_row_website"),
                  ElementReference(targetDfasdl.id, "accounts_row_date_entered"),
                  ElementReference(targetDfasdl.id, "accounts_row_vorname"),
                  ElementReference(targetDfasdl.id, "accounts_row_deleted"),
                  ElementReference(targetDfasdl.id, "accounts_row_phone_office"),
                  ElementReference(targetDfasdl.id, "accounts_row_birthday"),
                  ElementReference(targetDfasdl.id, "accounts_row_description"),
                  ElementReference(targetDfasdl.id, "accounts_row_name")
                )
              )
            )
          )

          // Map-All-To-All
          val recipe_all_to_all = new Recipe(
            "Map-ATA",
            MapAllToAll,
            List(
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_id")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_id"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_website")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_website"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_date_entered")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_date_entered"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_vorname")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_vorname"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_deleted")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_deleted"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_phone_office")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_phone_office"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_birthday")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_birthday"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_description")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_description"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_name")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_name"))
              )
            )
          )

          val cookbook = Cookbook("MY-COOKBOOK",
                                  List(sourceDfasdl),
                                  Option(targetDfasdl),
                                  List(recipe_all_to_all, recipe_one_to_one))

          val sourceConnection = new ConnectionInformation(
            uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
            dfasdlRef = Option(new DFASDLReference(cookbook.id, sourceDfasdl.id))
          )
          val targetConnection = new ConnectionInformation(
            uri = new URI(targetDb.getMetaData.getURL),
            dfasdlRef = Option(new DFASDLReference(cookbook.id, targetDfasdl.id))
          )

          val startTransformationMsg = new AgentStartTransformationMessage(
            sources = List(sourceConnection),
            target = targetConnection,
            cookbook = cookbook
          )

          val dataTree = TestActorRef(
            DataTreeDocument.props(sourceDfasdl, Option("AssociationTest"), Set.empty[String])
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(sourceConnection, cookbook, dataTree, Option("AssociationTest"))
          )
          dbParser ! BaseParserMessages.Start

          val message = expectMsgType[ParserStatusMessage](5.seconds)
          message.status should be(ParserStatus.COMPLETED)

          val processor = TestFSMRef(new Processor(Option("AssociationTest")))
          processor.stateName should be(ProcessorState.Idle)

          processor ! StartProcessingMessage(startTransformationMsg, List(dataTree))
          expectMsg(FiniteDuration(90, SECONDS), Completed)

          val sourceData   = new mutable.HashMap[Long, List[String]]()
          val statement    = sourceDatabaseConnection.createStatement()
          val expectedData = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id;")
          while (expectedData.next()) {
            val id = expectedData.getInt("id")
            val otherData = List[String](
              id.toString,
              expectedData.getString("name"),
              expectedData.getString("vorname"),
              expectedData.getTimestamp("date_entered").toString,
              expectedData.getDate("birthday").toString,
              expectedData.getString("description"),
              expectedData.getInt("deleted").toString,
              expectedData.getString("phone_office"),
              expectedData.getString("website")
            )
            sourceData.put(id.toLong, otherData)
          }

          val targetStatement = targetDb.createStatement()
          val one_to_one_data = targetStatement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id;")
          while (one_to_one_data.next()) {
            val sourceLine = sourceData.get(one_to_one_data.getInt("id").toLong)

            withClue("compare `id` in oto") {
              one_to_one_data.getInt("id").toString should be(sourceLine.get.head)
            }
            withClue("compare `name` in oto") {
              one_to_one_data.getString("name") should be(sourceLine.get(1))
            }
            withClue("compare `vorname` in oto") {
              one_to_one_data.getString("vorname") should be(sourceLine.get(2))
            }
            withClue("compare `date_entered` in oto") {
              one_to_one_data.getTimestamp("date_entered").toString should be(sourceLine.get(3))
            }
            withClue("compare `birthday` in oto") {
              one_to_one_data.getDate("birthday").toString should be(sourceLine.get(4))
            }
            withClue("compare `description` in oto") {
              one_to_one_data.getString("description") should be(sourceLine.get(5))
            }
            withClue("compare `deleted` in oto") {
              one_to_one_data.getInt("deleted").toString should be(sourceLine.get(6))
            }
            withClue("compare `phone_office` in oto") {
              one_to_one_data.getString("phone_office") should be(sourceLine.get(7))
            }
            withClue("compare `website` in oto") {
              one_to_one_data.getString("website") should be(sourceLine.get(8))
            }
          }

          val all_to_all_data =
            targetStatement.executeQuery("SELECT * FROM ACCOUNTS_ATA ORDER BY id;")
          while (all_to_all_data.next()) {
            val sourceLine = sourceData.get(all_to_all_data.getInt("id").toLong)

            withClue("compare `id` in ata") {
              all_to_all_data.getInt("id").toString should be(sourceLine.get.head)
            }
            withClue("compare `name` in ata") {
              all_to_all_data.getString("name") should be(sourceLine.get(1))
            }
            withClue("compare `vorname` in ata") {
              all_to_all_data.getString("vorname") should be(sourceLine.get(2))
            }
            withClue("compare `date_entered` in ata") {
              all_to_all_data.getTimestamp("date_entered").toString should be(sourceLine.get(3))
            }
            withClue("compare `birthday` in ata") {
              all_to_all_data.getDate("birthday").toString should be(sourceLine.get(4))
            }
            withClue("compare `description` in ata") {
              all_to_all_data.getString("description") should be(sourceLine.get(5))
            }
            withClue("compare `deleted` in ata") {
              all_to_all_data.getInt("deleted").toString should be(sourceLine.get(6))
            }
            withClue("compare `phone_office` in ata") {
              all_to_all_data.getString("phone_office") should be(sourceLine.get(7))
            }
            withClue("compare `website` in ata") {
              all_to_all_data.getString("website") should be(sourceLine.get(8))
            }
          }
        }
      }
    }
    describe("by mapping not all elements from the source into the target") {
      describe("with the mapped elements in the first mappings") {
        it("should migrate correctly", DbTest, DbTestH2) {
          lazy val sourceDfasdl: DFASDL = {
            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/association.xml")
            val xml = scala.io.Source.fromInputStream(in).mkString
            DFASDL("Association", xml)
          }

          lazy val targetDfasdl = {
            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/association-target.xml")
            val xml = scala.io.Source.fromInputStream(in).mkString
            DFASDL("Target", xml)
          }

          val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

          // Map-One-To-One
          val recipe_one_to_one = new Recipe(
            "Map-OTO",
            MapOneToOne,
            List(
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_name"),
                  ElementReference(sourceDfasdl.id, "accounts_row_birthday"),
                  ElementReference(sourceDfasdl.id, "accounts_row_phone_office")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_name"),
                  ElementReference(targetDfasdl.id, "accounts_row_birthday"),
                  ElementReference(targetDfasdl.id, "accounts_row_phone_office")
                )
              ),
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_id"),
                  ElementReference(sourceDfasdl.id, "accounts_row_vorname"),
                  ElementReference(sourceDfasdl.id, "accounts_row_date_entered"),
                  ElementReference(sourceDfasdl.id, "accounts_row_description"),
                  ElementReference(sourceDfasdl.id, "accounts_row_deleted"),
                  ElementReference(sourceDfasdl.id, "accounts_row_website")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_id"),
                  ElementReference(targetDfasdl.id, "accounts_row_vorname"),
                  ElementReference(targetDfasdl.id, "accounts_row_date_entered"),
                  ElementReference(targetDfasdl.id, "accounts_row_description"),
                  ElementReference(targetDfasdl.id, "accounts_row_deleted"),
                  ElementReference(targetDfasdl.id, "accounts_row_website")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              )
            )
          )

          // Map-All-To-All
          val recipe_all_to_all = new Recipe(
            "Map-ATA",
            MapAllToAll,
            List(
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_name")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_name"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_birthday")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_birthday"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_phone_office")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_phone_office"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_id")),
                List(
                  ElementReference(targetDfasdl.id, "accounts_ata_row_id"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_vorname"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_date_entered"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_description"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_deleted"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_website")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              )
            )
          )

          val cookbook = Cookbook("MY-COOKBOOK",
                                  List(sourceDfasdl),
                                  Option(targetDfasdl),
                                  List(recipe_one_to_one, recipe_all_to_all))

          val sourceConnection = new ConnectionInformation(
            uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
            dfasdlRef = Option(new DFASDLReference(cookbook.id, sourceDfasdl.id))
          )
          val targetConnection = new ConnectionInformation(
            uri = new URI(targetDb.getMetaData.getURL),
            dfasdlRef = Option(new DFASDLReference(cookbook.id, targetDfasdl.id))
          )

          val startTransformationMsg = new AgentStartTransformationMessage(
            sources = List(sourceConnection),
            target = targetConnection,
            cookbook = cookbook
          )

          val dataTree = TestActorRef(
            DataTreeDocument.props(sourceDfasdl, Option("AssociationTest"), Set.empty[String])
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(sourceConnection, cookbook, dataTree, Option("AssociationTest"))
          )
          dbParser ! BaseParserMessages.Start

          val message = expectMsgType[ParserStatusMessage](5.seconds)
          message.status should be(ParserStatus.COMPLETED)

          val processor = TestFSMRef(new Processor(Option("AssociationTest")))
          processor.stateName should be(ProcessorState.Idle)

          processor ! StartProcessingMessage(startTransformationMsg, List(dataTree))
          expectMsg(FiniteDuration(90, SECONDS), Completed)

          val sourceData   = new mutable.HashMap[Long, List[String]]()
          val statement    = sourceDatabaseConnection.createStatement()
          val expectedData = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id;")
          while (expectedData.next()) {
            val id = expectedData.getInt("id")
            val otherData = List[String](
              id.toString,
              expectedData.getString("name"),
              expectedData.getString("vorname"),
              expectedData.getTimestamp("date_entered").toString,
              expectedData.getDate("birthday").toString,
              expectedData.getString("description"),
              expectedData.getInt("deleted").toString,
              expectedData.getString("phone_office"),
              expectedData.getString("website")
            )
            sourceData.put(id.toLong, otherData)
          }

          //println(sourceData)

          val targetStatement = targetDb.createStatement()
          val one_to_one_data = targetStatement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id;")
          var runner          = 1
          while (one_to_one_data.next()) {
            val sourceLine = sourceData.get(runner.toLong)

            withClue("compare `id` in oto") {
              one_to_one_data.getInt("id").toString should be("0")
            }
            withClue("compare `name` in oto") {
              one_to_one_data.getString("name") should be(sourceLine.get(1))
            }
            withClue("compare `vorname` in oto") {
              one_to_one_data.getString("vorname") should be(null)
            }
            withClue("compare `date_entered` in oto") {
              one_to_one_data.getTimestamp("date_entered") should be(null)
            }
            withClue("compare `birthday` in oto") {
              one_to_one_data.getDate("birthday").toString should be(sourceLine.get(4))
            }
            withClue("compare `description` in oto") {
              one_to_one_data.getString("description") should be(null)
            }
            withClue("compare `deleted` in oto") {
              one_to_one_data.getInt("deleted").toString should be("0")
            }
            withClue("compare `phone_office` in oto") {
              one_to_one_data.getString("phone_office") should be(sourceLine.get(7))
            }
            withClue("compare `website` in oto") {
              one_to_one_data.getString("website") should be(null)
            }
            runner += 1
          }

          val all_to_all_data =
            targetStatement.executeQuery("SELECT * FROM ACCOUNTS_ATA ORDER BY id;")
          runner = 1
          while (all_to_all_data.next()) {
            val sourceLine = sourceData.get(runner.toLong)

            withClue("compare `id` in ata") {
              all_to_all_data.getInt("id").toString should be("0")
            }
            withClue("compare `name` in ata") {
              all_to_all_data.getString("name") should be(sourceLine.get(1))
            }
            withClue("compare `vorname` in ata") {
              all_to_all_data.getString("vorname") should be(null)
            }
            withClue("compare `date_entered` in ata") {
              all_to_all_data.getTimestamp("date_entered") should be(null)
            }
            withClue("compare `birthday` in ata") {
              all_to_all_data.getDate("birthday").toString should be(sourceLine.get(4))
            }
            withClue("compare `description` in ata") {
              all_to_all_data.getString("description") should be(null)
            }
            withClue("compare `deleted` in ata") {
              all_to_all_data.getInt("deleted").toString should be("0")
            }
            withClue("compare `phone_office` in ata") {
              all_to_all_data.getString("phone_office") should be(sourceLine.get(7))
            }
            withClue("compare `website` in ata") {
              all_to_all_data.getString("website") should be(null)
            }
            runner += 1
          }
        }
      }
      describe("with the nullified elements in the first mappings") {
        it("should migrate correctly", DbTest, DbTestH2) {
          lazy val sourceDfasdl: DFASDL = {
            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/association.xml")
            val xml = scala.io.Source.fromInputStream(in).mkString
            DFASDL("Association", xml)
          }

          lazy val targetDfasdl = {
            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/association-target.xml")
            val xml = scala.io.Source.fromInputStream(in).mkString
            DFASDL("Target", xml)
          }

          val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

          // Map-One-To-One
          val recipe_one_to_one = new Recipe(
            "Map-OTO",
            MapOneToOne,
            List(
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_id"),
                  ElementReference(sourceDfasdl.id, "accounts_row_vorname"),
                  ElementReference(sourceDfasdl.id, "accounts_row_date_entered"),
                  ElementReference(sourceDfasdl.id, "accounts_row_description"),
                  ElementReference(sourceDfasdl.id, "accounts_row_deleted"),
                  ElementReference(sourceDfasdl.id, "accounts_row_website")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_id"),
                  ElementReference(targetDfasdl.id, "accounts_row_vorname"),
                  ElementReference(targetDfasdl.id, "accounts_row_date_entered"),
                  ElementReference(targetDfasdl.id, "accounts_row_description"),
                  ElementReference(targetDfasdl.id, "accounts_row_deleted"),
                  ElementReference(targetDfasdl.id, "accounts_row_website")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_name"),
                  ElementReference(sourceDfasdl.id, "accounts_row_birthday"),
                  ElementReference(sourceDfasdl.id, "accounts_row_phone_office")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_name"),
                  ElementReference(targetDfasdl.id, "accounts_row_birthday"),
                  ElementReference(targetDfasdl.id, "accounts_row_phone_office")
                )
              )
            )
          )

          // Map-All-To-All
          val recipe_all_to_all = new Recipe(
            "Map-ATA",
            MapAllToAll,
            List(
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_id")),
                List(
                  ElementReference(targetDfasdl.id, "accounts_ata_row_id"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_vorname"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_date_entered"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_description"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_deleted"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_website")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_name")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_name"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_birthday")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_birthday"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_phone_office")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_phone_office"))
              )
            )
          )

          val cookbook = Cookbook("MY-COOKBOOK",
                                  List(sourceDfasdl),
                                  Option(targetDfasdl),
                                  List(recipe_all_to_all, recipe_one_to_one))

          val sourceConnection = new ConnectionInformation(
            uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
            dfasdlRef = Option(new DFASDLReference(cookbook.id, sourceDfasdl.id))
          )
          val targetConnection = new ConnectionInformation(
            uri = new URI(targetDb.getMetaData.getURL),
            dfasdlRef = Option(new DFASDLReference(cookbook.id, targetDfasdl.id))
          )

          val startTransformationMsg = new AgentStartTransformationMessage(
            sources = List(sourceConnection),
            target = targetConnection,
            cookbook = cookbook
          )

          val dataTree = TestActorRef(
            DataTreeDocument.props(sourceDfasdl, Option("AssociationTest"), Set.empty[String])
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(sourceConnection, cookbook, dataTree, Option("AssociationTest"))
          )
          dbParser ! BaseParserMessages.Start

          val message = expectMsgType[ParserStatusMessage](5.seconds)
          message.status should be(ParserStatus.COMPLETED)

          val processor = TestFSMRef(new Processor(Option("AssociationTest")))
          processor.stateName should be(ProcessorState.Idle)

          processor ! StartProcessingMessage(startTransformationMsg, List(dataTree))
          expectMsg(FiniteDuration(90, SECONDS), Completed)

          val sourceData   = new mutable.HashMap[Long, List[String]]()
          val statement    = sourceDatabaseConnection.createStatement()
          val expectedData = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id;")
          while (expectedData.next()) {
            val id = expectedData.getInt("id")
            val otherData = List[String](
              id.toString,
              expectedData.getString("name"),
              expectedData.getString("vorname"),
              expectedData.getTimestamp("date_entered").toString,
              expectedData.getDate("birthday").toString,
              expectedData.getString("description"),
              expectedData.getInt("deleted").toString,
              expectedData.getString("phone_office"),
              expectedData.getString("website")
            )
            sourceData.put(id.toLong, otherData)
          }

          //println(sourceData)

          val targetStatement = targetDb.createStatement()
          val one_to_one_data = targetStatement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id;")
          var runner          = 1
          while (one_to_one_data.next()) {
            val sourceLine = sourceData.get(runner.toLong)

            withClue("compare `id` in oto") {
              one_to_one_data.getInt("id").toString should be("0")
            }
            withClue("compare `name` in oto") {
              one_to_one_data.getString("name") should be(sourceLine.get(1))
            }
            withClue("compare `vorname` in oto") {
              one_to_one_data.getString("vorname") should be(null)
            }
            withClue("compare `date_entered` in oto") {
              one_to_one_data.getTimestamp("date_entered") should be(null)
            }
            withClue("compare `birthday` in oto") {
              one_to_one_data.getDate("birthday").toString should be(sourceLine.get(4))
            }
            withClue("compare `description` in oto") {
              one_to_one_data.getString("description") should be(null)
            }
            withClue("compare `deleted` in oto") {
              one_to_one_data.getInt("deleted").toString should be("0")
            }
            withClue("compare `phone_office` in oto") {
              one_to_one_data.getString("phone_office") should be(sourceLine.get(7))
            }
            withClue("compare `website` in oto") {
              one_to_one_data.getString("website") should be(null)
            }
            runner += 1
          }

          val all_to_all_data =
            targetStatement.executeQuery("SELECT * FROM ACCOUNTS_ATA ORDER BY id;")
          runner = 1
          while (all_to_all_data.next()) {
            val sourceLine = sourceData.get(runner.toLong)

            withClue("compare `id` in ata") {
              all_to_all_data.getInt("id").toString should be("0")
            }
            withClue("compare `name` in ata") {
              all_to_all_data.getString("name") should be(sourceLine.get(1))
            }
            withClue("compare `vorname` in ata") {
              all_to_all_data.getString("vorname") should be(null)
            }
            withClue("compare `date_entered` in ata") {
              all_to_all_data.getTimestamp("date_entered") should be(null)
            }
            withClue("compare `birthday` in ata") {
              all_to_all_data.getDate("birthday").toString should be(sourceLine.get(4))
            }
            withClue("compare `description` in ata") {
              all_to_all_data.getString("description") should be(null)
            }
            withClue("compare `deleted` in ata") {
              all_to_all_data.getInt("deleted").toString should be("0")
            }
            withClue("compare `phone_office` in ata") {
              all_to_all_data.getString("phone_office") should be(sourceLine.get(7))
            }
            withClue("compare `website` in ata") {
              all_to_all_data.getString("website") should be(null)
            }
            runner += 1
          }
        }
      }
      describe("with the mapped and nullified elements in the correct order mappings") {
        it("should migrate correctly", DbTest, DbTestH2) {
          lazy val sourceDfasdl: DFASDL = {
            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/association.xml")
            val xml = scala.io.Source.fromInputStream(in).mkString
            DFASDL("Association", xml)
          }

          lazy val targetDfasdl = {
            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/association-target.xml")
            val xml = scala.io.Source.fromInputStream(in).mkString
            DFASDL("Target", xml)
          }

          val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

          // Map-One-To-One
          val recipe_one_to_one = new Recipe(
            "Map-OTO",
            MapOneToOne,
            List(
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_id")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_id")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_name"),
                  ElementReference(sourceDfasdl.id, "accounts_row_birthday"),
                  ElementReference(sourceDfasdl.id, "accounts_row_phone_office")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_name"),
                  ElementReference(targetDfasdl.id, "accounts_row_birthday"),
                  ElementReference(targetDfasdl.id, "accounts_row_phone_office")
                )
              ),
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_vorname"),
                  ElementReference(sourceDfasdl.id, "accounts_row_date_entered")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_vorname"),
                  ElementReference(targetDfasdl.id, "accounts_row_date_entered")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_description"),
                  ElementReference(sourceDfasdl.id, "accounts_row_deleted")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_description"),
                  ElementReference(targetDfasdl.id, "accounts_row_deleted")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_website")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_website")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              )
            )
          )

          // Map-All-To-All
          val recipe_all_to_all = new Recipe(
            "Map-ATA",
            MapAllToAll,
            List(
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_id")),
                List(
                  ElementReference(targetDfasdl.id, "accounts_ata_row_id")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_name")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_name"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_id")),
                List(
                  ElementReference(targetDfasdl.id, "accounts_ata_row_vorname"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_date_entered")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_birthday")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_birthday"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_id")),
                List(
                  ElementReference(targetDfasdl.id, "accounts_ata_row_description"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_deleted")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_phone_office")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_phone_office"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_id")),
                List(
                  ElementReference(targetDfasdl.id, "accounts_ata_row_website")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              )
            )
          )

          val cookbook = Cookbook("MY-COOKBOOK",
                                  List(sourceDfasdl),
                                  Option(targetDfasdl),
                                  List(recipe_all_to_all, recipe_one_to_one))

          val sourceConnection = new ConnectionInformation(
            uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
            dfasdlRef = Option(new DFASDLReference(cookbook.id, sourceDfasdl.id))
          )
          val targetConnection = new ConnectionInformation(
            uri = new URI(targetDb.getMetaData.getURL),
            dfasdlRef = Option(new DFASDLReference(cookbook.id, targetDfasdl.id))
          )

          val startTransformationMsg = new AgentStartTransformationMessage(
            sources = List(sourceConnection),
            target = targetConnection,
            cookbook = cookbook
          )

          val dataTree = TestActorRef(
            DataTreeDocument.props(sourceDfasdl, Option("AssociationTest"), Set.empty[String])
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(sourceConnection, cookbook, dataTree, Option("AssociationTest"))
          )
          dbParser ! BaseParserMessages.Start

          val message = expectMsgType[ParserStatusMessage](5.seconds)
          message.status should be(ParserStatus.COMPLETED)

          val processor = TestFSMRef(new Processor(Option("AssociationTest")))
          processor.stateName should be(ProcessorState.Idle)

          processor ! StartProcessingMessage(startTransformationMsg, List(dataTree))
          expectMsg(FiniteDuration(90, SECONDS), Completed)

          val sourceData   = new mutable.HashMap[Long, List[String]]()
          val statement    = sourceDatabaseConnection.createStatement()
          val expectedData = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id;")
          while (expectedData.next()) {
            val id = expectedData.getInt("id")
            val otherData = List[String](
              id.toString,
              expectedData.getString("name"),
              expectedData.getString("vorname"),
              expectedData.getTimestamp("date_entered").toString,
              expectedData.getDate("birthday").toString,
              expectedData.getString("description"),
              expectedData.getInt("deleted").toString,
              expectedData.getString("phone_office"),
              expectedData.getString("website")
            )
            sourceData.put(id.toLong, otherData)
          }

          //println(sourceData)

          val targetStatement = targetDb.createStatement()
          val one_to_one_data = targetStatement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id;")
          var runner          = 1
          while (one_to_one_data.next()) {
            val sourceLine = sourceData.get(runner.toLong)

            withClue("compare `id` in oto") {
              one_to_one_data.getInt("id").toString should be("0")
            }
            withClue("compare `name` in oto") {
              one_to_one_data.getString("name") should be(sourceLine.get(1))
            }
            withClue("compare `vorname` in oto") {
              one_to_one_data.getString("vorname") should be(null)
            }
            withClue("compare `date_entered` in oto") {
              one_to_one_data.getTimestamp("date_entered") should be(null)
            }
            withClue("compare `birthday` in oto") {
              one_to_one_data.getDate("birthday").toString should be(sourceLine.get(4))
            }
            withClue("compare `description` in oto") {
              one_to_one_data.getString("description") should be(null)
            }
            withClue("compare `deleted` in oto") {
              one_to_one_data.getInt("deleted").toString should be("0")
            }
            withClue("compare `phone_office` in oto") {
              one_to_one_data.getString("phone_office") should be(sourceLine.get(7))
            }
            withClue("compare `website` in oto") {
              one_to_one_data.getString("website") should be(null)
            }
            runner += 1
          }

          val all_to_all_data =
            targetStatement.executeQuery("SELECT * FROM ACCOUNTS_ATA ORDER BY id;")
          runner = 1
          while (all_to_all_data.next()) {
            val sourceLine = sourceData.get(runner.toLong)

            withClue("compare `id` in ata") {
              all_to_all_data.getInt("id").toString should be("0")
            }
            withClue("compare `name` in ata") {
              all_to_all_data.getString("name") should be(sourceLine.get(1))
            }
            withClue("compare `vorname` in ata") {
              all_to_all_data.getString("vorname") should be(null)
            }
            withClue("compare `date_entered` in ata") {
              all_to_all_data.getTimestamp("date_entered") should be(null)
            }
            withClue("compare `birthday` in ata") {
              all_to_all_data.getDate("birthday").toString should be(sourceLine.get(4))
            }
            withClue("compare `description` in ata") {
              all_to_all_data.getString("description") should be(null)
            }
            withClue("compare `deleted` in ata") {
              all_to_all_data.getInt("deleted").toString should be("0")
            }
            withClue("compare `phone_office` in ata") {
              all_to_all_data.getString("phone_office") should be(sourceLine.get(7))
            }
            withClue("compare `website` in ata") {
              all_to_all_data.getString("website") should be(null)
            }
            runner += 1
          }
        }
      }
      describe("with the mapped and the nullified elements in random mappings order") {
        it("should migrate correctly", DbTest, DbTestH2) {
          lazy val sourceDfasdl: DFASDL = {
            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/association.xml")
            val xml = scala.io.Source.fromInputStream(in).mkString
            DFASDL("Association", xml)
          }

          lazy val targetDfasdl = {
            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/association-target.xml")
            val xml = scala.io.Source.fromInputStream(in).mkString
            DFASDL("Target", xml)
          }

          val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

          // Map-One-To-One
          val recipe_one_to_one = new Recipe(
            "Map-OTO",
            MapOneToOne,
            List(
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_description"),
                  ElementReference(sourceDfasdl.id, "accounts_row_deleted")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_description"),
                  ElementReference(targetDfasdl.id, "accounts_row_deleted")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_vorname"),
                  ElementReference(sourceDfasdl.id, "accounts_row_date_entered")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_vorname"),
                  ElementReference(targetDfasdl.id, "accounts_row_date_entered")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_name"),
                  ElementReference(sourceDfasdl.id, "accounts_row_birthday"),
                  ElementReference(sourceDfasdl.id, "accounts_row_phone_office")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_name"),
                  ElementReference(targetDfasdl.id, "accounts_row_birthday"),
                  ElementReference(targetDfasdl.id, "accounts_row_phone_office")
                )
              ),
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_website")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_website")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "accounts_row_id")
                ),
                List(
                  ElementReference(targetDfasdl.id, "accounts_row_id")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              )
            )
          )

          // Map-All-To-All
          val recipe_all_to_all = new Recipe(
            "Map-ATA",
            MapAllToAll,
            List(
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_id")),
                List(
                  ElementReference(targetDfasdl.id, "accounts_ata_row_website")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_birthday")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_birthday"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_id")),
                List(
                  ElementReference(targetDfasdl.id, "accounts_ata_row_id")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_id")),
                List(
                  ElementReference(targetDfasdl.id, "accounts_ata_row_vorname"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_date_entered")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_id")),
                List(
                  ElementReference(targetDfasdl.id, "accounts_ata_row_description"),
                  ElementReference(targetDfasdl.id, "accounts_ata_row_deleted")
                ),
                List(
                  TransformationDescription(
                    "com.wegtam.tensei.agent.transformers.Nullify",
                    TransformerOptions(classOf[String], classOf[String])
                  )
                )
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_name")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_name"))
              ),
              new MappingTransformation(
                List(ElementReference(sourceDfasdl.id, "accounts_row_phone_office")),
                List(ElementReference(targetDfasdl.id, "accounts_ata_row_phone_office"))
              )
            )
          )

          val cookbook = Cookbook("MY-COOKBOOK",
                                  List(sourceDfasdl),
                                  Option(targetDfasdl),
                                  List(recipe_all_to_all, recipe_one_to_one))

          val sourceConnection = new ConnectionInformation(
            uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
            dfasdlRef = Option(new DFASDLReference(cookbook.id, sourceDfasdl.id))
          )
          val targetConnection = new ConnectionInformation(
            uri = new URI(targetDb.getMetaData.getURL),
            dfasdlRef = Option(new DFASDLReference(cookbook.id, targetDfasdl.id))
          )

          val startTransformationMsg = new AgentStartTransformationMessage(
            sources = List(sourceConnection),
            target = targetConnection,
            cookbook = cookbook
          )

          val dataTree = TestActorRef(
            DataTreeDocument.props(sourceDfasdl, Option("AssociationTest"), Set.empty[String])
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(sourceConnection, cookbook, dataTree, Option("AssociationTest"))
          )
          dbParser ! BaseParserMessages.Start

          val message = expectMsgType[ParserStatusMessage](5.seconds)
          message.status should be(ParserStatus.COMPLETED)

          val processor = TestFSMRef(new Processor(Option("AssociationTest")))
          processor.stateName should be(ProcessorState.Idle)

          processor ! StartProcessingMessage(startTransformationMsg, List(dataTree))
          expectMsg(FiniteDuration(90, SECONDS), Completed)

          val sourceData   = new mutable.HashMap[Long, List[String]]()
          val statement    = sourceDatabaseConnection.createStatement()
          val expectedData = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id;")
          while (expectedData.next()) {
            val id = expectedData.getInt("id")
            val otherData = List[String](
              id.toString,
              expectedData.getString("name"),
              expectedData.getString("vorname"),
              expectedData.getTimestamp("date_entered").toString,
              expectedData.getDate("birthday").toString,
              expectedData.getString("description"),
              expectedData.getInt("deleted").toString,
              expectedData.getString("phone_office"),
              expectedData.getString("website")
            )
            sourceData.put(id.toLong, otherData)
          }

          //println(sourceData)

          val targetStatement = targetDb.createStatement()
          val one_to_one_data = targetStatement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id;")
          var runner          = 1
          while (one_to_one_data.next()) {
            val sourceLine = sourceData.get(runner.toLong)

            withClue("compare `id` in oto") {
              one_to_one_data.getInt("id").toString should be("0")
            }
            withClue("compare `name` in oto") {
              one_to_one_data.getString("name") should be(sourceLine.get(1))
            }
            withClue("compare `vorname` in oto") {
              one_to_one_data.getString("vorname") should be(null)
            }
            withClue("compare `date_entered` in oto") {
              one_to_one_data.getTimestamp("date_entered") should be(null)
            }
            withClue("compare `birthday` in oto") {
              one_to_one_data.getDate("birthday").toString should be(sourceLine.get(4))
            }
            withClue("compare `description` in oto") {
              one_to_one_data.getString("description") should be(null)
            }
            withClue("compare `deleted` in oto") {
              one_to_one_data.getInt("deleted").toString should be("0")
            }
            withClue("compare `phone_office` in oto") {
              one_to_one_data.getString("phone_office") should be(sourceLine.get(7))
            }
            withClue("compare `website` in oto") {
              one_to_one_data.getString("website") should be(null)
            }
            runner += 1
          }

          val all_to_all_data =
            targetStatement.executeQuery("SELECT * FROM ACCOUNTS_ATA ORDER BY id;")
          runner = 1
          while (all_to_all_data.next()) {
            val sourceLine = sourceData.get(runner.toLong)

            withClue("compare `id` in ata") {
              all_to_all_data.getInt("id").toString should be("0")
            }
            withClue("compare `name` in ata") {
              all_to_all_data.getString("name") should be(sourceLine.get(1))
            }
            withClue("compare `vorname` in ata") {
              all_to_all_data.getString("vorname") should be(null)
            }
            withClue("compare `date_entered` in ata") {
              all_to_all_data.getTimestamp("date_entered") should be(null)
            }
            withClue("compare `birthday` in ata") {
              all_to_all_data.getDate("birthday").toString should be(sourceLine.get(4))
            }
            withClue("compare `description` in ata") {
              all_to_all_data.getString("description") should be(null)
            }
            withClue("compare `deleted` in ata") {
              all_to_all_data.getInt("deleted").toString should be("0")
            }
            withClue("compare `phone_office` in ata") {
              all_to_all_data.getString("phone_office") should be(sourceLine.get(7))
            }
            withClue("compare `website` in ata") {
              all_to_all_data.getString("website") should be(null)
            }
            runner += 1
          }
        }
      }
    }
  }

}
