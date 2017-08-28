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
import java.nio.file.{ FileSystems, Files }

import akka.testkit.TestActorRef
import com.wegtam.scalatest.tags.{ DbTest, DbTestH2, DbTestSqlite }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.Processor.ProcessorMessages.{ Completed, StartProcessingMessage }
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.parsers.{ BaseParserMessages, DatabaseParser }
import com.wegtam.tensei.agent.{ DataTreeDocument, Processor, XmlActorSpec }
import org.scalatest.BeforeAndAfterEach
import org.xml.sax.InputSource

import scala.concurrent.duration._

class CopyBetweenDatabases extends XmlActorSpec with BeforeAndAfterEach {
  val sourceDatabaseName = "testsrc"
  val targetDatabaseName = "testdst"

  val sqliteDbPath1 =
    File.createTempFile("tensei-agent", "testSqlite.db").getAbsolutePath.replace("\\", "/")
  val sqliteDbPath2 =
    File.createTempFile("tensei-agent", "testSqlite.db").getAbsolutePath.replace("\\", "/")

  override def beforeEach(): Unit = {
    val c =
      java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
    c.close()
    val p1 = FileSystems.getDefault.getPath(sqliteDbPath1)
    Files.deleteIfExists(p1)
    val p2 = FileSystems.getDefault.getPath(sqliteDbPath2)
    Files.deleteIfExists(p2)
    ()
  }

  override def afterEach(): Unit = {
    val t  = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")
    val st = t.createStatement()
    st.execute("SHUTDOWN")
    st.close()
    t.close()
    val c  = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$sourceDatabaseName")
    val cs = c.createStatement()
    cs.execute("SHUTDOWN")
    cs.close()
    c.close()
    val p1 = FileSystems.getDefault.getPath(sqliteDbPath1)
    Files.deleteIfExists(p1)
    val p2 = FileSystems.getDefault.getPath(sqliteDbPath2)
    Files.deleteIfExists(p2)
    ()
  }

  private def executeDbQuery(db: java.sql.Connection, sql: String): Unit = {
    val s = db.createStatement()
    s.execute(sql)
    s.close()
  }

  describe("Use cases") {
    describe("using databases") {
      describe("copying between databases") {
        describe("copying a single table between two databases") {
          it("should properly copy the data", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE accounts (
                |  id BIGINT,
                |  name VARCHAR(254),
                |  description VARCHAR,
                |  birthday DATE,
                |  salary DOUBLE,
                |);
                |INSERT INTO accounts VALUES (1, 'Max Mustermann', 'Afraid of his wife...', '1963-01-01', '1500000.83');
                |INSERT INTO accounts VALUES (2, 'Eva Musterfrau', NULL, '1968-01-01', '2800000.00');
                |INSERT INTO accounts VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '1968-08-08', '3.14256');
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/copy-between-databases-01.xml")
            val xml    = scala.io.Source.fromInputStream(in).mkString
            val dfasdl = DFASDL("MY-DFASDL", xml)
            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(dfasdl),
              Option(dfasdl),
              List(
                Recipe.createOneToOneRecipe(
                  "ID",
                  List(
                    MappingTransformation(
                      List(
                        ElementReference(dfasdl.id, "id"),
                        ElementReference(dfasdl.id, "name"),
                        ElementReference(dfasdl.id, "description"),
                        ElementReference(dfasdl.id, "birthday"),
                        ElementReference(dfasdl.id, "salary")
                      ),
                      List(
                        ElementReference(dfasdl.id, "id"),
                        ElementReference(dfasdl.id, "name"),
                        ElementReference(dfasdl.id, "description"),
                        ElementReference(dfasdl.id, "birthday"),
                        ElementReference(dfasdl.id, "salary")
                      ),
                      List()
                    )
                  )
                )
              )
            )
            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(dfasdl, Option("CopyBetweenDatabasesTest"), Set.empty[String])
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("CopyBetweenDatabasesTest"))
            )
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](5.seconds)
            message.status should be(ParserStatus.COMPLETED)

            val processor = TestActorRef(new Processor(Option("CopyBetweenDatabasesTest")))
            processor ! new StartProcessingMessage(msg, List(dataTree))
            expectMsg(Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
            withClue("Data should have been written to the database!") {
              results.next() should be(true)
              results.getLong("id") should be(1)
              results.getString("name") should be("Max Mustermann")
              results.getString("description") should be("Afraid of his wife...")
              results.getDate("birthday") should be(java.sql.Date.valueOf("1963-01-01"))
              results.getDouble("salary") should be(1500000.83d)
              results.next() should be(true)
              results.getLong("id") should be(2)
              results.getString("name") should be("Eva Musterfrau")
              results.getString("description") should be(null)
              results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
              results.getDouble("salary") should be(2800000.0d)
              results.next() should be(true)
              results.getLong("id") should be(3)
              results.getString("name") should be("Dr. Evil")
              results.getString("description") should be("Afraid of Austin Powers!")
              results.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
              results.getDouble("salary") should be(3.14256d)
              results.next() should be(false)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("copying an empty table between two databases") {
          it("should properly copy the data", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE accounts (
                |  id BIGINT,
                |  name VARCHAR(254),
                |  description VARCHAR,
                |  birthday DATE,
                |  salary DOUBLE,
                |);
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/copy-between-databases-01.xml")
            val xml    = scala.io.Source.fromInputStream(in).mkString
            val dfasdl = DFASDL("MY-DFASDL", xml)
            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(dfasdl),
              Option(dfasdl),
              List(
                Recipe.createOneToOneRecipe(
                  "ID",
                  List(
                    MappingTransformation(
                      List(
                        ElementReference(dfasdl.id, "id"),
                        ElementReference(dfasdl.id, "name"),
                        ElementReference(dfasdl.id, "description"),
                        ElementReference(dfasdl.id, "birthday"),
                        ElementReference(dfasdl.id, "salary")
                      ),
                      List(
                        ElementReference(dfasdl.id, "id"),
                        ElementReference(dfasdl.id, "name"),
                        ElementReference(dfasdl.id, "description"),
                        ElementReference(dfasdl.id, "birthday"),
                        ElementReference(dfasdl.id, "salary")
                      ),
                      List()
                    )
                  )
                )
              )
            )
            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(dfasdl, Option("CopyBetweenDatabasesTest"), Set.empty[String])
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("CopyBetweenDatabasesTest"))
            )
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](5.seconds)
            message.status should be(ParserStatus.COMPLETED)

            val processor = TestActorRef(new Processor(Option("CopyBetweenDatabasesTest")))
            processor ! new StartProcessingMessage(msg, List(dataTree))
            expectMsg(Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
            withClue("Data should have been written to the database!") {
              results.next() should be(false)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("copying two tables between two databases") {
          it("should properly copy the data", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE accounts (
                |  id BIGINT,
                |  name VARCHAR(254),
                |  description VARCHAR,
                |  birthday DATE,
                |  salary DOUBLE,
                |);
                |CREATE TABLE accounts2 (
                |  id BIGINT,
                |  name VARCHAR(254),
                |  description VARCHAR,
                |  birthday DATE,
                |  salary DOUBLE,
                |);
                |INSERT INTO accounts VALUES (1, 'Max Mustermann', 'Afraid of his wife...', '1980-01-01', '1500000.83');
                |INSERT INTO accounts VALUES (2, 'Eva Musterfrau', NULL, '1988-01-01', '2800000.00');
                |INSERT INTO accounts VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '1972-08-08', '3.14256');
                |INSERT INTO accounts2 VALUES (4, 'Max Mustermann', 'Afraid of his wife...', '1999-01-01', '1500000.83');
                |INSERT INTO accounts2 VALUES (5, 'Eva Musterfrau', NULL, '1981-01-01', '2800000.00');
                |INSERT INTO accounts2 VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '2001-08-08', '3.14256');
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/copy-between-databases-02.xml")
            val xml    = scala.io.Source.fromInputStream(in).mkString
            val dfasdl = DFASDL("MY-DFASDL", xml)
            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(dfasdl),
              Option(dfasdl),
              List(
                Recipe.createOneToOneRecipe(
                  "ID1",
                  List(
                    MappingTransformation(
                      List(
                        ElementReference(dfasdl.id, "id"),
                        ElementReference(dfasdl.id, "name"),
                        ElementReference(dfasdl.id, "description"),
                        ElementReference(dfasdl.id, "birthday"),
                        ElementReference(dfasdl.id, "salary")
                      ),
                      List(
                        ElementReference(dfasdl.id, "id"),
                        ElementReference(dfasdl.id, "name"),
                        ElementReference(dfasdl.id, "description"),
                        ElementReference(dfasdl.id, "birthday"),
                        ElementReference(dfasdl.id, "salary")
                      ),
                      List()
                    )
                  )
                ),
                Recipe.createOneToOneRecipe(
                  "ID2",
                  List(
                    MappingTransformation(
                      List(
                        ElementReference(dfasdl.id, "id2"),
                        ElementReference(dfasdl.id, "name2"),
                        ElementReference(dfasdl.id, "description2"),
                        ElementReference(dfasdl.id, "birthday2"),
                        ElementReference(dfasdl.id, "salary2")
                      ),
                      List(
                        ElementReference(dfasdl.id, "id2"),
                        ElementReference(dfasdl.id, "name2"),
                        ElementReference(dfasdl.id, "description2"),
                        ElementReference(dfasdl.id, "birthday2"),
                        ElementReference(dfasdl.id, "salary2")
                      ),
                      List()
                    )
                  )
                )
              )
            )
            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(dfasdl, Option("CopyBetweenDatabasesTest"), Set.empty[String])
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("CopyBetweenDatabasesTest"))
            )
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](5.seconds)
            message.status should be(ParserStatus.COMPLETED)

            val expectedDataXml = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/usecases/databases/copy-between-databases-02-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            compareSequenceData("id", expectedDataTree, dataTree)
            compareSequenceData("name", expectedDataTree, dataTree)
            compareSequenceData("description", expectedDataTree, dataTree)
            compareSequenceData("birthday", expectedDataTree, dataTree)
            compareSequenceData("salary", expectedDataTree, dataTree)
            compareSequenceData("id2", expectedDataTree, dataTree)
            compareSequenceData("name2", expectedDataTree, dataTree)
            compareSequenceData("description2", expectedDataTree, dataTree)
            compareSequenceData("birthday2", expectedDataTree, dataTree)
            compareSequenceData("salary2", expectedDataTree, dataTree)

            val processor = TestActorRef(new Processor(Option("CopyBetweenDatabasesTest")))
            processor ! new StartProcessingMessage(msg, List(dataTree))
            expectMsg(Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
            withClue("Data should have been written to the database!") {
              results.next() should be(true)
              results.getLong("id") should be(1)
              results.getString("name") should be("Max Mustermann")
              results.getString("description") should be("Afraid of his wife...")
              results.getDate("birthday") should be(java.sql.Date.valueOf("1980-01-01"))
              results.getDouble("salary") should be(1500000.83d)
              results.next() should be(true)
              results.getLong("id") should be(2)
              results.getString("name") should be("Eva Musterfrau")
              results.getString("description") should be(null)
              results.getDate("birthday") should be(java.sql.Date.valueOf("1988-01-01"))
              results.getDouble("salary") should be(2800000.0d)
              results.next() should be(true)
              results.getLong("id") should be(3)
              results.getString("name") should be("Dr. Evil")
              results.getString("description") should be("Afraid of Austin Powers!")
              results.getDate("birthday") should be(java.sql.Date.valueOf("1972-08-08"))
              results.getDouble("salary") should be(3.14256d)
              results.next() should be(false)
            }

            val results2 = statement.executeQuery("SELECT * FROM ACCOUNTS2 ORDER BY id")
            withClue("Data should have been written to the database!") {
              results2.next() should be(true)
              results2.getLong("id") should be(3)
              results2.getString("name") should be("Dr. Evil")
              results2.getString("description") should be("Afraid of Austin Powers!")
              results2.getDate("birthday") should be(java.sql.Date.valueOf("2001-08-08"))
              results2.getDouble("salary") should be(3.14256d)
              results2.next() should be(true)
              results2.getLong("id") should be(4)
              results2.getString("name") should be("Max Mustermann")
              results2.getString("description") should be("Afraid of his wife...")
              results2.getDate("birthday") should be(java.sql.Date.valueOf("1999-01-01"))
              results2.getDouble("salary") should be(1500000.83d)
              results2.next() should be(true)
              results2.getLong("id") should be(5)
              results2.getString("name") should be("Eva Musterfrau")
              results2.getString("description") should be(null)
              results2.getDate("birthday") should be(java.sql.Date.valueOf("1981-01-01"))
              results2.getDouble("salary") should be(2800000.0d)
              results2.next() should be(false)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("copying three tables between two databases") {
          it("should properly copy the data", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE accounts (
                |  id BIGINT,
                |  name VARCHAR(254),
                |  description VARCHAR,
                |  birthday DATE,
                |  salary DOUBLE,
                |);
                |CREATE TABLE accounts2 (
                |  id BIGINT,
                |  name VARCHAR(254),
                |  description VARCHAR,
                |  birthday DATE,
                |  salary DOUBLE,
                |);
                |CREATE TABLE accounts3 (
                |  id BIGINT,
                |  name VARCHAR(254),
                |  description VARCHAR,
                |  birthday DATE,
                |  salary DOUBLE,
                |);
                |INSERT INTO accounts VALUES (1, 'Max Mustermann', 'Afraid of his wife...', '1963-01-01', '1500000.83');
                |INSERT INTO accounts VALUES (2, 'Eva Musterfrau', NULL, '1968-01-01', '2800000.00');
                |INSERT INTO accounts VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '1968-08-08', '3.14256');
                |INSERT INTO accounts2 VALUES (4, 'Max Mustermann', 'Afraid of his wife...', '1963-01-01', '1500000.83');
                |INSERT INTO accounts2 VALUES (5, 'Eva Musterfrau', NULL, '1968-01-01', '2800000.00');
                |INSERT INTO accounts2 VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '1968-08-08', '3.14256');
                |INSERT INTO accounts3 VALUES (6, 'Max Mustermann', 'Afraid of his wife...', '1963-01-01', '1500000.83');
                |INSERT INTO accounts3 VALUES (7, 'Eva Musterfrau', NULL, '1968-01-01', '2800000.00');
                |INSERT INTO accounts3 VALUES (8, 'Dr. Evil', 'Afraid of Austin Powers!', '1968-08-08', '3.14256');
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/copy-between-databases-03.xml")
            val xml    = scala.io.Source.fromInputStream(in).mkString
            val dfasdl = DFASDL("MY-DFASDL", xml)
            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(dfasdl),
              Option(dfasdl),
              List(
                Recipe.createOneToOneRecipe(
                  "ID1",
                  List(
                    MappingTransformation(
                      List(
                        ElementReference(dfasdl.id, "id"),
                        ElementReference(dfasdl.id, "name"),
                        ElementReference(dfasdl.id, "description"),
                        ElementReference(dfasdl.id, "birthday"),
                        ElementReference(dfasdl.id, "salary")
                      ),
                      List(
                        ElementReference(dfasdl.id, "id"),
                        ElementReference(dfasdl.id, "name"),
                        ElementReference(dfasdl.id, "description"),
                        ElementReference(dfasdl.id, "birthday"),
                        ElementReference(dfasdl.id, "salary")
                      ),
                      List()
                    )
                  )
                ),
                Recipe.createOneToOneRecipe(
                  "ID2",
                  List(
                    MappingTransformation(
                      List(
                        ElementReference(dfasdl.id, "id2"),
                        ElementReference(dfasdl.id, "name2"),
                        ElementReference(dfasdl.id, "description2"),
                        ElementReference(dfasdl.id, "birthday2"),
                        ElementReference(dfasdl.id, "salary2")
                      ),
                      List(
                        ElementReference(dfasdl.id, "id2"),
                        ElementReference(dfasdl.id, "name2"),
                        ElementReference(dfasdl.id, "description2"),
                        ElementReference(dfasdl.id, "birthday2"),
                        ElementReference(dfasdl.id, "salary2")
                      ),
                      List()
                    )
                  )
                ),
                Recipe.createOneToOneRecipe(
                  "ID3",
                  List(
                    MappingTransformation(
                      List(
                        ElementReference(dfasdl.id, "id3"),
                        ElementReference(dfasdl.id, "name3"),
                        ElementReference(dfasdl.id, "description3"),
                        ElementReference(dfasdl.id, "birthday3"),
                        ElementReference(dfasdl.id, "salary3")
                      ),
                      List(
                        ElementReference(dfasdl.id, "id3"),
                        ElementReference(dfasdl.id, "name3"),
                        ElementReference(dfasdl.id, "description3"),
                        ElementReference(dfasdl.id, "birthday3"),
                        ElementReference(dfasdl.id, "salary3")
                      ),
                      List()
                    )
                  )
                )
              )
            )
            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(dfasdl, Option("CopyBetweenDatabasesTest"), Set.empty[String])
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("CopyBetweenDatabasesTest"))
            )
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](5.seconds)
            message.status should be(ParserStatus.COMPLETED)

            val expectedDataXml = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/usecases/databases/copy-between-databases-03-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            compareSequenceData("id", expectedDataTree, dataTree)
            compareSequenceData("name", expectedDataTree, dataTree)
            compareSequenceData("description", expectedDataTree, dataTree)
            compareSequenceData("birthday", expectedDataTree, dataTree)
            compareSequenceData("salary", expectedDataTree, dataTree)
            compareSequenceData("id2", expectedDataTree, dataTree)
            compareSequenceData("name2", expectedDataTree, dataTree)
            compareSequenceData("description2", expectedDataTree, dataTree)
            compareSequenceData("birthday2", expectedDataTree, dataTree)
            compareSequenceData("salary2", expectedDataTree, dataTree)
            compareSequenceData("id3", expectedDataTree, dataTree)
            compareSequenceData("name3", expectedDataTree, dataTree)
            compareSequenceData("description3", expectedDataTree, dataTree)
            compareSequenceData("birthday3", expectedDataTree, dataTree)
            compareSequenceData("salary3", expectedDataTree, dataTree)

            val processor = TestActorRef(new Processor(Option("CopyBetweenDatabasesTest")))
            processor ! new StartProcessingMessage(msg, List(dataTree))
            expectMsg(Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
            withClue("Data should have been written to the database!") {
              results.next() should be(true)
              results.getLong("id") should be(1)
              results.getString("name") should be("Max Mustermann")
              results.getString("description") should be("Afraid of his wife...")
              results.getDate("birthday") should be(java.sql.Date.valueOf("1963-01-01"))
              results.getDouble("salary") should be(1500000.83d)
              results.next() should be(true)
              results.getLong("id") should be(2)
              results.getString("name") should be("Eva Musterfrau")
              results.getString("description") should be(null)
              results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
              results.getDouble("salary") should be(2800000.0d)
              results.next() should be(true)
              results.getLong("id") should be(3)
              results.getString("name") should be("Dr. Evil")
              results.getString("description") should be("Afraid of Austin Powers!")
              results.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
              results.getDouble("salary") should be(3.14256d)
              results.next() should be(false)
            }

            val results2 = statement.executeQuery("SELECT * FROM ACCOUNTS2 ORDER BY id")
            withClue("Data should have been written to the database!") {
              results2.next() should be(true)
              results2.getLong("id") should be(3)
              results2.getString("name") should be("Dr. Evil")
              results2.getString("description") should be("Afraid of Austin Powers!")
              results2.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
              results2.getDouble("salary") should be(3.14256d)
              results2.next() should be(true)
              results2.getLong("id") should be(4)
              results2.getString("name") should be("Max Mustermann")
              results2.getString("description") should be("Afraid of his wife...")
              results2.getDate("birthday") should be(java.sql.Date.valueOf("1963-01-01"))
              results2.getDouble("salary") should be(1500000.83d)
              results2.next() should be(true)
              results2.getLong("id") should be(5)
              results2.getString("name") should be("Eva Musterfrau")
              results2.getString("description") should be(null)
              results2.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
              results2.getDouble("salary") should be(2800000.0d)
              results2.next() should be(false)
            }

            val results3 = statement.executeQuery("SELECT * FROM ACCOUNTS3 ORDER BY id")
            withClue("Data should have been written to the database!") {
              results3.next() should be(true)
              results3.getLong("id") should be(6)
              results3.getString("name") should be("Max Mustermann")
              results3.getString("description") should be("Afraid of his wife...")
              results3.getDate("birthday") should be(java.sql.Date.valueOf("1963-01-01"))
              results3.getDouble("salary") should be(1500000.83d)
              results3.next() should be(true)
              results3.getLong("id") should be(7)
              results3.getString("name") should be("Eva Musterfrau")
              results3.getString("description") should be(null)
              results3.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
              results3.getDouble("salary") should be(2800000.0d)
              results3.next() should be(true)
              results3.getLong("id") should be(8)
              results3.getString("name") should be("Dr. Evil")
              results3.getString("description") should be("Afraid of Austin Powers!")
              results3.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
              results3.getDouble("salary") should be(3.14256d)
              results3.next() should be(false)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("copying three tables with one empty table between two databases") {
          it("should properly copy the data", DbTest, DbTestH2) {
            val sourceDb = java.sql.DriverManager
              .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
            val targetDb = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

            val sql =
              """
                |CREATE TABLE accounts (
                |  id BIGINT,
                |  name VARCHAR(254),
                |  description VARCHAR,
                |  birthday DATE,
                |  salary DOUBLE,
                |);
                |CREATE TABLE accounts2 (
                |  id BIGINT,
                |  name VARCHAR(254),
                |  description VARCHAR,
                |  birthday DATE,
                |  salary DOUBLE,
                |);
                |CREATE TABLE accounts3 (
                |  id BIGINT,
                |  name VARCHAR(254),
                |  description VARCHAR,
                |  birthday DATE,
                |  salary DOUBLE,
                |);
                |INSERT INTO accounts VALUES (1, 'Max Mustermann', 'Afraid of his wife...', '1963-01-01', '1500000.83');
                |INSERT INTO accounts VALUES (2, 'Eva Musterfrau', NULL, '1968-01-01', '2800000.00');
                |INSERT INTO accounts VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '1968-08-08', '3.14256');
                |INSERT INTO accounts3 VALUES (6, 'Max Mustermann', 'Afraid of his wife...', '1963-01-01', '1500000.83');
                |INSERT INTO accounts3 VALUES (7, 'Eva Musterfrau', NULL, '1968-01-01', '2800000.00');
                |INSERT INTO accounts3 VALUES (8, 'Dr. Evil', 'Afraid of Austin Powers!', '1968-08-08', '3.14256');
              """.stripMargin
            executeDbQuery(sourceDb, sql)

            val in: InputStream =
              getClass.getResourceAsStream("/usecases/databases/copy-between-databases-04.xml")
            val xml    = scala.io.Source.fromInputStream(in).mkString
            val dfasdl = DFASDL("MY-DFASDL", xml)
            val cookbook = Cookbook(
              "MY-COOKBOOK",
              List(dfasdl),
              Option(dfasdl),
              List(
                Recipe.createOneToOneRecipe(
                  "ID1",
                  List(
                    MappingTransformation(
                      List(
                        ElementReference(dfasdl.id, "id"),
                        ElementReference(dfasdl.id, "name"),
                        ElementReference(dfasdl.id, "description"),
                        ElementReference(dfasdl.id, "birthday"),
                        ElementReference(dfasdl.id, "salary")
                      ),
                      List(
                        ElementReference(dfasdl.id, "id"),
                        ElementReference(dfasdl.id, "name"),
                        ElementReference(dfasdl.id, "description"),
                        ElementReference(dfasdl.id, "birthday"),
                        ElementReference(dfasdl.id, "salary")
                      ),
                      List()
                    )
                  )
                ),
                Recipe.createOneToOneRecipe(
                  "ID2",
                  List(
                    MappingTransformation(
                      List(
                        ElementReference(dfasdl.id, "id2"),
                        ElementReference(dfasdl.id, "name2"),
                        ElementReference(dfasdl.id, "description2"),
                        ElementReference(dfasdl.id, "birthday2"),
                        ElementReference(dfasdl.id, "salary2")
                      ),
                      List(
                        ElementReference(dfasdl.id, "id2"),
                        ElementReference(dfasdl.id, "name2"),
                        ElementReference(dfasdl.id, "description2"),
                        ElementReference(dfasdl.id, "birthday2"),
                        ElementReference(dfasdl.id, "salary2")
                      ),
                      List()
                    )
                  )
                ),
                Recipe.createOneToOneRecipe(
                  "ID3",
                  List(
                    MappingTransformation(
                      List(
                        ElementReference(dfasdl.id, "id3"),
                        ElementReference(dfasdl.id, "name3"),
                        ElementReference(dfasdl.id, "description3"),
                        ElementReference(dfasdl.id, "birthday3"),
                        ElementReference(dfasdl.id, "salary3")
                      ),
                      List(
                        ElementReference(dfasdl.id, "id3"),
                        ElementReference(dfasdl.id, "name3"),
                        ElementReference(dfasdl.id, "description3"),
                        ElementReference(dfasdl.id, "birthday3"),
                        ElementReference(dfasdl.id, "salary3")
                      ),
                      List()
                    )
                  )
                )
              )
            )
            val source =
              new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))
            val target =
              new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                        dfasdlRef =
                                          Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))

            val msg = AgentStartTransformationMessage(List(source), target, cookbook)

            val dataTree = TestActorRef(
              DataTreeDocument.props(dfasdl, Option("CopyBetweenDatabasesTest"), Set.empty[String])
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("CopyBetweenDatabasesTest"))
            )
            dbParser ! BaseParserMessages.Start

            val message = expectMsgType[ParserStatusMessage](5.seconds)
            message.status should be(ParserStatus.COMPLETED)

            val expectedDataXml = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/usecases/databases/copy-between-databases-04-expected-data.xml"
                )
              )
              .mkString
            val expectedDataTree =
              createTestDocumentBuilder().parse(new InputSource(new StringReader(expectedDataXml)))

            compareSequenceData("id", expectedDataTree, dataTree)
            compareSequenceData("name", expectedDataTree, dataTree)
            compareSequenceData("description", expectedDataTree, dataTree)
            compareSequenceData("birthday", expectedDataTree, dataTree)
            compareSequenceData("salary", expectedDataTree, dataTree)
            compareSequenceData("id2", expectedDataTree, dataTree)
            compareSequenceData("name2", expectedDataTree, dataTree)
            compareSequenceData("description2", expectedDataTree, dataTree)
            compareSequenceData("birthday2", expectedDataTree, dataTree)
            compareSequenceData("salary2", expectedDataTree, dataTree)
            compareSequenceData("id3", expectedDataTree, dataTree)
            compareSequenceData("name3", expectedDataTree, dataTree)
            compareSequenceData("description3", expectedDataTree, dataTree)
            compareSequenceData("birthday3", expectedDataTree, dataTree)
            compareSequenceData("salary3", expectedDataTree, dataTree)

            val processor = TestActorRef(new Processor(Option("CopyBetweenDatabasesTest")))
            processor ! new StartProcessingMessage(msg, List(dataTree))
            expectMsg(Completed)

            val statement = targetDb.createStatement()
            val results   = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
            withClue("Data should have been written to the database!") {
              results.next() should be(true)
              results.getLong("id") should be(1)
              results.getString("name") should be("Max Mustermann")
              results.getString("description") should be("Afraid of his wife...")
              results.getDate("birthday") should be(java.sql.Date.valueOf("1963-01-01"))
              results.getDouble("salary") should be(1500000.83d)
              results.next() should be(true)
              results.getLong("id") should be(2)
              results.getString("name") should be("Eva Musterfrau")
              results.getString("description") should be(null)
              results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
              results.getDouble("salary") should be(2800000.0d)
              results.next() should be(true)
              results.getLong("id") should be(3)
              results.getString("name") should be("Dr. Evil")
              results.getString("description") should be("Afraid of Austin Powers!")
              results.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
              results.getDouble("salary") should be(3.14256d)
              results.next() should be(false)
            }

            val results2 = statement.executeQuery("SELECT * FROM ACCOUNTS2 ORDER BY id")
            withClue("Data should have been written to the database!") {
              results2.next() should be(false)
            }

            val results3 = statement.executeQuery("SELECT * FROM ACCOUNTS3 ORDER BY id")
            withClue("Data should have been written to the database!") {
              results3.next() should be(true)
              results3.getLong("id") should be(6)
              results3.getString("name") should be("Max Mustermann")
              results3.getString("description") should be("Afraid of his wife...")
              results3.getDate("birthday") should be(java.sql.Date.valueOf("1963-01-01"))
              results3.getDouble("salary") should be(1500000.83d)
              results3.next() should be(true)
              results3.getLong("id") should be(7)
              results3.getString("name") should be("Eva Musterfrau")
              results3.getString("description") should be(null)
              results3.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
              results3.getDouble("salary") should be(2800000.0d)
              results3.next() should be(true)
              results3.getLong("id") should be(8)
              results3.getString("name") should be("Dr. Evil")
              results3.getString("description") should be("Afraid of Austin Powers!")
              results3.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
              results3.getDouble("salary") should be(3.14256d)
              results3.next() should be(false)
            }

            executeDbQuery(sourceDb, "SHUTDOWN")
            sourceDb.close()
            executeDbQuery(targetDb, "SHUTDOWN")
            targetDb.close()
          }
        }

        describe("copying three tables with one empty table between two databases") {
          describe("and with only one mapping") {
            it("should properly copy the data", DbTest, DbTestH2) {
              val sourceDb = java.sql.DriverManager
                .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
              val targetDb =
                java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

              val sql =
                """
                  |CREATE TABLE accounts (
                  |  id BIGINT,
                  |  name VARCHAR(254),
                  |  description VARCHAR,
                  |  birthday DATE,
                  |  salary DOUBLE,
                  |);
                  |CREATE TABLE accounts2 (
                  |  id BIGINT,
                  |  name VARCHAR(254),
                  |  description VARCHAR,
                  |  birthday DATE,
                  |  salary DOUBLE,
                  |);
                  |CREATE TABLE accounts3 (
                  |  id BIGINT,
                  |  name VARCHAR(254),
                  |  description VARCHAR,
                  |  birthday DATE,
                  |  salary DOUBLE,
                  |);
                  |INSERT INTO accounts VALUES (1, 'Max Mustermann', 'Afraid of his wife...', '1963-01-01', '1500000.83');
                  |INSERT INTO accounts VALUES (2, 'Eva Musterfrau', NULL, '1968-01-01', '2800000.00');
                  |INSERT INTO accounts VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '1968-08-08', '3.14256');
                  |INSERT INTO accounts3 VALUES (6, 'Max Mustermann', 'Afraid of his wife...', '1963-01-01', '1500000.83');
                  |INSERT INTO accounts3 VALUES (7, 'Eva Musterfrau', NULL, '1968-01-01', '2800000.00');
                  |INSERT INTO accounts3 VALUES (8, 'Dr. Evil', 'Afraid of Austin Powers!', '1968-08-08', '3.14256');
                """.stripMargin
              executeDbQuery(sourceDb, sql)

              val in: InputStream =
                getClass.getResourceAsStream("/usecases/databases/copy-between-databases-05.xml")
              val xml    = scala.io.Source.fromInputStream(in).mkString
              val dfasdl = DFASDL("MY-DFASDL", xml)
              val cookbook = Cookbook(
                "MY-COOKBOOK",
                List(dfasdl),
                Option(dfasdl),
                List(
                  Recipe.createOneToOneRecipe(
                    "ID3",
                    List(
                      MappingTransformation(
                        List(
                          ElementReference(dfasdl.id, "id3"),
                          ElementReference(dfasdl.id, "name3"),
                          ElementReference(dfasdl.id, "description3"),
                          ElementReference(dfasdl.id, "birthday3"),
                          ElementReference(dfasdl.id, "salary3")
                        ),
                        List(
                          ElementReference(dfasdl.id, "id3"),
                          ElementReference(dfasdl.id, "name3"),
                          ElementReference(dfasdl.id, "description3"),
                          ElementReference(dfasdl.id, "birthday3"),
                          ElementReference(dfasdl.id, "salary3")
                        ),
                        List()
                      )
                    )
                  )
                )
              )
              val source =
                new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                          dfasdlRef =
                                            Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))
              val target =
                new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                          dfasdlRef =
                                            Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))

              val msg = AgentStartTransformationMessage(List(source), target, cookbook)

              val dataTree = TestActorRef(
                DataTreeDocument.props(dfasdl,
                                       Option("CopyBetweenDatabasesTest"),
                                       Set.empty[String])
              )
              val dbParser = TestActorRef(
                DatabaseParser
                  .props(source, cookbook, dataTree, Option("CopyBetweenDatabasesTest"))
              )
              dbParser ! BaseParserMessages.Start

              val message = expectMsgType[ParserStatusMessage](5.seconds)
              message.status should be(ParserStatus.COMPLETED)

              val expectedDataXml = scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream(
                    "/usecases/databases/copy-between-databases-05-expected-data.xml"
                  )
                )
                .mkString
              val expectedDataTree = createTestDocumentBuilder().parse(
                new InputSource(new StringReader(expectedDataXml))
              )

              compareSequenceData("id", expectedDataTree, dataTree)
              compareSequenceData("name", expectedDataTree, dataTree)
              compareSequenceData("description", expectedDataTree, dataTree)
              compareSequenceData("birthday", expectedDataTree, dataTree)
              compareSequenceData("salary", expectedDataTree, dataTree)
              compareSequenceData("id2", expectedDataTree, dataTree)
              compareSequenceData("name2", expectedDataTree, dataTree)
              compareSequenceData("description2", expectedDataTree, dataTree)
              compareSequenceData("birthday2", expectedDataTree, dataTree)
              compareSequenceData("salary2", expectedDataTree, dataTree)
              compareSequenceData("id3", expectedDataTree, dataTree)
              compareSequenceData("name3", expectedDataTree, dataTree)
              compareSequenceData("description3", expectedDataTree, dataTree)
              compareSequenceData("birthday3", expectedDataTree, dataTree)
              compareSequenceData("salary3", expectedDataTree, dataTree)

              val processor = TestActorRef(new Processor(Option("CopyBetweenDatabasesTest")))
              processor ! new StartProcessingMessage(msg, List(dataTree))
              expectMsg(Completed)

              val statement = targetDb.createStatement()
              val results   = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
              withClue("Data should have been written to the database!") {
                results.next() should be(false)
              }

              val results2 = statement.executeQuery("SELECT * FROM ACCOUNTS2 ORDER BY id")
              withClue("Data should have been written to the database!") {
                results2.next() should be(false)
              }

              val results3 = statement.executeQuery("SELECT * FROM ACCOUNTS3 ORDER BY id")
              withClue("Data should have been written to the database!") {
                results3.next() should be(true)
                results3.getLong("id") should be(6)
                results3.getString("name") should be("Max Mustermann")
                results3.getString("description") should be("Afraid of his wife...")
                results3.getDate("birthday") should be(java.sql.Date.valueOf("1963-01-01"))
                results3.getDouble("salary") should be(1500000.83d)
                results3.next() should be(true)
                results3.getLong("id") should be(7)
                results3.getString("name") should be("Eva Musterfrau")
                results3.getString("description") should be(null)
                results3.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
                results3.getDouble("salary") should be(2800000.0d)
                results3.next() should be(true)
                results3.getLong("id") should be(8)
                results3.getString("name") should be("Dr. Evil")
                results3.getString("description") should be("Afraid of Austin Powers!")
                results3.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
                results3.getDouble("salary") should be(3.14256d)
                results3.next() should be(false)
              }

              executeDbQuery(sourceDb, "SHUTDOWN")
              sourceDb.close()
              executeDbQuery(targetDb, "SHUTDOWN")
              targetDb.close()
            }
          }
        }

        describe("copying six tables") {
          describe(
            "where the first two are filled, the coming two are empty, the fifth filled and the sixth empty"
          ) {
            describe("and a different target DFASDL") {
              it("should work", DbTest, DbTestH2) {
                val sourceDb = java.sql.DriverManager
                  .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")
                val targetDb =
                  java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

                val sql =
                  """
                    |CREATE TABLE accounts (
                    |  id BIGINT,
                    |  name VARCHAR(254),
                    |  description VARCHAR,
                    |  birthday DATE,
                    |  salary DOUBLE,
                    |);
                    |CREATE TABLE accounts2 (
                    |  id BIGINT,
                    |  name VARCHAR(254),
                    |  description VARCHAR,
                    |  birthday DATE,
                    |  salary DOUBLE,
                    |);
                    |CREATE TABLE accounts3 (
                    |  id BIGINT,
                    |  name VARCHAR(254),
                    |  description VARCHAR,
                    |  birthday DATE,
                    |  salary DOUBLE,
                    |);
                    |CREATE TABLE accounts4 (
                    |  id BIGINT,
                    |  name VARCHAR(254),
                    |  description VARCHAR,
                    |  birthday DATE,
                    |  salary DOUBLE,
                    |);
                    |CREATE TABLE accounts5 (
                    |  id BIGINT,
                    |  name VARCHAR(254),
                    |  description VARCHAR,
                    |  birthday DATE,
                    |  salary DOUBLE,
                    |);
                    |CREATE TABLE accounts6 (
                    |  id BIGINT,
                    |  name VARCHAR(254),
                    |  description VARCHAR,
                    |  birthday DATE,
                    |  salary DOUBLE,
                    |);
                    |INSERT INTO accounts VALUES (1, 'Max Mustermann', 'Afraid of his wife...', '1963-01-01', '1500000.83');
                    |INSERT INTO accounts VALUES (2, 'Eva Musterfrau', NULL, '1968-01-01', '2800000.00');
                    |INSERT INTO accounts VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '1968-08-08', '3.14256');
                    |INSERT INTO accounts2 VALUES (6, 'Max Mustermann', 'Afraid of his wife...', '1963-01-01', '1500000.83');
                    |INSERT INTO accounts2 VALUES (7, 'Eva Musterfrau', NULL, '1968-01-01', '2800000.00');
                    |INSERT INTO accounts2 VALUES (8, 'Dr. Evil', 'Afraid of Austin Powers!', '1968-08-08', '3.14256');
                    |INSERT INTO accounts5 VALUES (6, 'Max Mustermann', 'Afraid of his wife...', '1963-01-01', '1500000.83');
                    |INSERT INTO accounts5 VALUES (7, 'Eva Musterfrau', NULL, '1968-01-01', '2800000.00');
                    |INSERT INTO accounts5 VALUES (8, 'Dr. Evil', 'Afraid of Austin Powers!', '1968-08-08', '3.14256');
                  """.stripMargin
                executeDbQuery(sourceDb, sql)

                val in: InputStream =
                  getClass.getResourceAsStream("/usecases/databases/copy-between-databases-06.xml")
                val xml    = scala.io.Source.fromInputStream(in).mkString
                val dfasdl = DFASDL("MY-DFASDL", xml)

                val inTarget: InputStream = getClass.getResourceAsStream(
                  "/usecases/databases/copy-between-databases-06-target.xml"
                )
                val xmlTarget    = scala.io.Source.fromInputStream(inTarget).mkString
                val targetDfasdl = DFASDL("MY-DFASDL-TARGET", xmlTarget)

                val cookbook = Cookbook(
                  "MY-COOKBOOK",
                  List(dfasdl),
                  Option(targetDfasdl),
                  List(
                    Recipe.createOneToOneRecipe(
                      "ID5",
                      List(
                        MappingTransformation(
                          List(
                            ElementReference(dfasdl.id, "id5"),
                            ElementReference(dfasdl.id, "name5"),
                            ElementReference(dfasdl.id, "birthday5"),
                            ElementReference(dfasdl.id, "salary5")
                          ),
                          List(
                            ElementReference(targetDfasdl.id, "id5"),
                            ElementReference(targetDfasdl.id, "name5"),
                            ElementReference(targetDfasdl.id, "birthday5"),
                            ElementReference(targetDfasdl.id, "salary5")
                          ),
                          List()
                        )
                      )
                    )
                  )
                )
                val source =
                  new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                            dfasdlRef =
                                              Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))
                val target = new ConnectionInformation(
                  uri = new URI(targetDb.getMetaData.getURL),
                  dfasdlRef = Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL-TARGET"))
                )

                val msg = AgentStartTransformationMessage(List(source), target, cookbook)

                val dataTree = TestActorRef(
                  DataTreeDocument.props(dfasdl,
                                         Option("CopyBetweenDatabasesTest"),
                                         Set.empty[String])
                )
                val dbParser = TestActorRef(
                  DatabaseParser.props(source,
                                       cookbook,
                                       dataTree,
                                       Option("CopyBetweenDatabasesTest"))
                )
                dbParser ! BaseParserMessages.Start

                val message = expectMsgType[ParserStatusMessage](5.seconds)
                message.status should be(ParserStatus.COMPLETED)

                val expectedDataXml = scala.io.Source
                  .fromInputStream(
                    getClass.getResourceAsStream(
                      "/usecases/databases/copy-between-databases-06-expected-data.xml"
                    )
                  )
                  .mkString
                val expectedDataTree = createTestDocumentBuilder().parse(
                  new InputSource(new StringReader(expectedDataXml))
                )

                compareSequenceData("id", expectedDataTree, dataTree)
                compareSequenceData("name", expectedDataTree, dataTree)
                compareSequenceData("description", expectedDataTree, dataTree)
                compareSequenceData("birthday", expectedDataTree, dataTree)
                compareSequenceData("salary", expectedDataTree, dataTree)
                compareSequenceData("id2", expectedDataTree, dataTree)
                compareSequenceData("name2", expectedDataTree, dataTree)
                compareSequenceData("description2", expectedDataTree, dataTree)
                compareSequenceData("birthday2", expectedDataTree, dataTree)
                compareSequenceData("salary2", expectedDataTree, dataTree)
                compareSequenceData("id3", expectedDataTree, dataTree)
                compareSequenceData("name3", expectedDataTree, dataTree)
                compareSequenceData("description3", expectedDataTree, dataTree)
                compareSequenceData("birthday3", expectedDataTree, dataTree)
                compareSequenceData("salary3", expectedDataTree, dataTree)

                val processor = TestActorRef(new Processor(Option("CopyBetweenDatabasesTest")))
                processor ! new StartProcessingMessage(msg, List(dataTree))
                expectMsg(Completed)

                val statement = targetDb.createStatement()

                val results3 = statement.executeQuery("SELECT * FROM ACCOUNTS5 ORDER BY id")
                withClue("Data should have been written to the database!") {
                  results3.next() should be(true)
                  results3.getLong("id") should be(6)
                  results3.getString("name") should be("Max Mustermann")
                  results3.getDate("birthday") should be(java.sql.Date.valueOf("1963-01-01"))
                  results3.getDouble("salary") should be(1500000.83d)
                  results3.next() should be(true)
                  results3.getLong("id") should be(7)
                  results3.getString("name") should be("Eva Musterfrau")
                  results3.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
                  results3.getDouble("salary") should be(2800000.0d)
                  results3.next() should be(true)
                  results3.getLong("id") should be(8)
                  results3.getString("name") should be("Dr. Evil")
                  results3.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
                  results3.getDouble("salary") should be(3.14256d)
                  results3.next() should be(false)
                }

                executeDbQuery(sourceDb, "SHUTDOWN")
                sourceDb.close()
                executeDbQuery(targetDb, "SHUTDOWN")
                targetDb.close()
              }
            }
          }
        }

        describe("using sqlite") {
          describe("as source database") {
            it("should work", DbTest, DbTestH2, DbTestSqlite) {
              val sourceDatabaseConnectionSqlite =
                java.sql.DriverManager.getConnection(s"jdbc:sqlite:$sqliteDbPath1")

              val targetDb =
                java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")

              val sql =
                """
                  |CREATE TABLE accounts (id BIGINT, name VARCHAR(254), description VARCHAR, birthday DATE, salary DOUBLE);
                """.stripMargin
              val sql2 =
                """
                  |CREATE TABLE accounts2 (id BIGINT, name VARCHAR(254), description VARCHAR, birthday DATE, salary DOUBLE);
                """.stripMargin
              val sql3 =
                """
                  |INSERT INTO accounts VALUES (1, 'Max Mustermann', 'Afraid of his wife...', '1980-01-01', '1500000.83');
                """.stripMargin
              val sql4 =
                """
                  |INSERT INTO accounts VALUES (2, 'Eva Musterfrau', NULL, '1988-01-01', '2800000.00');
                """.stripMargin
              val sql5 =
                """
                  |INSERT INTO accounts VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '1972-08-08', '3.14256');
                """.stripMargin
              val sql6 =
                """
                  |INSERT INTO accounts2 VALUES (4, 'Max Mustermann', 'Afraid of his wife...', '1999-01-01', '1500000.83');
                """.stripMargin
              val sql7 =
                """
                  |INSERT INTO accounts2 VALUES (5, 'Eva Musterfrau', NULL, '1981-01-01', '2800000.00');
                """.stripMargin
              val sql8 =
                """
                  |INSERT INTO accounts2 VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '2001-08-08', '3.14256');
                """.stripMargin

              executeDbQuery(sourceDatabaseConnectionSqlite, sql)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql2)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql3)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql4)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql5)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql6)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql7)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql8)

              val in: InputStream =
                getClass.getResourceAsStream("/usecases/databases/copy-between-databases-02.xml")
              val xml    = scala.io.Source.fromInputStream(in).mkString
              val dfasdl = DFASDL("MY-DFASDL", xml)
              val cookbook = Cookbook(
                "MY-COOKBOOK",
                List(dfasdl),
                Option(dfasdl),
                List(
                  Recipe.createOneToOneRecipe(
                    "ID1",
                    List(
                      MappingTransformation(
                        List(
                          ElementReference(dfasdl.id, "id"),
                          ElementReference(dfasdl.id, "name"),
                          ElementReference(dfasdl.id, "description"),
                          ElementReference(dfasdl.id, "birthday"),
                          ElementReference(dfasdl.id, "salary")
                        ),
                        List(
                          ElementReference(dfasdl.id, "id"),
                          ElementReference(dfasdl.id, "name"),
                          ElementReference(dfasdl.id, "description"),
                          ElementReference(dfasdl.id, "birthday"),
                          ElementReference(dfasdl.id, "salary")
                        ),
                        List()
                      )
                    )
                  ),
                  Recipe.createOneToOneRecipe(
                    "ID2",
                    List(
                      MappingTransformation(
                        List(
                          ElementReference(dfasdl.id, "id2"),
                          ElementReference(dfasdl.id, "name2"),
                          ElementReference(dfasdl.id, "description2"),
                          ElementReference(dfasdl.id, "birthday2"),
                          ElementReference(dfasdl.id, "salary2")
                        ),
                        List(
                          ElementReference(dfasdl.id, "id2"),
                          ElementReference(dfasdl.id, "name2"),
                          ElementReference(dfasdl.id, "description2"),
                          ElementReference(dfasdl.id, "birthday2"),
                          ElementReference(dfasdl.id, "salary2")
                        ),
                        List()
                      )
                    )
                  )
                )
              )
              val source = new ConnectionInformation(
                uri = new URI(sourceDatabaseConnectionSqlite.getMetaData.getURL),
                dfasdlRef = Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL"))
              )
              val target =
                new ConnectionInformation(uri = new URI(targetDb.getMetaData.getURL),
                                          dfasdlRef =
                                            Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))

              val msg = AgentStartTransformationMessage(List(source), target, cookbook)

              val dataTree = TestActorRef(
                DataTreeDocument.props(dfasdl,
                                       Option("CopyBetweenDatabasesTest"),
                                       Set.empty[String])
              )
              val dbParser = TestActorRef(
                DatabaseParser
                  .props(source, cookbook, dataTree, Option("CopyBetweenDatabasesTest"))
              )
              dbParser ! BaseParserMessages.Start

              val message = expectMsgType[ParserStatusMessage](5.seconds)
              message.status should be(ParserStatus.COMPLETED)

              val expectedDataXml = scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream(
                    "/usecases/databases/copy-between-databases-02-expected-data.xml"
                  )
                )
                .mkString
              val expectedDataTree = createTestDocumentBuilder().parse(
                new InputSource(new StringReader(expectedDataXml))
              )

              compareSequenceData("id", expectedDataTree, dataTree)
              compareSequenceData("name", expectedDataTree, dataTree)
              compareSequenceData("description", expectedDataTree, dataTree)
              compareSequenceData("birthday", expectedDataTree, dataTree)
              compareSequenceData("salary", expectedDataTree, dataTree)
              compareSequenceData("id2", expectedDataTree, dataTree)
              compareSequenceData("name2", expectedDataTree, dataTree)
              compareSequenceData("description2", expectedDataTree, dataTree)
              compareSequenceData("birthday2", expectedDataTree, dataTree)
              compareSequenceData("salary2", expectedDataTree, dataTree)

              val processor = TestActorRef(new Processor(Option("CopyBetweenDatabasesTest")))
              processor ! new StartProcessingMessage(msg, List(dataTree))
              expectMsg(Completed)

              val statement = targetDb.createStatement()
              val results   = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
              withClue("Data should have been written to the database!") {
                results.next() should be(true)
                results.getLong("id") should be(1)
                results.getString("name") should be("Max Mustermann")
                results.getString("description") should be("Afraid of his wife...")
                results.getDate("birthday") should be(java.sql.Date.valueOf("1980-01-01"))
                results.getDouble("salary") should be(1500000.83d)
                results.next() should be(true)
                results.getLong("id") should be(2)
                results.getString("name") should be("Eva Musterfrau")
                results.getString("description") should be(null)
                results.getDate("birthday") should be(java.sql.Date.valueOf("1988-01-01"))
                results.getDouble("salary") should be(2800000.0d)
                results.next() should be(true)
                results.getLong("id") should be(3)
                results.getString("name") should be("Dr. Evil")
                results.getString("description") should be("Afraid of Austin Powers!")
                results.getDate("birthday") should be(java.sql.Date.valueOf("1972-08-08"))
                results.getDouble("salary") should be(3.14256d)
                results.next() should be(false)
              }

              val results2 = statement.executeQuery("SELECT * FROM ACCOUNTS2 ORDER BY id")
              withClue("Data should have been written to the database!") {
                results2.next() should be(true)
                results2.getLong("id") should be(3)
                results2.getString("name") should be("Dr. Evil")
                results2.getString("description") should be("Afraid of Austin Powers!")
                results2.getDate("birthday") should be(java.sql.Date.valueOf("2001-08-08"))
                results2.getDouble("salary") should be(3.14256d)
                results2.next() should be(true)
                results2.getLong("id") should be(4)
                results2.getString("name") should be("Max Mustermann")
                results2.getString("description") should be("Afraid of his wife...")
                results2.getDate("birthday") should be(java.sql.Date.valueOf("1999-01-01"))
                results2.getDouble("salary") should be(1500000.83d)
                results2.next() should be(true)
                results2.getLong("id") should be(5)
                results2.getString("name") should be("Eva Musterfrau")
                results2.getString("description") should be(null)
                results2.getDate("birthday") should be(java.sql.Date.valueOf("1981-01-01"))
                results2.getDouble("salary") should be(2800000.0d)
                results2.next() should be(false)
              }

              sourceDatabaseConnectionSqlite.close()
              executeDbQuery(targetDb, "SHUTDOWN")
              targetDb.close()
            }
          }

          describe("as target database") {
            it("should work", DbTest, DbTestH2, DbTestSqlite) {
              val sourceDb = java.sql.DriverManager
                .getConnection(s"jdbc:h2:mem:$sourceDatabaseName;DB_CLOSE_DELAY=-1")

              val targetDatabaseConnectionSqlite =
                java.sql.DriverManager.getConnection(s"jdbc:sqlite:$sqliteDbPath1")

              val sql =
                """
                |CREATE TABLE accounts (
                |  id BIGINT,
                |  name VARCHAR(254),
                |  description VARCHAR,
                |  birthday DATE,
                |  salary DOUBLE,
                |);
                |CREATE TABLE accounts2 (
                |  id BIGINT,
                |  name VARCHAR(254),
                |  description VARCHAR,
                |  birthday DATE,
                |  salary DOUBLE,
                |);
                |INSERT INTO accounts VALUES (1, 'Max Mustermann', 'Afraid of his wife...', '1980-01-01', '1500000.83');
                |INSERT INTO accounts VALUES (2, 'Eva Musterfrau', NULL, '1988-01-01', '2800000.00');
                |INSERT INTO accounts VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '1972-08-08', '3.14256');
                |INSERT INTO accounts2 VALUES (4, 'Max Mustermann', 'Afraid of his wife...', '1999-01-01', '1500000.83');
                |INSERT INTO accounts2 VALUES (5, 'Eva Musterfrau', NULL, '1981-01-01', '2800000.00');
                |INSERT INTO accounts2 VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '2001-08-08', '3.14256');
              """.stripMargin
              executeDbQuery(sourceDb, sql)

              val in: InputStream =
                getClass.getResourceAsStream("/usecases/databases/copy-between-databases-02.xml")
              val xml    = scala.io.Source.fromInputStream(in).mkString
              val dfasdl = DFASDL("MY-DFASDL", xml)
              val cookbook = Cookbook(
                "MY-COOKBOOK",
                List(dfasdl),
                Option(dfasdl),
                List(
                  Recipe.createOneToOneRecipe(
                    "ID1",
                    List(
                      MappingTransformation(
                        List(
                          ElementReference(dfasdl.id, "id"),
                          ElementReference(dfasdl.id, "name"),
                          ElementReference(dfasdl.id, "description"),
                          ElementReference(dfasdl.id, "birthday"),
                          ElementReference(dfasdl.id, "salary")
                        ),
                        List(
                          ElementReference(dfasdl.id, "id"),
                          ElementReference(dfasdl.id, "name"),
                          ElementReference(dfasdl.id, "description"),
                          ElementReference(dfasdl.id, "birthday"),
                          ElementReference(dfasdl.id, "salary")
                        ),
                        List()
                      )
                    )
                  ),
                  Recipe.createOneToOneRecipe(
                    "ID2",
                    List(
                      MappingTransformation(
                        List(
                          ElementReference(dfasdl.id, "id2"),
                          ElementReference(dfasdl.id, "name2"),
                          ElementReference(dfasdl.id, "description2"),
                          ElementReference(dfasdl.id, "birthday2"),
                          ElementReference(dfasdl.id, "salary2")
                        ),
                        List(
                          ElementReference(dfasdl.id, "id2"),
                          ElementReference(dfasdl.id, "name2"),
                          ElementReference(dfasdl.id, "description2"),
                          ElementReference(dfasdl.id, "birthday2"),
                          ElementReference(dfasdl.id, "salary2")
                        ),
                        List()
                      )
                    )
                  )
                )
              )
              val source =
                new ConnectionInformation(uri = new URI(sourceDb.getMetaData.getURL),
                                          dfasdlRef =
                                            Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL")))
              val target = new ConnectionInformation(
                uri = new URI(targetDatabaseConnectionSqlite.getMetaData.getURL),
                dfasdlRef = Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL"))
              )

              val msg = AgentStartTransformationMessage(List(source), target, cookbook)

              val dataTree = TestActorRef(
                DataTreeDocument.props(dfasdl,
                                       Option("CopyBetweenDatabasesTest"),
                                       Set.empty[String])
              )
              val dbParser = TestActorRef(
                DatabaseParser
                  .props(source, cookbook, dataTree, Option("CopyBetweenDatabasesTest"))
              )
              dbParser ! BaseParserMessages.Start

              val message = expectMsgType[ParserStatusMessage](5.seconds)
              message.status should be(ParserStatus.COMPLETED)

              val expectedDataXml = scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream(
                    "/usecases/databases/copy-between-databases-02-expected-data.xml"
                  )
                )
                .mkString
              val expectedDataTree = createTestDocumentBuilder().parse(
                new InputSource(new StringReader(expectedDataXml))
              )

              compareSequenceData("id", expectedDataTree, dataTree)
              compareSequenceData("name", expectedDataTree, dataTree)
              compareSequenceData("description", expectedDataTree, dataTree)
              compareSequenceData("birthday", expectedDataTree, dataTree)
              compareSequenceData("salary", expectedDataTree, dataTree)
              compareSequenceData("id2", expectedDataTree, dataTree)
              compareSequenceData("name2", expectedDataTree, dataTree)
              compareSequenceData("description2", expectedDataTree, dataTree)
              compareSequenceData("birthday2", expectedDataTree, dataTree)
              compareSequenceData("salary2", expectedDataTree, dataTree)

              val processor = TestActorRef(new Processor(Option("CopyBetweenDatabasesTest")))
              processor ! new StartProcessingMessage(msg, List(dataTree))
              expectMsg(Completed)

              val statement = targetDatabaseConnectionSqlite.createStatement()
              val results   = statement.executeQuery("SELECT * FROM accounts ORDER BY id")
              withClue("Data should have been written to the database!") {
                results.next() should be(true)
                results.getLong("id") should be(1)
                results.getString("name") should be("Max Mustermann")
                results.getString("description") should be("Afraid of his wife...")
                results.getDate("birthday") should be(java.sql.Date.valueOf("1980-01-01"))
                results.getDouble("salary") should be(1500000.83d)
                results.next() should be(true)
                results.getLong("id") should be(2)
                results.getString("name") should be("Eva Musterfrau")
                results.getString("description") should be(null)
                results.getDate("birthday") should be(java.sql.Date.valueOf("1988-01-01"))
                results.getDouble("salary") should be(2800000.0d)
                results.next() should be(true)
                results.getLong("id") should be(3)
                results.getString("name") should be("Dr. Evil")
                results.getString("description") should be("Afraid of Austin Powers!")
                results.getDate("birthday") should be(java.sql.Date.valueOf("1972-08-08"))
                results.getDouble("salary") should be(3.14256d)
                results.next() should be(false)
              }

              val results2 = statement.executeQuery("SELECT * FROM accounts2 ORDER BY id")
              withClue("Data should have been written to the database!") {
                results2.next() should be(true)
                results2.getLong("id") should be(3)
                results2.getString("name") should be("Dr. Evil")
                results2.getString("description") should be("Afraid of Austin Powers!")
                results2.getDate("birthday") should be(java.sql.Date.valueOf("2001-08-08"))
                results2.getDouble("salary") should be(3.14256d)
                results2.next() should be(true)
                results2.getLong("id") should be(4)
                results2.getString("name") should be("Max Mustermann")
                results2.getString("description") should be("Afraid of his wife...")
                results2.getDate("birthday") should be(java.sql.Date.valueOf("1999-01-01"))
                results2.getDouble("salary") should be(1500000.83d)
                results2.next() should be(true)
                results2.getLong("id") should be(5)
                results2.getString("name") should be("Eva Musterfrau")
                results2.getString("description") should be(null)
                results2.getDate("birthday") should be(java.sql.Date.valueOf("1981-01-01"))
                results2.getDouble("salary") should be(2800000.0d)
                results2.next() should be(false)
              }

              executeDbQuery(sourceDb, "SHUTDOWN")
              sourceDb.close()
              targetDatabaseConnectionSqlite.close()
            }
          }

          describe("as source and target database") {
            it("should work", DbTest, DbTestSqlite) {
              val sourceDatabaseConnectionSqlite =
                java.sql.DriverManager.getConnection(s"jdbc:sqlite:$sqliteDbPath1")

              val targetDatabaseConnectionSqlite =
                java.sql.DriverManager.getConnection(s"jdbc:sqlite:$sqliteDbPath2")

              val sql =
                """
                |CREATE TABLE accounts (id BIGINT, name TEXT, description TEXT, birthday TEXT, salary DOUBLE);
              """.stripMargin
              val sql2 =
                """
                |CREATE TABLE accounts2 (id BIGINT, name TEXT, description TEXT, birthday TEXT, salary DOUBLE);
              """.stripMargin
              val sql3 =
                """
                |INSERT INTO accounts VALUES (1, 'Max Mustermann', 'Afraid of his wife...', '1980-01-01', '1500000.83');
              """.stripMargin
              val sql4 =
                """
                |INSERT INTO accounts VALUES (2, 'Eva Musterfrau', NULL, '1988-01-01', '2800000.00');
              """.stripMargin
              val sql5 =
                """
                |INSERT INTO accounts VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '1972-08-08', '3.14256');
              """.stripMargin
              val sql6 =
                """
                |INSERT INTO accounts2 VALUES (4, 'Max Mustermann', 'Afraid of his wife...', '1999-01-01', '1500000.83');
              """.stripMargin
              val sql7 =
                """
                |INSERT INTO accounts2 VALUES (5, 'Eva Musterfrau', NULL, '1981-01-01', '2800000.00');
              """.stripMargin
              val sql8 =
                """
                |INSERT INTO accounts2 VALUES (3, 'Dr. Evil', 'Afraid of Austin Powers!', '2001-08-08', '3.14256');
              """.stripMargin

              executeDbQuery(sourceDatabaseConnectionSqlite, sql)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql2)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql3)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql4)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql5)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql6)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql7)
              executeDbQuery(sourceDatabaseConnectionSqlite, sql8)

              val in: InputStream =
                getClass.getResourceAsStream("/usecases/databases/copy-between-databases-02.xml")
              val xml    = scala.io.Source.fromInputStream(in).mkString
              val dfasdl = DFASDL("MY-DFASDL", xml)
              val cookbook = Cookbook(
                "MY-COOKBOOK",
                List(dfasdl),
                Option(dfasdl),
                List(
                  Recipe.createOneToOneRecipe(
                    "ID1",
                    List(
                      MappingTransformation(
                        List(
                          ElementReference(dfasdl.id, "id"),
                          ElementReference(dfasdl.id, "name"),
                          ElementReference(dfasdl.id, "description"),
                          ElementReference(dfasdl.id, "birthday"),
                          ElementReference(dfasdl.id, "salary")
                        ),
                        List(
                          ElementReference(dfasdl.id, "id"),
                          ElementReference(dfasdl.id, "name"),
                          ElementReference(dfasdl.id, "description"),
                          ElementReference(dfasdl.id, "birthday"),
                          ElementReference(dfasdl.id, "salary")
                        ),
                        List()
                      )
                    )
                  ),
                  Recipe.createOneToOneRecipe(
                    "ID2",
                    List(
                      MappingTransformation(
                        List(
                          ElementReference(dfasdl.id, "id2"),
                          ElementReference(dfasdl.id, "name2"),
                          ElementReference(dfasdl.id, "description2"),
                          ElementReference(dfasdl.id, "birthday2"),
                          ElementReference(dfasdl.id, "salary2")
                        ),
                        List(
                          ElementReference(dfasdl.id, "id2"),
                          ElementReference(dfasdl.id, "name2"),
                          ElementReference(dfasdl.id, "description2"),
                          ElementReference(dfasdl.id, "birthday2"),
                          ElementReference(dfasdl.id, "salary2")
                        ),
                        List()
                      )
                    )
                  )
                )
              )
              val source = new ConnectionInformation(
                uri = new URI(sourceDatabaseConnectionSqlite.getMetaData.getURL),
                dfasdlRef = Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL"))
              )
              val target = new ConnectionInformation(
                uri = new URI(targetDatabaseConnectionSqlite.getMetaData.getURL),
                dfasdlRef = Option(DFASDLReference("MY-COOKBOOK", "MY-DFASDL"))
              )

              val msg = AgentStartTransformationMessage(List(source), target, cookbook)

              val dataTree = TestActorRef(
                DataTreeDocument.props(dfasdl,
                                       Option("CopyBetweenDatabasesTest"),
                                       Set.empty[String])
              )
              val dbParser = TestActorRef(
                DatabaseParser
                  .props(source, cookbook, dataTree, Option("CopyBetweenDatabasesTest"))
              )
              dbParser ! BaseParserMessages.Start

              val message = expectMsgType[ParserStatusMessage](5.seconds)
              message.status should be(ParserStatus.COMPLETED)

              val expectedDataXml = scala.io.Source
                .fromInputStream(
                  getClass.getResourceAsStream(
                    "/usecases/databases/copy-between-databases-02-expected-data.xml"
                  )
                )
                .mkString
              val expectedDataTree = createTestDocumentBuilder().parse(
                new InputSource(new StringReader(expectedDataXml))
              )

              compareSequenceData("id", expectedDataTree, dataTree)
              compareSequenceData("name", expectedDataTree, dataTree)
              compareSequenceData("description", expectedDataTree, dataTree)
              compareSequenceData("birthday", expectedDataTree, dataTree)
              compareSequenceData("salary", expectedDataTree, dataTree)
              compareSequenceData("id2", expectedDataTree, dataTree)
              compareSequenceData("name2", expectedDataTree, dataTree)
              compareSequenceData("description2", expectedDataTree, dataTree)
              compareSequenceData("birthday2", expectedDataTree, dataTree)
              compareSequenceData("salary2", expectedDataTree, dataTree)

              val processor = TestActorRef(new Processor(Option("CopyBetweenDatabasesTest")))
              processor ! new StartProcessingMessage(msg, List(dataTree))
              expectMsg(Completed)

              val statement = targetDatabaseConnectionSqlite.createStatement()
              val results   = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
              withClue("Data should have been written to the database!") {
                results.next() should be(true)
                results.getLong("id") should be(1)
                results.getString("name") should be("Max Mustermann")
                results.getString("description") should be("Afraid of his wife...")
                results.getDate("birthday") should be(java.sql.Date.valueOf("1980-01-01"))
                results.getDouble("salary") should be(1500000.83d)
                results.next() should be(true)
                results.getLong("id") should be(2)
                results.getString("name") should be("Eva Musterfrau")
                results.getString("description") should be(null)
                results.getDate("birthday") should be(java.sql.Date.valueOf("1988-01-01"))
                results.getDouble("salary") should be(2800000.0d)
                results.next() should be(true)
                results.getLong("id") should be(3)
                results.getString("name") should be("Dr. Evil")
                results.getString("description") should be("Afraid of Austin Powers!")
                results.getDate("birthday") should be(java.sql.Date.valueOf("1972-08-08"))
                results.getDouble("salary") should be(3.14256d)
                results.next() should be(false)
              }

              val results2 = statement.executeQuery("SELECT * FROM accounts2 ORDER BY id")
              withClue("Data should have been written to the database!") {
                results2.next() should be(true)
                results2.getLong("id") should be(3)
                results2.getString("name") should be("Dr. Evil")
                results2.getString("description") should be("Afraid of Austin Powers!")
                results2.getDate("birthday") should be(java.sql.Date.valueOf("2001-08-08"))
                results2.getDouble("salary") should be(3.14256d)
                results2.next() should be(true)
                results2.getLong("id") should be(4)
                results2.getString("name") should be("Max Mustermann")
                results2.getString("description") should be("Afraid of his wife...")
                results2.getDate("birthday") should be(java.sql.Date.valueOf("1999-01-01"))
                results2.getDouble("salary") should be(1500000.83d)
                results2.next() should be(true)
                results2.getLong("id") should be(5)
                results2.getString("name") should be("Eva Musterfrau")
                results2.getString("description") should be(null)
                results2.getDate("birthday") should be(java.sql.Date.valueOf("1981-01-01"))
                results2.getDouble("salary") should be(2800000.0d)
                results2.next() should be(false)
              }

              sourceDatabaseConnectionSqlite.close()
              targetDatabaseConnectionSqlite.close()
            }
          }
        }
      }
    }
  }
}
