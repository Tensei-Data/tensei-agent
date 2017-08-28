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

package databases.hsqldb

import java.math.BigDecimal
import java.net.URI
import java.sql.{ SQLException, SQLIntegrityConstraintViolationException, SQLSyntaxErrorException }

import akka.testkit.{ EventFilter, TestFSMRef }
import com.wegtam.scalatest.tags.{ DbTest, DbTestHsqlDb }
import com.wegtam.tensei.adt.{ ConnectionInformation, DFASDL, DFASDLReference }
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages._
import com.wegtam.tensei.agent.writers.BaseWriter._
import com.wegtam.tensei.agent.writers.DatabaseWriterActor.DatabaseWriterData
import com.wegtam.tensei.agent.writers.{ BaseWriter, DatabaseWriterActor }
import org.scalatest.BeforeAndAfterEach

import scalaz.Scalaz._

class DatabaseWriterActorTest extends ActorSpec with BeforeAndAfterEach {

  val databaseName = "test"

  override protected def beforeEach(): Unit = {
    val c = java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$databaseName")
    val s = c.createStatement()
    s.execute("DROP SCHEMA PUBLIC CASCADE")
    s.close()
    c.close()
    super.beforeEach()
  }

  private def initializeWriter(
      con: ConnectionInformation,
      dfasdl: DFASDL
  ): TestFSMRef[BaseWriter.State, DatabaseWriterData, DatabaseWriterActor] = {
    val writer = TestFSMRef(
      new DatabaseWriterActor(con, dfasdl, Option("DatabaseWriterActorTest"))
    )
    writer.stateName should be(BaseWriter.State.Initializing)
    writer ! BaseWriterMessages.InitializeTarget
    writer ! AreYouReady
    val expectedMsg = ReadyToWork
    expectMsg(expectedMsg)
    writer
  }

  describe("DatabaseWriterActor") {
    describe("using hsqldb") {
      describe("initialize") {
        it("should create the tables", DbTest, DbTestHsqlDb) {
          val connection =
            java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

          val dfasdlFile = "/databases/generic/DatabaseWriter/simple-01.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val target = new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                                 dfasdlRef =
                                                   Option(DFASDLReference("TEST", "SIMPLE-01")))

          initializeWriter(target, dfasdl)

          val statement = connection.createStatement()
          val results = statement.executeQuery(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ACCOUNTS'"
          )
          withClue("Database table 'accounts' should be created!'") {
            results.next() should be(true)
            results.getString("TABLE_NAME") shouldEqual "ACCOUNTS"
          }
          connection.close()
        }

        it("should not create tables that are already existing", DbTest, DbTestHsqlDb) {
          val connection =
            java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")
          val statement = connection.createStatement()
          statement.execute("CREATE TABLE accounts (id DOUBLE)")

          val dfasdlFile = "/databases/generic/DatabaseWriter/simple-01.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val target = new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                                 dfasdlRef =
                                                   Option(DFASDLReference("TEST", "SIMPLE-01")))

          EventFilter.warning(occurrences = 1, start = "Table") intercept {
            initializeWriter(target, dfasdl)
          }

          connection.close()
        }

        it("should create primary keys if defined", DbTest, DbTestHsqlDb) {
          val connection =
            java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

          val dfasdlFile = "/databases/generic/DatabaseWriter/simple-01-with-primary-key.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val target = new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                                 dfasdlRef =
                                                   Option(DFASDLReference("TEST", "SIMPLE-01")))

          initializeWriter(target, dfasdl)

          val statement = connection.createStatement()
          val results = statement.executeQuery(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ACCOUNTS'"
          )
          withClue("Database table 'accounts' should be created!'") {
            results.next() should be(true)
            results.getString("TABLE_NAME") shouldEqual "ACCOUNTS"
          }
          statement.execute("INSERT INTO ACCOUNTS VALUES(1, 'John Doe', NULL, '2001-01-01', 3.14)")
          an[SQLIntegrityConstraintViolationException] should be thrownBy statement.execute(
            "INSERT INTO ACCOUNTS VALUES(1, 'Jane Doe', NULL, '2001-01-02', 2.76)"
          )
          connection.close()
        }

        it("should create auto-increment columns if defined", DbTest, DbTestHsqlDb) {
          val connection =
            java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

          val dfasdlFile = "/databases/generic/DatabaseWriter/simple-01-with-pk-and-auto-inc.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val target = new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                                 dfasdlRef =
                                                   Option(DFASDLReference("TEST", "SIMPLE-01")))

          initializeWriter(target, dfasdl)

          val statement = connection.createStatement()
          val results = statement.executeQuery(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ACCOUNTS'"
          )
          withClue("Database table 'accounts' should be created!'") {
            results.next() should be(true)
            results.getString("TABLE_NAME") shouldEqual "ACCOUNTS"
          }
          statement.execute(
            "INSERT INTO ACCOUNTS (name, description, birthday, salary) VALUES('John Doe', NULL, '2001-01-01', 3.14)"
          )
          statement.execute(
            "INSERT INTO ACCOUNTS (name, description, birthday, salary) VALUES('Jane Doe', NULL, '2001-01-01', 3.14)"
          )
          val entries = statement.executeQuery("SELECT * FROM ACCOUNTS WHERE name = 'Jane Doe'")
          withClue("Column should be incremented automatically.") {
            entries.next() should be(true)
            entries.getInt("id") should be(1)
          }
          // HSQLDB permits any statements that modify a key column.
          an[SQLSyntaxErrorException] should be thrownBy statement.execute(
            "INSERT INTO ACCOUNTS VALUES(1, 'Jane Doe', NULL, '2001-01-02', 2.76)"
          )
          connection.close()
        }

        it("should fail to create foreign keys without primary keys", DbTest, DbTestHsqlDb) {
          val connection =
            java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

          val dfasdlFile = "/databases/generic/DatabaseWriter/simple-02-with-foreign-key.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val target = new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                                 dfasdlRef =
                                                   Option(DFASDLReference("TEST", "SIMPLE-01")))

          val writer =
            TestFSMRef(new DatabaseWriterActor(target, dfasdl, Option("DatabaseWriterActorTest")))
          writer.stateName should be(BaseWriter.State.Initializing)
          EventFilter[SQLException](source = writer.path.toString, occurrences = 1) intercept {
            writer ! BaseWriterMessages.InitializeTarget
            writer ! AreYouReady
            val expectedMsg = ReadyToWork
            expectMsg(expectedMsg)
          }

          writer ! CloseWriter
          expectMsgType[WriterClosed]

          connection.close()
        }

        it("should create the unique columns if defined", DbTest, DbTestHsqlDb) {
          val connection =
            java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

          val dfasdlFile = "/databases/generic/DatabaseWriter/simple-01-with-unique.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val target = new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                                 dfasdlRef =
                                                   Option(DFASDLReference("TEST", "SIMPLE-01")))

          initializeWriter(target, dfasdl)

          val statement = connection.createStatement()
          statement.execute("INSERT INTO ACCOUNTS VALUES(1, 'John Doe', NULL, '2001-01-01', 3.14)")
          val entries = statement.executeQuery("SELECT * FROM ACCOUNTS WHERE name = 'John Doe'")
          withClue("Unique should work.") {
            entries.next() should be(true)
            entries.getString("name") should be("John Doe")

            an[java.sql.SQLIntegrityConstraintViolationException] should be thrownBy statement
              .execute("INSERT INTO ACCOUNTS VALUES(2, 'John Doe', NULL, '2001-01-02', 2.76)")
          }
          connection.close()
        }

        it("should create primary keys, foreign keys and auto increments", DbTest, DbTestHsqlDb) {
          val connection =
            java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

          val dfasdlFile =
            "/databases/generic/DatabaseWriter/simple-02-with-pk-and-fk-and-auto-inc.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val target = new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                                 dfasdlRef =
                                                   Option(DFASDLReference("TEST", "SIMPLE-01")))

          initializeWriter(target, dfasdl)

          val statement = connection.createStatement()
          val results = statement.executeQuery(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_NAME = 'ACCOUNTS' OR TABLE_NAME = 'COMPANIES') ORDER BY TABLE_NAME ASC"
          )
          withClue("Database tables should be created!'") {
            results.next() should be(true)
            results.getString("TABLE_NAME") shouldEqual "ACCOUNTS"
            results.next() should be(true)
            results.getString("TABLE_NAME") shouldEqual "COMPANIES"
          }

          statement.execute("INSERT INTO COMPANIES VALUES(1, 'Letterbox Inc.', NULL)")
          statement.execute(
            "INSERT INTO ACCOUNTS (name, description, birthday, salary, company_id) VALUES('John Doe', NULL, '2001-01-01', 3.14, 1)"
          )
          statement.execute(
            "INSERT INTO ACCOUNTS (name, description, birthday, salary, company_id) VALUES('Jane Doe', NULL, '2001-01-01', 3.14, 1)"
          )
          val entries = statement.executeQuery(
            "SELECT ACCOUNTS.id AS id, COMPANIES.name AS name FROM ACCOUNTS JOIN COMPANIES ON ACCOUNTS.company_id = COMPANIES.id WHERE ACCOUNTS.name = 'Jane Doe'"
          )
          withClue("Foreign keys should work.") {
            entries.next() should be(true)
            withClue("Column id should be auto-incremented.")(entries.getInt("id") should be(1))
            entries.getString("name") should be("Letterbox Inc.")

            an[SQLIntegrityConstraintViolationException] should be thrownBy statement.execute(
              "INSERT INTO ACCOUNTS (name, description, birthday, salary, company_id) VALUES('Jane Doe', NULL, '2001-01-02', 2.76, -1)"
            )
          }
          connection.close()
        }
      }

      describe("writing data") {
        describe("using a single sequence") {
          describe("when given data for a single row") {
            it("should write a sequence row", DbTest, DbTestHsqlDb) {
              val connection = java.sql.DriverManager
                .getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

              val dfasdlFile = "/databases/generic/DatabaseWriter/simple-01.xml"
              val xml =
                scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
              val dfasdl = new DFASDL("SIMPLE-01", xml)
              val target =
                new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                          dfasdlRef = Option(DFASDLReference("TEST", "SIMPLE-01")))

              val databaseWriter = initializeWriter(target, dfasdl)
              val msg = new WriteBatchData(
                batch = List(
                  new WriteData(1, 1, List(), Option(new WriterMessageMetaData("id"))),
                  new WriteData(2,
                                "Max Mustermann",
                                List(),
                                Option(new WriterMessageMetaData("name"))),
                  new WriteData(3,
                                "Some fancy text...",
                                List(),
                                Option(new WriterMessageMetaData("description"))),
                  new WriteData(4,
                                java.sql.Date.valueOf("1968-01-03"),
                                List(),
                                Option(new WriterMessageMetaData("birthday"))),
                  new WriteData(5,
                                new BigDecimal("1500.23"),
                                List(),
                                Option(new WriterMessageMetaData("salary")))
                )
              )
              databaseWriter ! msg

              databaseWriter ! BaseWriterMessages.CloseWriter
              val expectedMessage = BaseWriterMessages.WriterClosed("".right[String])
              expectMsg(expectedMessage)

              val statement = connection.createStatement()
              val results   = statement.executeQuery("SELECT * FROM ACCOUNTS")
              withClue("Data should have been written to the database!") {
                results.next() should be(true)
                results.getLong("id") should be(1)
                results.getString("name") should be("Max Mustermann")
                results.getString("description") should be("Some fancy text...")
                results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-03"))
                results.getDouble("salary") should be(1500.23)
              }
              connection.close()
            }
          }

          describe("when given data for multiple rows") {
            describe("without primary key") {
              it("should write all possible sequence rows", DbTest, DbTestHsqlDb) {
                val connection = java.sql.DriverManager
                  .getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

                val dfasdlFile = "/databases/generic/DatabaseWriter/simple-01.xml"
                val xml = scala.io.Source
                  .fromInputStream(getClass.getResourceAsStream(dfasdlFile))
                  .mkString
                val dfasdl = new DFASDL("SIMPLE-01", xml)
                val target =
                  new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                            dfasdlRef =
                                              Option(DFASDLReference("TEST", "SIMPLE-01")))

                val databaseWriter = initializeWriter(target, dfasdl)
                val msg = new WriteBatchData(
                  batch = List(
                    new WriteData(1, 1, List(), Option(new WriterMessageMetaData("id"))),
                    new WriteData(2,
                                  "Max Mustermann",
                                  List(),
                                  Option(new WriterMessageMetaData("name"))),
                    new WriteData(3,
                                  "Some fancy text...",
                                  List(),
                                  Option(new WriterMessageMetaData("description"))),
                    new WriteData(4,
                                  java.sql.Date.valueOf("1968-01-03"),
                                  List(),
                                  Option(new WriterMessageMetaData("birthday"))),
                    new WriteData(5,
                                  new BigDecimal("1500.23"),
                                  List(),
                                  Option(new WriterMessageMetaData("salary"))),
                    new WriteData(6, 2, List(), Option(new WriterMessageMetaData("id"))),
                    new WriteData(7,
                                  "Eva Mustermann",
                                  List(),
                                  Option(new WriterMessageMetaData("name"))),
                    new WriteData(8,
                                  "Some fancy text...",
                                  List(),
                                  Option(new WriterMessageMetaData("description"))),
                    new WriteData(9,
                                  java.sql.Date.valueOf("1968-01-01"),
                                  List(),
                                  Option(new WriterMessageMetaData("birthday"))),
                    new WriteData(10,
                                  new BigDecimal("1500.00"),
                                  List(),
                                  Option(new WriterMessageMetaData("salary"))),
                    new WriteData(11, 3, List(), Option(new WriterMessageMetaData("id"))),
                    new WriteData(12,
                                  "Dr. Evil",
                                  List(),
                                  Option(new WriterMessageMetaData("name"))),
                    new WriteData(13,
                                  "Beware of Austin Powers!",
                                  List(),
                                  Option(new WriterMessageMetaData("description"))),
                    new WriteData(14,
                                  java.sql.Date.valueOf("1968-08-08"),
                                  List(),
                                  Option(new WriterMessageMetaData("birthday"))),
                    new WriteData(15,
                                  new BigDecimal("1500000.00"),
                                  List(),
                                  Option(new WriterMessageMetaData("salary")))
                  )
                )
                databaseWriter ! msg

                databaseWriter ! BaseWriterMessages.CloseWriter
                val expectedMessage = BaseWriterMessages.WriterClosed("".right[String])
                expectMsg(expectedMessage)

                val statement = connection.createStatement()
                val results   = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
                withClue("Data should have been written to the database!") {
                  results.next() should be(true)
                  results.getLong("id") should be(1)
                  results.getString("name") should be("Max Mustermann")
                  results.getString("description") should be("Some fancy text...")
                  results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-03"))
                  results.getDouble("salary") should be(1500.23)
                  results.next() should be(true)
                  results.getLong("id") should be(2)
                  results.getString("name") should be("Eva Mustermann")
                  results.getString("description") should be("Some fancy text...")
                  results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
                  results.getDouble("salary") should be(1500.00)
                  results.next() should be(true)
                  results.getLong("id") should be(3)
                  results.getString("name") should be("Dr. Evil")
                  results.getString("description") should be("Beware of Austin Powers!")
                  results.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
                  results.getDouble("salary") should be(1500000.00)
                }
                connection.close()
              }
            }

            describe("with primary key") {
              it("should write new and update existing rows", DbTest, DbTestHsqlDb) {
                val connection = java.sql.DriverManager
                  .getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

                val dfasdlFile = "/databases/generic/DatabaseWriter/simple-01-with-primary-key.xml"
                val xml = scala.io.Source
                  .fromInputStream(getClass.getResourceAsStream(dfasdlFile))
                  .mkString
                val dfasdl = new DFASDL("SIMPLE-01", xml)
                val target =
                  new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                            dfasdlRef =
                                              Option(DFASDLReference("TEST", "SIMPLE-01")))

                val databaseWriter = initializeWriter(target, dfasdl)

                val ps = connection.prepareStatement(
                  "INSERT INTO accounts (id, name, description, birthday, salary) VALUES(?, ?, ?, ?, ?)"
                )
                ps.setInt(1, 1)
                ps.setString(2, "Max Mustermann")
                ps.setString(3, "Some fancy text...")
                ps.setDate(4, java.sql.Date.valueOf("1968-01-03"))
                ps.setBigDecimal(5, new BigDecimal("1500.23"))
                ps.execute()

                val msg = new WriteBatchData(
                  batch = List(
                    new WriteData(1, 2, List(), Option(new WriterMessageMetaData("id"))),
                    new WriteData(2,
                                  "Eva Mustermann",
                                  List(),
                                  Option(new WriterMessageMetaData("name"))),
                    new WriteData(3,
                                  "Some fancy text...",
                                  List(),
                                  Option(new WriterMessageMetaData("description"))),
                    new WriteData(4,
                                  java.sql.Date.valueOf("1968-01-01"),
                                  List(),
                                  Option(new WriterMessageMetaData("birthday"))),
                    new WriteData(5,
                                  new BigDecimal("1500.00"),
                                  List(),
                                  Option(new WriterMessageMetaData("salary"))),
                    new WriteData(6, 3, List(), Option(new WriterMessageMetaData("id"))),
                    new WriteData(7,
                                  "Dr. Evil",
                                  List(),
                                  Option(new WriterMessageMetaData("name"))),
                    new WriteData(8,
                                  "Beware of Austin Powers!",
                                  List(),
                                  Option(new WriterMessageMetaData("description"))),
                    new WriteData(9,
                                  java.sql.Date.valueOf("1968-08-08"),
                                  List(),
                                  Option(new WriterMessageMetaData("birthday"))),
                    new WriteData(10,
                                  new BigDecimal("1500000.00"),
                                  List(),
                                  Option(new WriterMessageMetaData("salary"))),
                    new WriteData(11, 1, List(), Option(new WriterMessageMetaData("id"))),
                    new WriteData(12,
                                  "Lord Fancy Pants",
                                  List(),
                                  Option(new WriterMessageMetaData("name"))),
                    new WriteData(13,
                                  "An updated description text.",
                                  List(),
                                  Option(new WriterMessageMetaData("description"))),
                    new WriteData(14,
                                  java.sql.Date.valueOf("1968-04-01"),
                                  List(),
                                  Option(new WriterMessageMetaData("birthday"))),
                    new WriteData(15,
                                  new BigDecimal("999.97"),
                                  List(),
                                  Option(new WriterMessageMetaData("salary")))
                  )
                )
                databaseWriter ! msg

                databaseWriter ! BaseWriterMessages.CloseWriter
                val expectedMessage = BaseWriterMessages.WriterClosed("".right[String])
                expectMsg(expectedMessage)

                val statement = connection.createStatement()

                withClue("The exact number of rows should have been written!") {
                  val count = statement.executeQuery("SELECT COUNT(*) FROM ACCOUNTS")
                  count.next() should be(true)
                  count.getInt(1) shouldEqual 3
                }

                withClue("Data should have been written to the database!") {
                  val results = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
                  results.next() should be(true)
                  results.getLong("id") should be(1)
                  results.getString("name") should be("Lord Fancy Pants")
                  results.getString("description") should be("An updated description text.")
                  results.getDate("birthday") should be(java.sql.Date.valueOf("1968-04-01"))
                  results.getDouble("salary") should be(999.97)
                  results.next() should be(true)
                  results.getLong("id") should be(2)
                  results.getString("name") should be("Eva Mustermann")
                  results.getString("description") should be("Some fancy text...")
                  results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
                  results.getDouble("salary") should be(1500.00)
                  results.next() should be(true)
                  results.getLong("id") should be(3)
                  results.getString("name") should be("Dr. Evil")
                  results.getString("description") should be("Beware of Austin Powers!")
                  results.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
                  results.getDouble("salary") should be(1500000.00)
                }
                connection.close()
              }
            }
          }
        }

        describe("using multiple sequences") {
          describe("when given data for multiple rows") {
            it("should write all possible sequence rows", DbTest, DbTestHsqlDb) {
              val connection = java.sql.DriverManager
                .getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

              val dfasdlFile = "/databases/generic/DatabaseWriter/simple-02.xml"
              val xml =
                scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
              val dfasdl = new DFASDL("SIMPLE-01", xml)
              val target =
                new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                          dfasdlRef = Option(DFASDLReference("TEST", "SIMPLE-01")))

              val databaseWriter = initializeWriter(target, dfasdl)
              val msg = new WriteBatchData(
                batch = List(
                  new WriteData(1, 1, List(), Option(new WriterMessageMetaData("id"))),
                  new WriteData(2,
                                "Max Mustermann",
                                List(),
                                Option(new WriterMessageMetaData("name"))),
                  new WriteData(3,
                                "Some fancy text...",
                                List(),
                                Option(new WriterMessageMetaData("description"))),
                  new WriteData(4,
                                java.sql.Date.valueOf("1968-01-03"),
                                List(),
                                Option(new WriterMessageMetaData("birthday"))),
                  new WriteData(5,
                                new BigDecimal("1500.23"),
                                List(),
                                Option(new WriterMessageMetaData("salary"))),
                  new WriteData(6, 2, List(), Option(new WriterMessageMetaData("id"))),
                  new WriteData(7,
                                "Eva Mustermann",
                                List(),
                                Option(new WriterMessageMetaData("name"))),
                  new WriteData(8,
                                "Some fancy text...",
                                List(),
                                Option(new WriterMessageMetaData("description"))),
                  new WriteData(9,
                                java.sql.Date.valueOf("1968-01-01"),
                                List(),
                                Option(new WriterMessageMetaData("birthday"))),
                  new WriteData(10,
                                new BigDecimal("1500.00"),
                                List(),
                                Option(new WriterMessageMetaData("salary"))),
                  new WriteData(11, 1, List(), Option(new WriterMessageMetaData("id2"))),
                  new WriteData(12,
                                "Dr. Evil",
                                List(),
                                Option(new WriterMessageMetaData("name2"))),
                  new WriteData(13,
                                "Beware of Austin Powers!",
                                List(),
                                Option(new WriterMessageMetaData("description2"))),
                  new WriteData(14,
                                java.sql.Date.valueOf("1968-08-08"),
                                List(),
                                Option(new WriterMessageMetaData("birthday2"))),
                  new WriteData(15,
                                new BigDecimal("1500000.00"),
                                List(),
                                Option(new WriterMessageMetaData("salary2"))),
                  new WriteData(16, 2, List(), Option(new WriterMessageMetaData("id2"))),
                  new WriteData(17,
                                "Eva Mustermann",
                                List(),
                                Option(new WriterMessageMetaData("name2"))),
                  new WriteData(18,
                                "Some fancy text...",
                                List(),
                                Option(new WriterMessageMetaData("description2"))),
                  new WriteData(19,
                                java.sql.Date.valueOf("1968-01-01"),
                                List(),
                                Option(new WriterMessageMetaData("birthday2"))),
                  new WriteData(20,
                                new BigDecimal("1500.00"),
                                List(),
                                Option(new WriterMessageMetaData("salary2"))),
                  new WriteData(21, 3, List(), Option(new WriterMessageMetaData("id2"))),
                  new WriteData(22,
                                "Dr. Evil",
                                List(),
                                Option(new WriterMessageMetaData("name2"))),
                  new WriteData(23,
                                "Beware of Austin Powers!",
                                List(),
                                Option(new WriterMessageMetaData("description2"))),
                  new WriteData(24,
                                java.sql.Date.valueOf("1968-08-08"),
                                List(),
                                Option(new WriterMessageMetaData("birthday2"))),
                  new WriteData(25,
                                new BigDecimal("1500000.00"),
                                List(),
                                Option(new WriterMessageMetaData("salary2")))
                )
              )
              databaseWriter ! msg

              databaseWriter ! BaseWriterMessages.CloseWriter
              val expectedMessage = BaseWriterMessages.WriterClosed("".right[String])
              expectMsg(expectedMessage)

              val statement = connection.createStatement()
              val results   = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
              withClue("Data should have been written to the database!") {
                results.next() should be(true)
                results.getLong("id") should be(1)
                results.getString("name") should be("Max Mustermann")
                results.getString("description") should be("Some fancy text...")
                results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-03"))
                results.getDouble("salary") should be(1500.23)
                results.next() should be(true)
                results.getLong("id") should be(2)
                results.getString("name") should be("Eva Mustermann")
                results.getString("description") should be("Some fancy text...")
                results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
                results.getDouble("salary") should be(1500.00)
                results.next() should be(false)
              }

              val results2 = statement.executeQuery("SELECT * FROM ACCOUNTS2 ORDER BY id")
              withClue("Data should have been written to the database!") {
                results2.next() should be(true)
                results2.getLong("id") should be(1)
                results2.getString("name") should be("Dr. Evil")
                results2.getString("description") should be("Beware of Austin Powers!")
                results2.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
                results2.getDouble("salary") should be(1500000.00)
                results2.next() should be(true)
                results2.getLong("id") should be(2)
                results2.getString("name") should be("Eva Mustermann")
                results2.getString("description") should be("Some fancy text...")
                results2.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
                results2.getDouble("salary") should be(1500.00)
                results2.next() should be(true)
                results2.getLong("id") should be(3)
                results2.getString("name") should be("Dr. Evil")
                results2.getString("description") should be("Beware of Austin Powers!")
                results2.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
                results2.getDouble("salary") should be(1500000.00)
                results2.next() should be(false)
              }

              connection.close()
            }
          }

          describe("when given data for multiple rows in random order") {
            it("should write all possible sequence rows", DbTest, DbTestHsqlDb) {
              val connection = java.sql.DriverManager
                .getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

              val dfasdlFile = "/databases/generic/DatabaseWriter/simple-02.xml"
              val xml =
                scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
              val dfasdl = new DFASDL("SIMPLE-01", xml)
              val target =
                new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                          dfasdlRef = Option(DFASDLReference("TEST", "SIMPLE-01")))

              val databaseWriter = initializeWriter(target, dfasdl)
              val msg = new WriteBatchData(
                batch = List(
                  new WriteData(1, 1, List(), Option(new WriterMessageMetaData("id"))),
                  new WriteData(5,
                                new BigDecimal("1500.23"),
                                List(),
                                Option(new WriterMessageMetaData("salary"))),
                  new WriteData(2,
                                "Max Mustermann",
                                List(),
                                Option(new WriterMessageMetaData("name"))),
                  new WriteData(3,
                                "Some fancy text...",
                                List(),
                                Option(new WriterMessageMetaData("description"))),
                  new WriteData(6, 2, List(), Option(new WriterMessageMetaData("id"))),
                  new WriteData(7,
                                "Eva Mustermann",
                                List(),
                                Option(new WriterMessageMetaData("name"))),
                  new WriteData(4,
                                java.sql.Date.valueOf("1968-01-03"),
                                List(),
                                Option(new WriterMessageMetaData("birthday"))),
                  new WriteData(8,
                                "Some fancy text...",
                                List(),
                                Option(new WriterMessageMetaData("description"))),
                  new WriteData(11, 1, List(), Option(new WriterMessageMetaData("id2"))),
                  new WriteData(12,
                                "Dr. Evil",
                                List(),
                                Option(new WriterMessageMetaData("name2"))),
                  new WriteData(13,
                                "Beware of Austin Powers!",
                                List(),
                                Option(new WriterMessageMetaData("description2"))),
                  new WriteData(9,
                                java.sql.Date.valueOf("1968-01-01"),
                                List(),
                                Option(new WriterMessageMetaData("birthday"))),
                  new WriteData(10,
                                new BigDecimal("1500.00"),
                                List(),
                                Option(new WriterMessageMetaData("salary"))),
                  new WriteData(14,
                                java.sql.Date.valueOf("1968-08-08"),
                                List(),
                                Option(new WriterMessageMetaData("birthday2"))),
                  new WriteData(15,
                                new BigDecimal("1500000.00"),
                                List(),
                                Option(new WriterMessageMetaData("salary2"))),
                  new WriteData(23,
                                "Beware of Austin Powers!",
                                List(),
                                Option(new WriterMessageMetaData("description2"))),
                  new WriteData(24,
                                java.sql.Date.valueOf("1968-08-08"),
                                List(),
                                Option(new WriterMessageMetaData("birthday2"))),
                  new WriteData(16, 2, List(), Option(new WriterMessageMetaData("id2"))),
                  new WriteData(18,
                                "Some fancy text...",
                                List(),
                                Option(new WriterMessageMetaData("description2"))),
                  new WriteData(17,
                                "Eva Mustermann",
                                List(),
                                Option(new WriterMessageMetaData("name2"))),
                  new WriteData(19,
                                java.sql.Date.valueOf("1968-01-01"),
                                List(),
                                Option(new WriterMessageMetaData("birthday2"))),
                  new WriteData(20,
                                new BigDecimal("1500.00"),
                                List(),
                                Option(new WriterMessageMetaData("salary2"))),
                  new WriteData(22,
                                "Dr. Evil",
                                List(),
                                Option(new WriterMessageMetaData("name2"))),
                  new WriteData(25,
                                new BigDecimal("1500000.00"),
                                List(),
                                Option(new WriterMessageMetaData("salary2"))),
                  new WriteData(21, 3, List(), Option(new WriterMessageMetaData("id2")))
                )
              )
              databaseWriter ! msg

              databaseWriter ! BaseWriterMessages.CloseWriter
              val expectedMessage = BaseWriterMessages.WriterClosed("".right[String])
              expectMsg(expectedMessage)

              val statement = connection.createStatement()
              val results   = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
              withClue("Data should have been written to the database!") {
                results.next() should be(true)
                results.getLong("id") should be(1)
                results.getString("name") should be("Max Mustermann")
                results.getString("description") should be("Some fancy text...")
                results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-03"))
                results.getDouble("salary") should be(1500.23)
                results.next() should be(true)
                results.getLong("id") should be(2)
                results.getString("name") should be("Eva Mustermann")
                results.getString("description") should be("Some fancy text...")
                results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
                results.getDouble("salary") should be(1500.00)
                results.next() should be(false)
              }

              val results2 = statement.executeQuery("SELECT * FROM ACCOUNTS2 ORDER BY id")
              withClue("Data should have been written to the database!") {
                results2.next() should be(true)
                results2.getLong("id") should be(1)
                results2.getString("name") should be("Dr. Evil")
                results2.getString("description") should be("Beware of Austin Powers!")
                results2.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
                results2.getDouble("salary") should be(1500000.00)
                results2.next() should be(true)
                results2.getLong("id") should be(2)
                results2.getString("name") should be("Eva Mustermann")
                results2.getString("description") should be("Some fancy text...")
                results2.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
                results2.getDouble("salary") should be(1500.00)
                results2.next() should be(true)
                results2.getLong("id") should be(3)
                results2.getString("name") should be("Dr. Evil")
                results2.getString("description") should be("Beware of Austin Powers!")
                results2.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
                results2.getDouble("salary") should be(1500000.00)
                results2.next() should be(false)
              }

              connection.close()
            }
          }
        }

        describe("when retrieving ordered column data") {
          it("should write the columns in correct order", DbTest, DbTestHsqlDb) {
            val connection =
              java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

            val dfasdlFile = "/databases/generic/DatabaseWriter/simple-01.xml"
            val xml =
              scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
            val dfasdl = new DFASDL("SIMPLE-01", xml)
            val target = new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                                   dfasdlRef =
                                                     Option(DFASDLReference("TEST", "SIMPLE-01")))

            val databaseWriter = initializeWriter(target, dfasdl)
            val msg = new WriteBatchData(
              batch = List(
                new WriteData(1, 1, List(), Option(new WriterMessageMetaData("id"))),
                new WriteData(2,
                              "Max Mustermann",
                              List(),
                              Option(new WriterMessageMetaData("name"))),
                new WriteData(3,
                              "Some fancy text...",
                              List(),
                              Option(new WriterMessageMetaData("description"))),
                new WriteData(4,
                              java.sql.Date.valueOf("1968-01-03"),
                              List(),
                              Option(new WriterMessageMetaData("birthday"))),
                new WriteData(5,
                              new BigDecimal("1500.23"),
                              List(),
                              Option(new WriterMessageMetaData("salary")))
              )
            )
            databaseWriter ! msg

            databaseWriter ! BaseWriterMessages.CloseWriter
            val expectedMessage = BaseWriterMessages.WriterClosed("".right[String])
            expectMsg(expectedMessage)

            val statement = connection.createStatement()
            val results   = statement.executeQuery("SELECT * FROM ACCOUNTS")
            withClue("Data should have been written to the database!") {
              results.next() should be(true)
              results.getLong("id") should be(1)
              results.getString("name") should be("Max Mustermann")
              results.getString("description") should be("Some fancy text...")
              results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-03"))
              results.getDouble("salary") should be(1500.23)
            }
            connection.close()
          }
        }

        describe("when retrieving unordered column data") {
          it("should write the columns in correct order", DbTest, DbTestHsqlDb) {
            val connection =
              java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

            val dfasdlFile = "/databases/generic/DatabaseWriter/simple-01.xml"
            val xml =
              scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
            val dfasdl = new DFASDL("SIMPLE-01", xml)
            val target = new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                                   dfasdlRef =
                                                     Option(DFASDLReference("TEST", "SIMPLE-01")))

            val databaseWriter = initializeWriter(target, dfasdl)
            val msg = new WriteBatchData(
              batch = List(
                new WriteData(1,
                              java.sql.Date.valueOf("1968-01-03"),
                              List(),
                              Option(new WriterMessageMetaData("birthday"))),
                new WriteData(2,
                              "Max Mustermann",
                              List(),
                              Option(new WriterMessageMetaData("name"))),
                new WriteData(3, 1, List(), Option(new WriterMessageMetaData("id"))),
                new WriteData(4,
                              new BigDecimal("1500.23"),
                              List(),
                              Option(new WriterMessageMetaData("salary"))),
                new WriteData(5,
                              "Some fancy text...",
                              List(),
                              Option(new WriterMessageMetaData("description")))
              )
            )
            databaseWriter ! msg

            databaseWriter ! BaseWriterMessages.CloseWriter
            val expectedMessage = BaseWriterMessages.WriterClosed("".right[String])
            expectMsg(expectedMessage)

            val statement = connection.createStatement()
            val results   = statement.executeQuery("SELECT * FROM ACCOUNTS")
            withClue("Data should have been written to the database!") {
              results.next() should be(true)
              results.getLong("id") should be(1)
              results.getString("name") should be("Max Mustermann")
              results.getString("description") should be("Some fancy text...")
              results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-03"))
              results.getDouble("salary") should be(1500.23)
            }
            connection.close()
          }
        }
      }

      describe("using auto increment columns") {
        it("should collect the written auto increment values", DbTest, DbTestHsqlDb) {
          val connection =
            java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

          val dfasdlFile =
            "/databases/generic/DatabaseWriter/simple-01-with-pk-and-auto-inc-not-first-column.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val target = new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                                 dfasdlRef =
                                                   Option(DFASDLReference("TEST", "SIMPLE-01")))

          val databaseWriter = initializeWriter(target, dfasdl)

          val statement = connection.createStatement()
          statement.execute(
            "INSERT INTO accounts (name, description, birthday, salary) VALUES('Max Mustermann', 'Some fancy text...', '1968-01-03', 1500.23)"
          )
          withClue("The manually inserted data should have the correct id!") {
            val rs = statement.executeQuery("SELECT MAX(id) FROM accounts")
            rs.next() should be(true)
            rs.getLong(1) should be(0L)
            rs.close()
          }

          val msg = new WriteBatchData(
            batch = List(
              new WriteData(1, None, List(), Option(new WriterMessageMetaData("id"))),
              new WriteData(2,
                            "Eva Mustermann",
                            List(),
                            Option(new WriterMessageMetaData("name"))),
              new WriteData(3,
                            "Some fancy text...",
                            List(),
                            Option(new WriterMessageMetaData("description"))),
              new WriteData(4,
                            java.sql.Date.valueOf("1968-01-01"),
                            List(),
                            Option(new WriterMessageMetaData("birthday"))),
              new WriteData(5,
                            new java.math.BigDecimal("1500.00"),
                            List(),
                            Option(new WriterMessageMetaData("salary"))),
              new WriteData(6, None, List(), Option(new WriterMessageMetaData("id"))),
              new WriteData(7, "Dr. Evil", List(), Option(new WriterMessageMetaData("name"))),
              new WriteData(8,
                            "Beware of Austin Powers!",
                            List(),
                            Option(new WriterMessageMetaData("description"))),
              new WriteData(9,
                            java.sql.Date.valueOf("1968-08-08"),
                            List(),
                            Option(new WriterMessageMetaData("birthday"))),
              new WriteData(10,
                            new java.math.BigDecimal("1500000.00"),
                            List(),
                            Option(new WriterMessageMetaData("salary"))),
              new WriteData(11, 0L, List(), Option(new WriterMessageMetaData("id"))),
              new WriteData(12,
                            "Lord Fancy Pants",
                            List(),
                            Option(new WriterMessageMetaData("name"))),
              new WriteData(13,
                            "An updated description text.",
                            List(),
                            Option(new WriterMessageMetaData("description"))),
              new WriteData(14,
                            java.sql.Date.valueOf("1968-04-01"),
                            List(),
                            Option(new WriterMessageMetaData("birthday"))),
              new WriteData(15,
                            new java.math.BigDecimal("999.97"),
                            List(),
                            Option(new WriterMessageMetaData("salary")))
            )
          )

          EventFilter.debug(occurrences = 1, message = "GENERATED INSERT KEY: 2") intercept {
            databaseWriter ! msg

            databaseWriter ! BaseWriterMessages.CloseWriter
            val expectedMessage = BaseWriterMessages.WriterClosed("".right[String])
            expectMsg(expectedMessage)
          }

          withClue("The exact number of rows should have been written!") {
            val count = statement.executeQuery("SELECT COUNT(*) FROM ACCOUNTS")
            count.next() should be(true)
            count.getInt(1) shouldEqual 3
          }

          withClue("Data should have been written to the database!") {
            val results = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
            results.next() should be(true)
            results.getLong("id") should be(0)
            results.getString("name") should be("Lord Fancy Pants")
            results.getString("description") should be("An updated description text.")
            results.getDate("birthday") should be(java.sql.Date.valueOf("1968-04-01"))
            results.getDouble("salary") should be(999.97)
            results.next() should be(true)
            results.getLong("id") should be(1)
            results.getString("name") should be("Eva Mustermann")
            results.getString("description") should be("Some fancy text...")
            results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
            results.getDouble("salary") should be(1500.00)
            results.next() should be(true)
            results.getLong("id") should be(2)
            results.getString("name") should be("Dr. Evil")
            results.getString("description") should be("Beware of Austin Powers!")
            results.getDate("birthday") should be(java.sql.Date.valueOf("1968-08-08"))
            results.getDouble("salary") should be(1500000.00)
          }
          connection.close()
        }
      }

      describe("using NULL values") {
        it("should set the correct parameter columns to null", DbTest, DbTestHsqlDb) {
          val connection =
            java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$databaseName;shutdown=true")

          val dfasdlFile = "/databases/generic/DatabaseWriter/simple-01.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val target = new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                                 dfasdlRef =
                                                   Option(DFASDLReference("TEST", "SIMPLE-01")))

          val databaseWriter = initializeWriter(target, dfasdl)

          val msg = new WriteBatchData(
            batch = List(
              new WriteData(1, 2L, List(), Option(new WriterMessageMetaData("id"))),
              new WriteData(2,
                            "Eva Mustermann",
                            List(),
                            Option(new WriterMessageMetaData("name"))),
              new WriteData(3, None, List(), Option(new WriterMessageMetaData("description"))),
              new WriteData(4,
                            java.sql.Date.valueOf("1968-01-01"),
                            List(),
                            Option(new WriterMessageMetaData("birthday"))),
              new WriteData(5,
                            new java.math.BigDecimal("1500.00"),
                            List(),
                            Option(new WriterMessageMetaData("salary"))),
              new WriteData(6, 3L, List(), Option(new WriterMessageMetaData("id"))),
              new WriteData(7, "Dr. Evil", List(), Option(new WriterMessageMetaData("name"))),
              new WriteData(8,
                            "Beware of Austin Powers!",
                            List(),
                            Option(new WriterMessageMetaData("description"))),
              new WriteData(9, None, List(), Option(new WriterMessageMetaData("birthday"))),
              new WriteData(10,
                            new java.math.BigDecimal("1500000.00"),
                            List(),
                            Option(new WriterMessageMetaData("salary"))),
              new WriteData(11, 1L, List(), Option(new WriterMessageMetaData("id"))),
              new WriteData(12,
                            "Lord Fancy Pants",
                            List(),
                            Option(new WriterMessageMetaData("name"))),
              new WriteData(13,
                            "An updated description text.",
                            List(),
                            Option(new WriterMessageMetaData("description"))),
              new WriteData(14,
                            java.sql.Date.valueOf("1968-04-01"),
                            List(),
                            Option(new WriterMessageMetaData("birthday"))),
              new WriteData(15, None, List(), Option(new WriterMessageMetaData("salary")))
            )
          )

          databaseWriter ! msg
          databaseWriter ! BaseWriterMessages.CloseWriter
          val expectedMessage = BaseWriterMessages.WriterClosed("".right[String])
          expectMsg(expectedMessage)

          val statement = connection.createStatement()

          withClue("The exact number of rows should have been written!") {
            val count = statement.executeQuery("SELECT COUNT(*) FROM ACCOUNTS")
            count.next() should be(true)
            count.getInt(1) shouldEqual 3
          }

          withClue("Data should have been written to the database!") {
            val results = statement.executeQuery("SELECT * FROM ACCOUNTS ORDER BY id")
            results.next() should be(true)
            results.getLong("id") should be(1)
            results.getString("name") should be("Lord Fancy Pants")
            results.getString("description") should be("An updated description text.")
            results.getDate("birthday") should be(java.sql.Date.valueOf("1968-04-01"))
            results.getDouble("salary") should be(0.0)
            results.next() should be(true)
            results.getLong("id") should be(2)
            results.getString("name") should be("Eva Mustermann")
            results.getString("description") should be(null)
            results.getDate("birthday") should be(java.sql.Date.valueOf("1968-01-01"))
            results.getDouble("salary") should be(1500.00)
            results.next() should be(true)
            results.getLong("id") should be(3)
            results.getString("name") should be("Dr. Evil")
            results.getString("description") should be("Beware of Austin Powers!")
            results.getDate("birthday") should be(null)
            results.getDouble("salary") should be(1500000.00)
          }
          connection.close()
        }
      }
    }
  }
}
