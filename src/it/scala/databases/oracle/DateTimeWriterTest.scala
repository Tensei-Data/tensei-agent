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

package databases.oracle

import java.net.URI
import java.time.{ LocalDate, LocalDateTime, LocalTime }
import java.time.format.DateTimeFormatter

import akka.testkit.TestFSMRef
import com.wegtam.scalatest.tags.{ DbTest, DbTestOracle }
import com.wegtam.tensei.adt.{ ConnectionInformation, DFASDL, DFASDLReference }
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.writers.BaseWriter.{ BaseWriterMessages, WriterMessageMetaData }
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages._
import com.wegtam.tensei.agent.writers.{ BaseWriter, DatabaseWriterActor }
import com.wegtam.tensei.agent.writers.DatabaseWriterActor.DatabaseWriterData
import org.scalatest.BeforeAndAfterEach

import scala.collection.mutable.ListBuffer
import scalaz.Scalaz._

class DateTimeWriterTest extends ActorSpec with BeforeAndAfterEach {

  val databaseHost = testConfig.getString("oracle.host")
  val databasePort = testConfig.getInt("oracle.port")
  val databaseName = testConfig.getString("oracle.target-db.name")
  val databaseUser = testConfig.getString("oracle.target-db.user")
  val databasePass = testConfig.getString("oracle.target-db.pass")

  /**
    * Initialise the test database.
    */
  override protected def beforeEach(): Unit = {
    withClue("Reseting database before test.") {
      val connection = java.sql.DriverManager.getConnection(
        s"jdbc:oracle:thin:@$databaseHost:$databasePort/$databaseName",
        databaseUser,
        databasePass
      )
      val s = connection.createStatement()
      val results = s.executeQuery(
        s"SELECT UPPER(TABLE_NAME) AS TABLE_NAME FROM USER_TABLES WHERE (UPPER(TABLE_NAME) = 'ACCOUNTS' OR UPPER(TABLE_NAME) = 'ACCOUNTS2' OR UPPER(TABLE_NAME) = 'COMPANIES') ORDER BY TABLE_NAME ASC"
      )
      val drops = new ListBuffer[String]
      while (results.next()) {
        drops += results.getString(1)
      }
      results.close()
      drops.foreach(t => s.execute(s"DROP TABLE $t"))
      s.close()
      connection.close()
    }
    super.beforeEach()
  }

  /**
    * Remove the test database.
    */
  override protected def afterEach(): Unit = {
    withClue("Reseting database after test.") {
      val connection = java.sql.DriverManager.getConnection(
        s"jdbc:oracle:thin:@$databaseHost:$databasePort/$databaseName",
        databaseUser,
        databasePass
      )
      val s = connection.createStatement()
      val results = s.executeQuery(
        s"SELECT UPPER(TABLE_NAME) AS TABLE_NAME FROM USER_TABLES WHERE (UPPER(TABLE_NAME) = 'ACCOUNTS' OR UPPER(TABLE_NAME) = 'ACCOUNTS2' OR UPPER(TABLE_NAME) = 'COMPANIES') ORDER BY TABLE_NAME ASC"
      )
      val drops = new ListBuffer[String]
      while (results.next()) {
        drops += results.getString(1)
      }
      results.close()
      drops.foreach(t => s.execute(s"DROP TABLE $t"))
      s.close()
      connection.close()
    }
    super.afterEach()
  }

  /**
    * Initialise the database writer actor.
    *
    * @param con The connection information for the target database.
    * @param dfasdl The target DFASDL.
    * @return The test actor ref of the writer actor.
    */
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

  describe("DateTimeWriterTest") {
    describe("using Oracle") {
      describe("initialize") {
        it("should create the tables", DbTest, DbTestOracle) {
          val connection = java.sql.DriverManager.getConnection(
            s"jdbc:oracle:thin:@$databaseHost:$databasePort/$databaseName",
            databaseUser,
            databasePass
          )

          val dfasdlFile = "/databases/generic/DatabaseWriter/simple-date-time-01.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-DATE-TIME-01", xml)
          val target = new ConnectionInformation(
            uri = new URI(connection.getMetaData.getURL),
            dfasdlRef = Option(DFASDLReference("TEST", "SIMPLE-DATE-TIME-01")),
            username = Option(databaseUser),
            password = Option(databasePass)
          )

          val databaseWriter = initializeWriter(target, dfasdl)

          databaseWriter ! CloseWriter
          expectMsgType[WriterClosed]

          val statement = connection.createStatement()
          val results = statement.executeQuery(
            s"SELECT UPPER(TABLE_NAME) AS TABLE_NAME FROM USER_TABLES WHERE UPPER(TABLE_NAME) = 'ACCOUNTS' ORDER BY TABLE_NAME ASC"
          )
          withClue("Database table 'accounts' should be created!'") {
            results.getFetchSize
            results.next() should be(true)
            results.getString("TABLE_NAME") shouldEqual "ACCOUNTS"
          }
          connection.close()
        }
      }

      describe("writing data") {
        describe("using a single sequence") {
          describe("given data for multiple rows") {
            it("should write all data correctly", DbTest, DbTestOracle) {
              val connection = java.sql.DriverManager.getConnection(
                s"jdbc:oracle:thin:@$databaseHost:$databasePort/$databaseName",
                databaseUser,
                databasePass
              )

              val dfasdlFile = "/databases/generic/DatabaseWriter/simple-date-time-01.xml"
              val xml =
                scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
              val dfasdl = new DFASDL("SIMPLE-DATE-TIME-01", xml)
              val target = new ConnectionInformation(
                uri = new URI(connection.getMetaData.getURL),
                dfasdlRef = Option(DFASDLReference("TEST", "SIMPLE-DATE-TIME-01")),
                username = Option(databaseUser),
                password = Option(databasePass)
              )

              val databaseWriter = initializeWriter(target, dfasdl)
              val msg = new WriteBatchData(
                batch = List(
                  new WriteData(1, 1L, List(), Option(new WriterMessageMetaData("id"))),
                  new WriteData(2,
                                java.sql.Date.valueOf("2016-04-27"),
                                List(),
                                Option(new WriterMessageMetaData("created"))),
                  new WriteData(3,
                                java.sql.Timestamp.valueOf("2016-04-27 12:12:12"),
                                List(),
                                Option(new WriterMessageMetaData("updated"))),
                  new WriteData(4,
                                java.sql.Time.valueOf("12:12:12"),
                                List(),
                                Option(new WriterMessageMetaData("accessed"))),
                  new WriteData(
                    5,
                    java.sql.Date
                      .valueOf(
                        LocalDate.parse("27.04.2016", DateTimeFormatter.ofPattern("dd.MM.yyyy"))
                      )
                      .toString,
                    List(),
                    Option(new WriterMessageMetaData("created2"))
                  ),
                  new WriteData(
                    6,
                    java.sql.Timestamp
                      .valueOf(
                        LocalDateTime.parse("27.04.2016 13:22:22",
                                            DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"))
                      )
                      .toString,
                    List(),
                    Option(new WriterMessageMetaData("updated2"))
                  ),
                  new WriteData(
                    7,
                    java.sql.Time
                      .valueOf(
                        LocalTime.parse("13:23:12", DateTimeFormatter.ofPattern("HH:mm:ss"))
                      )
                      .toString,
                    List(),
                    Option(new WriterMessageMetaData("accessed2"))
                  )
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
                results.getDate("created") should be(java.sql.Date.valueOf("2016-04-27"))
                results.getTimestamp("updated") should be(
                  java.sql.Timestamp.valueOf("2016-04-27 12:12:12")
                )
                results.getTime("accessed") should be(java.sql.Time.valueOf("12:12:12"))
                results.getString("created2") should be("2016-04-27")
                results.getString("updated2") should be("2016-04-27 13:22:22.0")
                results.getString("accessed2") should be("13:23:12")
              }

              connection.close()
            }
          }
        }
      }
    }
  }

}
