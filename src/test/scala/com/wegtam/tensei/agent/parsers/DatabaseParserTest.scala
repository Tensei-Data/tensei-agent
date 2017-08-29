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

package com.wegtam.tensei.agent.parsers

import java.io.File
import java.net.URI
import java.nio.file.{ FileSystems, Files }

import akka.testkit.TestActorRef
import akka.util.ByteString
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages.{
  GetSequenceRowCount,
  ReturnData,
  SequenceRowCount
}
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.{ DataTreeDocument, XmlActorSpec }
import org.dfasdl.utils.ElementNames
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * @todo Move to integration tests!
  */
class DatabaseParserTest extends XmlActorSpec with BeforeAndAfterEach {
  val sourceDatabaseName = "dbparsersrc"

  val tableNames = List("ACCOUNTS", "ACCOUNTS2", "ACCOUNTS3", "ACCOUNTS4")

  val sqliteDbPath1 =
    File.createTempFile("tensei-agent", "testSqlite.db").getAbsolutePath.replace("\\", "/")

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
    * Drop all tables before each test.
    */
  override def beforeEach(): Unit = {
    val stm = sourceDatabaseConnection.createStatement()
    tableNames foreach (n => stm.execute(s"DROP TABLE IF EXISTS $n"))
    stm.close()
    val p1 = FileSystems.getDefault.getPath(sqliteDbPath1)
    Files.deleteIfExists(p1)
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    val p1 = FileSystems.getDefault.getPath(sqliteDbPath1)
    Files.deleteIfExists(p1)
    super.afterEach()
  }

  describe("DatabaseParser") {
    describe("Parsing a simple table") {
      describe("with complete data sets") {
        it("should parse the table correctly") {
          val tableName = tableNames.head
          val stm       = sourceDatabaseConnection.createStatement()
          val sql =
            s"""
               |CREATE TABLE $tableName (
               |  id BIGINT,
               |  name VARCHAR(254),
               |  description VARCHAR,
               |  birthday DATE,
               |  salary VARCHAR
               |);
               |INSERT INTO $tableName VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1.500.000,83 €');
               |INSERT INTO $tableName VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2.800.000,00 €');
          """.stripMargin
          stm.execute(sql)
          stm.close()

          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-01.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val recipes = List(
            Recipe("DUMMY",
                   Recipe.MapOneToOne,
                   List(
                     MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                           List(ElementReference(dfasdl.id, "id")))
                   ))
          )
          val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
          val source =
            new ConnectionInformation(uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
                                      dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id)))
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("DatabaseParserTest"), Set.empty[String])
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
          )
          dbParser ! BaseParserMessages.Start

          val response = expectMsgType[ParserStatusMessage]
          response.status should be(ParserStatus.COMPLETED)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))

          val count = expectMsgType[SequenceRowCount]
          count.rows.getOrElse(0L) should be(2)

          dataTree ! ReturnData("id", Option(0L))
          val row1column1 = expectMsgType[DataTreeNodeMessages.Content]
          row1column1.data.size should be(1)
          row1column1.data.head.data shouldBe a[java.lang.Long]
          row1column1.data.head.data should be(1L)
          dataTree ! ReturnData("id", Option(1L))
          val row2column1 = expectMsgType[DataTreeNodeMessages.Content]
          row2column1.data.size should be(1)
          row2column1.data.head.data should be(2)

          dataTree ! ReturnData("birthday", Option(0L))
          val birthday1 = expectMsgType[DataTreeNodeMessages.Content]
          birthday1.data.size should be(1)
          birthday1.data.head.data should be(java.time.LocalDate.parse("1963-01-01"))
          dataTree ! ReturnData("birthday", Option(1L))
          val birthday2 = expectMsgType[DataTreeNodeMessages.Content]
          birthday2.data.size should be(1)
          birthday2.data.head.data should be(java.time.LocalDate.parse("1968-01-01"))
        }
      }

      describe("with empty columns") {
        it("should parse the table correctly") {
          val tableName = tableNames.head
          val stm       = sourceDatabaseConnection.createStatement()
          val sql =
            s"""
               |CREATE TABLE $tableName (
               |  id BIGINT,
               |  name VARCHAR(254),
               |  description VARCHAR,
               |  birthday DATE,
               |  salary VARCHAR
               |);
               |INSERT INTO $tableName VALUES (1, 'Max Mustermann', NULL, '1963-01-01', '1.500.000,83 €');
               |INSERT INTO $tableName VALUES (2, 'Eva Musterfrau', NULL, '1968-01-01', '2.800.000,00 €');
          """.stripMargin
          stm.execute(sql)
          stm.close()

          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-02.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val recipes = List(
            Recipe("DUMMY",
                   Recipe.MapOneToOne,
                   List(
                     MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                           List(ElementReference(dfasdl.id, "id")))
                   ))
          )
          val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
          val source =
            new ConnectionInformation(uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
                                      dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id)))
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("DatabaseParserTest"), Set.empty[String])
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
          )
          dbParser ! BaseParserMessages.Start

          val response = expectMsgType[ParserStatusMessage]
          response.status should be(ParserStatus.COMPLETED)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))

          val count = expectMsgType[SequenceRowCount]
          count.rows.getOrElse(0L) should be(2)

          dataTree ! ReturnData("description", Option(0L))
          val emptyColumn = expectMsgType[DataTreeNodeMessages.Content]
          emptyColumn.data.size should be(1)
          emptyColumn.data.head.data should be(None)
        }
      }

      describe(s"with complete data sets and a DOUBLE as ${ElementNames.FORMATTED_NUMBER}") {
        it("should parse the table correctly") {
          val tableName = tableNames.head
          val stm       = sourceDatabaseConnection.createStatement()
          val sql =
            s"""
               |CREATE TABLE $tableName (
               |  id BIGINT,
               |  name VARCHAR(254),
               |  description VARCHAR,
               |  birthday DATE,
               |  salary DOUBLE
               |);
               |INSERT INTO $tableName VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83');
               |INSERT INTO $tableName VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
          """.stripMargin
          stm.execute(sql)
          stm.close()

          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-03.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val recipes = List(
            Recipe("DUMMY",
                   Recipe.MapOneToOne,
                   List(
                     MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                           List(ElementReference(dfasdl.id, "id")))
                   ))
          )
          val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
          val source =
            new ConnectionInformation(uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
                                      dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id)))
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("DatabaseParserTest"), Set.empty[String])
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
          )
          dbParser ! BaseParserMessages.Start

          val response = expectMsgType[ParserStatusMessage]
          response.status should be(ParserStatus.COMPLETED)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))

          val count = expectMsgType[SequenceRowCount]
          count.rows.getOrElse(0L) should be(2)

          dataTree ! ReturnData("salary", Option(1L))
          val salary = expectMsgType[DataTreeNodeMessages.Content]
          salary.data.size should be(1)
          salary.data.head.data.toString should be("2800000.0")
        }
      }

      describe(s"with complete data sets and some more data") {
        describe("with a WHERE condition of bigger than") {
          it("should parse the table correctly") {
            val tableName = tableNames.head
            val stm       = sourceDatabaseConnection.createStatement()
            val sql =
              s"""
                 |CREATE TABLE $tableName (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |INSERT INTO $tableName VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO $tableName VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
                 |INSERT INTO $tableName VALUES (3, 'Marki Mark', 'Gesellschafter', '1974-02-22', '1900000.00');
                 |INSERT INTO $tableName VALUES (4, 'Chris Christoffer', 'IT-Chef', '1981-06-22', '3200000.00');
                 |INSERT INTO $tableName VALUES (5, 'Jack Slater', 'Marketing-Chef', '1977-11-12', '9900000.00');
            """.stripMargin
            stm.execute(sql)
            stm.close()

            val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-03-where.xml"
            val xml =
              scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
            val dfasdl = new DFASDL("SIMPLE-01", xml)
            val recipes = List(
              Recipe("DUMMY",
                     Recipe.MapOneToOne,
                     List(
                       MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                             List(ElementReference(dfasdl.id, "id")))
                     ))
            )
            val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
            val source = new ConnectionInformation(
              uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
              dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id))
            )
            val dataTree = TestActorRef(
              DataTreeDocument.props(dfasdl, Option("DatabaseParserTest"), Set.empty[String])
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
            )
            dbParser ! BaseParserMessages.Start

            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))

            val count = expectMsgType[SequenceRowCount]

            count.rows.getOrElse(0L) should be(3)

            dataTree ! ReturnData("salary", Option(0L))
            val salary = expectMsgType[DataTreeNodeMessages.Content]
            salary.data.size should be(1)
            salary.data.head.data.toString should be("2800000.0")

            dataTree ! ReturnData("name", Option(0L))
            val name = expectMsgType[DataTreeNodeMessages.Content]
            name.data.size should be(1)
            name.data.head.data should be(ByteString("Eva Musterfrau"))

            dataTree ! ReturnData("salary", Option(1L))
            val salary2 = expectMsgType[DataTreeNodeMessages.Content]
            salary2.data.size should be(1)
            salary2.data.head.data.toString should be("3200000.0")

            dataTree ! ReturnData("name", Option(1L))
            val name2 = expectMsgType[DataTreeNodeMessages.Content]
            name2.data.size should be(1)
            name2.data.head.data should be(ByteString("Chris Christoffer"))
          }
        }
      }
    }

    describe("Parsing two tables") {
      describe(s"with complete data sets and a DOUBLE as ${ElementNames.FORMATTED_NUMBER}") {
        it("should parse the table correctly") {
          val stm = sourceDatabaseConnection.createStatement()
          val sql =
            s"""
               |CREATE TABLE ${tableNames.head} (
               |  id BIGINT,
               |  name VARCHAR(254),
               |  description VARCHAR,
               |  birthday DATE,
               |  salary DOUBLE
               |);
               |CREATE TABLE ${tableNames(1)} (
               |  id BIGINT,
               |  name VARCHAR(254),
               |  description VARCHAR,
               |  birthday DATE,
               |  salary DOUBLE
               |);
               |INSERT INTO ${tableNames.head} VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83');
               |INSERT INTO ${tableNames.head} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
               |INSERT INTO ${tableNames(1)} VALUES (1, 'Friedrich Ferdinand', 'Vorstand', '1963-01-01', '1500000.83');
               |INSERT INTO ${tableNames(1)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
           """.stripMargin
          stm.execute(sql)
          stm.close()

          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-04.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val recipes = List(
            Recipe(
              "DUMMY",
              Recipe.MapOneToOne,
              List(
                MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                      List(ElementReference(dfasdl.id, "id"))),
                MappingTransformation(List(ElementReference(dfasdl.id, "id2")),
                                      List(ElementReference(dfasdl.id, "id2")))
              )
            )
          )
          val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
          val source =
            new ConnectionInformation(uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
                                      dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id)))
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("DatabaseParserTest"), Set.empty[String])
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
          )
          dbParser ! BaseParserMessages.Start

          val response = expectMsgType[ParserStatusMessage]
          response.status should be(ParserStatus.COMPLETED)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))
          val count1 = expectMsgType[SequenceRowCount]
          count1.rows.getOrElse(0L) should be(2)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts2"))
          val count2 = expectMsgType[SequenceRowCount]
          count2.rows.getOrElse(0L) should be(2)

          dataTree ! ReturnData("name", Option(0L))
          val name1 = expectMsgType[DataTreeNodeMessages.Content]
          name1.data.size should be(1)
          name1.data.head.data should be(ByteString("Max Mustermann"))

          dataTree ! ReturnData("name2", Option(0L))
          val name2 = expectMsgType[DataTreeNodeMessages.Content]
          name2.data.size should be(1)
          name2.data.head.data should be(ByteString("Friedrich Ferdinand"))
        }
      }

      describe(s"with complete data sets, a DOUBLE as ${ElementNames.FORMATTED_NUMBER}") {
        describe("and a WHERE condition of bigger than") {
          it("should parse the table correctly") {
            val stm = sourceDatabaseConnection.createStatement()
            val sql =
              s"""
                 |CREATE TABLE ${tableNames.head} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(1)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |INSERT INTO ${tableNames.head} VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames.head} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
                 |INSERT INTO ${tableNames(1)} VALUES (1, 'Friedrich Ferdinand', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames(1)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
             """.stripMargin
            stm.execute(sql)
            stm.close()

            val dfasdlFile =
              "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-04-where.xml"
            val xml =
              scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
            val dfasdl = new DFASDL("SIMPLE-01", xml)
            val recipes = List(
              Recipe(
                "DUMMY",
                Recipe.MapOneToOne,
                List(
                  MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                        List(ElementReference(dfasdl.id, "id"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id2")),
                                        List(ElementReference(dfasdl.id, "id2")))
                )
              )
            )
            val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
            val source =
              new ConnectionInformation(uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
                                        dfasdlRef = Option(
                                          DFASDLReference(cookbook.id, dfasdl.id)
                                        ))
            val dataTree = TestActorRef(
              DataTreeDocument.props(dfasdl, Option("DatabaseParserTest"), Set.empty[String])
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
            )
            dbParser ! BaseParserMessages.Start

            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))
            val count1 = expectMsgType[SequenceRowCount]
            count1.rows.getOrElse(0L) should be(1)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts2"))
            val count2 = expectMsgType[SequenceRowCount]
            count2.rows.getOrElse(0L) should be(1)

            dataTree ! ReturnData("name", Option(0L))
            val name1 = expectMsgType[DataTreeNodeMessages.Content]
            name1.data.size should be(1)
            name1.data.head.data should be(ByteString("Eva Musterfrau"))

            dataTree ! ReturnData("name2", Option(0L))
            val name2 = expectMsgType[DataTreeNodeMessages.Content]
            name2.data.size should be(1)
            name2.data.head.data should be(ByteString("Eva Musterfrau"))
          }
        }

        describe("and a WHERE condition of smaller than") {
          it("should parse the table correctly") {
            val stm = sourceDatabaseConnection.createStatement()
            val sql =
              s"""
                 |CREATE TABLE ${tableNames.head} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(1)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |INSERT INTO ${tableNames.head} VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames.head} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
                 |INSERT INTO ${tableNames(1)} VALUES (1, 'Friedrich Ferdinand', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames(1)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
             """.stripMargin
            stm.execute(sql)
            stm.close()

            val dfasdlFile =
              "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-04-where-smaller.xml"
            val xml =
              scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
            val dfasdl = new DFASDL("SIMPLE-01", xml)
            val recipes = List(
              Recipe(
                "DUMMY",
                Recipe.MapOneToOne,
                List(
                  MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                        List(ElementReference(dfasdl.id, "id"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id2")),
                                        List(ElementReference(dfasdl.id, "id2")))
                )
              )
            )
            val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
            val source =
              new ConnectionInformation(uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
                                        dfasdlRef = Option(
                                          DFASDLReference(cookbook.id, dfasdl.id)
                                        ))
            val dataTree = TestActorRef(
              DataTreeDocument.props(dfasdl, Option("DatabaseParserTest"), Set.empty[String])
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
            )
            dbParser ! BaseParserMessages.Start

            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))
            val count1 = expectMsgType[SequenceRowCount]
            count1.rows.getOrElse(0L) should be(1)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts2"))
            val count2 = expectMsgType[SequenceRowCount]
            count2.rows.getOrElse(0L) should be(1)

            dataTree ! ReturnData("name", Option(0L))
            val name1 = expectMsgType[DataTreeNodeMessages.Content]
            name1.data.size should be(1)
            name1.data.head.data should be(ByteString("Max Mustermann"))

            dataTree ! ReturnData("name2", Option(0L))
            val name2 = expectMsgType[DataTreeNodeMessages.Content]
            name2.data.size should be(1)
            name2.data.head.data should be(ByteString("Friedrich Ferdinand"))
          }
        }
      }
    }

    describe("Parsing two tables with a JOIN") {
      describe(s"that returns data") {
        it("should parse the table correctly") {
          val stm = sourceDatabaseConnection.createStatement()
          val sql =
            s"""
               |CREATE TABLE ${tableNames.head} (
               |  id BIGINT,
               |  name VARCHAR(254),
               |  description VARCHAR,
               |  birthday DATE,
               |  salary DOUBLE,
               |  familyid INT
               |);
               |CREATE TABLE ${tableNames(1)} (
               |  id BIGINT,
               |  name VARCHAR(254),
               |  description VARCHAR,
               |  birthday DATE,
               |  salary DOUBLE,
               |  familyid INT
               |);
               |INSERT INTO ${tableNames.head} VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83', 1);
               |INSERT INTO ${tableNames.head} VALUES (2, 'Clara Ferdinand', 'Aufsichtsrat', '1968-01-01', '2800000.00', 2);
               |INSERT INTO ${tableNames(1)} VALUES (1, 'Friedrich Ferdinand', 'Vorstand', '1963-01-01', '1500000.83', 2);
               |INSERT INTO ${tableNames(1)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00', 1);
           """.stripMargin
          stm.execute(sql)
          stm.close()

          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-05.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val recipes = List(
            Recipe(
              "DUMMY",
              Recipe.MapOneToOne,
              List(
                MappingTransformation(List(ElementReference(dfasdl.id, "name")),
                                      List(ElementReference(dfasdl.id, "name"))),
                MappingTransformation(List(ElementReference(dfasdl.id, "name2")),
                                      List(ElementReference(dfasdl.id, "name2")))
              )
            )
          )
          val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
          val source =
            new ConnectionInformation(uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
                                      dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id)))
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("DatabaseParserTest"), Set.empty[String])
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
          )
          dbParser ! BaseParserMessages.Start

          val response = expectMsgType[ParserStatusMessage]
          response.status should be(ParserStatus.COMPLETED)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))
          val count1 = expectMsgType[SequenceRowCount]
          count1.rows.getOrElse(0L) should be(2)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts2"))
          val count2 = expectMsgType[SequenceRowCount]
          count2.rows.getOrElse(0L) should be(2)

          dataTree ! ReturnData("name", Option(0L))
          val name1 = expectMsgType[DataTreeNodeMessages.Content]
          name1.data.size should be(1)
          name1.data.head.data should be(ByteString("Max Mustermann"))

          dataTree ! ReturnData("name2", Option(0L))
          val name2 = expectMsgType[DataTreeNodeMessages.Content]
          name2.data.size should be(1)
          name2.data.head.data should be(ByteString("Eva Musterfrau"))

          dataTree ! ReturnData("name2", Option(1L))
          val name21 = expectMsgType[DataTreeNodeMessages.Content]
          name21.data.size should be(1)
          name21.data.head.data should be(ByteString("Friedrich Ferdinand"))
        }
      }

      describe(s"that returns no data") {
        it("should parse the table correctly") {
          val stm = sourceDatabaseConnection.createStatement()
          val sql =
            s"""
               |CREATE TABLE ${tableNames.head} (
               |  id BIGINT,
               |  name VARCHAR(254),
               |  description VARCHAR,
               |  birthday DATE,
               |  salary DOUBLE,
               |  familyid INT
               |);
               |CREATE TABLE ${tableNames(1)} (
               |  id BIGINT,
               |  name VARCHAR(254),
               |  description VARCHAR,
               |  birthday DATE,
               |  salary DOUBLE,
               |  familyid INT
               |);
               |INSERT INTO ${tableNames.head} VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83', 1);
               |INSERT INTO ${tableNames.head} VALUES (2, 'Clara Ferdinand', 'Aufsichtsrat', '1968-01-01', '2800000.00', 2);
               |INSERT INTO ${tableNames(1)} VALUES (1, 'Friedrich Ferdinand', 'Vorstand', '1963-01-01', '1500000.83', 2);
               |INSERT INTO ${tableNames(1)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00', 1);
           """.stripMargin
          stm.execute(sql)
          stm.close()

          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-06.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val recipes = List(
            Recipe(
              "DUMMY",
              Recipe.MapOneToOne,
              List(
                MappingTransformation(List(ElementReference(dfasdl.id, "name")),
                                      List(ElementReference(dfasdl.id, "name"))),
                MappingTransformation(List(ElementReference(dfasdl.id, "name2")),
                                      List(ElementReference(dfasdl.id, "name2")))
              )
            )
          )
          val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
          val source =
            new ConnectionInformation(uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
                                      dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id)))
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("DatabaseParserTest"), Set.empty[String])
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
          )
          dbParser ! BaseParserMessages.Start

          val response = expectMsgType[ParserStatusMessage]
          response.status should be(ParserStatus.COMPLETED)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))
          val count1 = expectMsgType[SequenceRowCount]
          count1.rows.getOrElse(0L) should be(2)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts2"))
          val count2 = expectMsgType[SequenceRowCount]
          count2.rows.getOrElse(0L) should be(0)

          dataTree ! ReturnData("name", Option(0L))
          val name1 = expectMsgType[DataTreeNodeMessages.Content]
          name1.data.size should be(1)
          name1.data.head.data should be(ByteString("Max Mustermann"))
        }
      }
    }

    describe("Parsing three tables") {
      describe(s"with complete data sets and a DOUBLE as ${ElementNames.FORMATTED_NUMBER}") {
        describe("and the firdt two are empty") {
          it("should parse the tables correctly") {
            val stm = sourceDatabaseConnection.createStatement()
            val sql =
              s"""
                 |CREATE TABLE ${tableNames.head} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(1)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(2)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |INSERT INTO ${tableNames(2)} VALUES (1, 'Friedrich Ferdinand', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames(2)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
             """.stripMargin
            stm.execute(sql)
            stm.close()
            val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-07.xml"
            val xml =
              scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
            val dfasdl = new DFASDL("SIMPLE-01", xml)
            val recipes = List(
              Recipe(
                "DUMMY",
                Recipe.MapOneToOne,
                List(
                  MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                        List(ElementReference(dfasdl.id, "id"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id2")),
                                        List(ElementReference(dfasdl.id, "id2"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id3")),
                                        List(ElementReference(dfasdl.id, "id3")))
                )
              )
            )
            val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
            val source = new ConnectionInformation(
              uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
              dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id))
            )
            val dataTree = TestActorRef(
              DataTreeDocument.props(
                dfasdl,
                Option("DatabaseParserTest"),
                Set.empty[String]
              )
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
            )
            dbParser ! BaseParserMessages.Start

            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))
            val count1 = expectMsgType[SequenceRowCount]
            count1.rows.getOrElse(0L) should be(0)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts2"))
            val count2 = expectMsgType[SequenceRowCount]
            count2.rows.getOrElse(0L) should be(0)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts3"))
            val count3 = expectMsgType[SequenceRowCount]
            count3.rows.getOrElse(0L) should be(2)

            dataTree ! ReturnData("name3", Option(0L))
            val name2 = expectMsgType[DataTreeNodeMessages.Content]
            name2.data.size should be(1)
            name2.data.head.data should be(ByteString("Friedrich Ferdinand"))
          }
        }
      }

      describe(s"with complete data sets and a DOUBLE as ${ElementNames.FORMATTED_NUMBER}") {
        describe("and first table is empty") {
          it("should parse the tables correctly") {
            val stm = sourceDatabaseConnection.createStatement()
            val sql =
              s"""
                 |CREATE TABLE ${tableNames.head} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(1)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(2)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |INSERT INTO ${tableNames(1)} VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames(1)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
                 |INSERT INTO ${tableNames(2)} VALUES (1, 'Friedrich Ferdinand', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames(2)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
             """.stripMargin
            stm.execute(sql)
            stm.close()
            val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-07.xml"
            val xml =
              scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
            val dfasdl = new DFASDL("SIMPLE-01", xml)
            val recipes = List(
              Recipe(
                "DUMMY",
                Recipe.MapOneToOne,
                List(
                  MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                        List(ElementReference(dfasdl.id, "id"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id2")),
                                        List(ElementReference(dfasdl.id, "id2"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id3")),
                                        List(ElementReference(dfasdl.id, "id3")))
                )
              )
            )
            val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
            val source = new ConnectionInformation(
              uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
              dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id))
            )
            val dataTree = TestActorRef(
              DataTreeDocument.props(
                dfasdl,
                Option("DatabaseParserTest"),
                Set.empty[String]
              )
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
            )
            dbParser ! BaseParserMessages.Start

            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))
            val count1 = expectMsgType[SequenceRowCount]
            count1.rows.getOrElse(0L) should be(0)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts2"))
            val count2 = expectMsgType[SequenceRowCount]
            count2.rows.getOrElse(0L) should be(2)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts3"))
            val count3 = expectMsgType[SequenceRowCount]
            count3.rows.getOrElse(0L) should be(2)

            dataTree ! ReturnData("name2", Option(0L))
            val name1 = expectMsgType[DataTreeNodeMessages.Content]
            name1.data.size should be(1)
            name1.data.head.data should be(ByteString("Max Mustermann"))

            dataTree ! ReturnData("name3", Option(0L))
            val name2 = expectMsgType[DataTreeNodeMessages.Content]
            name2.data.size should be(1)
            name2.data.head.data should be(ByteString("Friedrich Ferdinand"))
          }
        }
      }

      describe(s"with complete data sets and a DOUBLE as ${ElementNames.FORMATTED_NUMBER}") {
        describe("and the second table is empty") {
          it("should parse the tables correctly") {
            val stm = sourceDatabaseConnection.createStatement()
            val sql =
              s"""
                 |CREATE TABLE ${tableNames.head} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(1)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(2)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |INSERT INTO ${tableNames.head} VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames.head} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
                 |INSERT INTO ${tableNames(2)} VALUES (1, 'Friedrich Ferdinand', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames(2)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
             """.stripMargin
            stm.execute(sql)
            stm.close()
            val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-07.xml"
            val xml =
              scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
            val dfasdl = new DFASDL("SIMPLE-01", xml)
            val recipes = List(
              Recipe(
                "DUMMY",
                Recipe.MapOneToOne,
                List(
                  MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                        List(ElementReference(dfasdl.id, "id"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id2")),
                                        List(ElementReference(dfasdl.id, "id2"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id3")),
                                        List(ElementReference(dfasdl.id, "id3")))
                )
              )
            )
            val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
            val source = new ConnectionInformation(
              uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
              dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id))
            )
            val dataTree = TestActorRef(
              DataTreeDocument.props(
                dfasdl,
                Option("DatabaseParserTest"),
                Set.empty[String]
              )
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
            )
            dbParser ! BaseParserMessages.Start

            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))
            val count1 = expectMsgType[SequenceRowCount]
            count1.rows.getOrElse(0L) should be(2)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts2"))
            val count2 = expectMsgType[SequenceRowCount]
            count2.rows.getOrElse(0L) should be(0)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts3"))
            val count3 = expectMsgType[SequenceRowCount]
            count3.rows.getOrElse(0L) should be(2)

            dataTree ! ReturnData("name", Option(0L))
            val name1 = expectMsgType[DataTreeNodeMessages.Content]
            name1.data.size should be(1)
            name1.data.head.data should be(ByteString("Max Mustermann"))

            dataTree ! ReturnData("name3", Option(0L))
            val name2 = expectMsgType[DataTreeNodeMessages.Content]
            name2.data.size should be(1)
            name2.data.head.data should be(ByteString("Friedrich Ferdinand"))
          }
        }
      }
    }

    describe("Parsing four tables") {
      describe(s"with complete data sets and a DOUBLE as ${ElementNames.FORMATTED_NUMBER}") {
        describe("and the first two are empty") {
          it("should parse the tables correctly") {
            val stm = sourceDatabaseConnection.createStatement()
            val sql =
              s"""
                 |CREATE TABLE ${tableNames.head} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(1)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(2)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(3)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |INSERT INTO ${tableNames(2)} VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames(2)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
                 |INSERT INTO ${tableNames(3)} VALUES (1, 'Friedrich Ferdinand', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames(3)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
             """.stripMargin
            stm.execute(sql)
            stm.close()
            val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-08.xml"
            val xml =
              scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
            val dfasdl = new DFASDL("SIMPLE-01", xml)
            val recipes = List(
              Recipe(
                "DUMMY",
                Recipe.MapOneToOne,
                List(
                  MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                        List(ElementReference(dfasdl.id, "id"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id2")),
                                        List(ElementReference(dfasdl.id, "id2"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id3")),
                                        List(ElementReference(dfasdl.id, "id3"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id4")),
                                        List(ElementReference(dfasdl.id, "id4")))
                )
              )
            )
            val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
            val source = new ConnectionInformation(
              uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
              dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id))
            )
            val dataTree = TestActorRef(
              DataTreeDocument.props(
                dfasdl,
                Option("DatabaseParserTest"),
                Set.empty[String]
              )
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
            )
            dbParser ! BaseParserMessages.Start

            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))
            val count1 = expectMsgType[SequenceRowCount]
            count1.rows.getOrElse(0L) should be(0)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts2"))
            val count2 = expectMsgType[SequenceRowCount]
            count2.rows.getOrElse(0L) should be(0)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts3"))
            val count3 = expectMsgType[SequenceRowCount]
            count3.rows.getOrElse(0L) should be(2)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts4"))
            val count4 = expectMsgType[SequenceRowCount]
            count4.rows.getOrElse(0L) should be(2)

            dataTree ! ReturnData("name3", Option(0L))
            val name3 = expectMsgType[DataTreeNodeMessages.Content]
            name3.data.size should be(1)
            name3.data.head.data should be(ByteString("Max Mustermann"))

            dataTree ! ReturnData("name4", Option(0L))
            val name4 = expectMsgType[DataTreeNodeMessages.Content]
            name4.data.size should be(1)
            name4.data.head.data should be(ByteString("Friedrich Ferdinand"))
          }
        }
      }

      describe(s"with complete data sets and a DOUBLE as ${ElementNames.FORMATTED_NUMBER}") {
        describe("and the first and the third are empty") {
          it("should parse the tables correctly") {
            val stm = sourceDatabaseConnection.createStatement()
            val sql =
              s"""
                 |CREATE TABLE ${tableNames.head} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(1)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(2)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(3)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |INSERT INTO ${tableNames(1)} VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames(1)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
                 |INSERT INTO ${tableNames(3)} VALUES (1, 'Friedrich Ferdinand', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames(3)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
             """.stripMargin
            stm.execute(sql)
            stm.close()
            val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-08.xml"
            val xml =
              scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
            val dfasdl = new DFASDL("SIMPLE-01", xml)
            val recipes = List(
              Recipe(
                "DUMMY",
                Recipe.MapOneToOne,
                List(
                  MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                        List(ElementReference(dfasdl.id, "id"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id2")),
                                        List(ElementReference(dfasdl.id, "id2"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id3")),
                                        List(ElementReference(dfasdl.id, "id3"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id4")),
                                        List(ElementReference(dfasdl.id, "id4")))
                )
              )
            )
            val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
            val source = new ConnectionInformation(
              uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
              dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id))
            )
            val dataTree = TestActorRef(
              DataTreeDocument.props(
                dfasdl,
                Option("DatabaseParserTest"),
                Set.empty[String]
              )
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
            )
            dbParser ! BaseParserMessages.Start

            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))
            val count1 = expectMsgType[SequenceRowCount]
            count1.rows.getOrElse(0L) should be(0)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts2"))
            val count2 = expectMsgType[SequenceRowCount]
            count2.rows.getOrElse(0L) should be(2)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts3"))
            val count3 = expectMsgType[SequenceRowCount]
            count3.rows.getOrElse(0L) should be(0)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts4"))
            val count4 = expectMsgType[SequenceRowCount]
            count4.rows.getOrElse(0L) should be(2)

            dataTree ! ReturnData("name2", Option(0L))
            val name2 = expectMsgType[DataTreeNodeMessages.Content]
            name2.data.size should be(1)
            name2.data.head.data should be(ByteString("Max Mustermann"))

            dataTree ! ReturnData("name4", Option(0L))
            val name4 = expectMsgType[DataTreeNodeMessages.Content]
            name4.data.size should be(1)
            name4.data.head.data should be(ByteString("Friedrich Ferdinand"))
          }
        }
      }

      describe(s"with complete data sets and a DOUBLE as ${ElementNames.FORMATTED_NUMBER}") {
        describe("and the second and the third are empty") {
          it("should parse the tables correctly") {
            val stm = sourceDatabaseConnection.createStatement()
            val sql =
              s"""
                 |CREATE TABLE ${tableNames.head} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(1)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(2)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(3)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |INSERT INTO ${tableNames.head} VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames.head} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
                 |INSERT INTO ${tableNames(3)} VALUES (1, 'Friedrich Ferdinand', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames(3)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
             """.stripMargin
            stm.execute(sql)
            stm.close()
            val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-08.xml"
            val xml =
              scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
            val dfasdl = new DFASDL("SIMPLE-01", xml)
            val recipes = List(
              Recipe(
                "DUMMY",
                Recipe.MapOneToOne,
                List(
                  MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                        List(ElementReference(dfasdl.id, "id"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id2")),
                                        List(ElementReference(dfasdl.id, "id2"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id3")),
                                        List(ElementReference(dfasdl.id, "id3"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id4")),
                                        List(ElementReference(dfasdl.id, "id4")))
                )
              )
            )
            val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
            val source = new ConnectionInformation(
              uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
              dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id))
            )
            val dataTree = TestActorRef(
              DataTreeDocument.props(
                dfasdl,
                Option("DatabaseParserTest"),
                Set.empty[String]
              )
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
            )
            dbParser ! BaseParserMessages.Start

            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))
            val count1 = expectMsgType[SequenceRowCount]
            count1.rows.getOrElse(0L) should be(2)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts2"))
            val count2 = expectMsgType[SequenceRowCount]
            count2.rows.getOrElse(0L) should be(0)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts3"))
            val count3 = expectMsgType[SequenceRowCount]
            count3.rows.getOrElse(0L) should be(0)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts4"))
            val count4 = expectMsgType[SequenceRowCount]
            count4.rows.getOrElse(0L) should be(2)

            dataTree ! ReturnData("name", Option(0L))
            val name = expectMsgType[DataTreeNodeMessages.Content]
            name.data.size should be(1)
            name.data.head.data should be(ByteString("Max Mustermann"))

            dataTree ! ReturnData("name4", Option(0L))
            val name4 = expectMsgType[DataTreeNodeMessages.Content]
            name4.data.size should be(1)
            name4.data.head.data should be(ByteString("Friedrich Ferdinand"))
          }
        }
      }

      describe(s"with complete data sets and a DOUBLE as ${ElementNames.FORMATTED_NUMBER}") {
        describe("and the second and the fourth are empty") {
          it("should parse the tables correctly") {
            val stm = sourceDatabaseConnection.createStatement()
            val sql =
              s"""
                 |CREATE TABLE ${tableNames.head} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(1)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(2)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |CREATE TABLE ${tableNames(3)} (
                 |  id BIGINT,
                 |  name VARCHAR(254),
                 |  description VARCHAR,
                 |  birthday DATE,
                 |  salary DOUBLE
                 |);
                 |INSERT INTO ${tableNames.head} VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames.head} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
                 |INSERT INTO ${tableNames(2)} VALUES (1, 'Friedrich Ferdinand', 'Vorstand', '1963-01-01', '1500000.83');
                 |INSERT INTO ${tableNames(2)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
             """.stripMargin
            stm.execute(sql)
            stm.close()
            val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-08.xml"
            val xml =
              scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
            val dfasdl = new DFASDL("SIMPLE-01", xml)
            val recipes = List(
              Recipe(
                "DUMMY",
                Recipe.MapOneToOne,
                List(
                  MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                        List(ElementReference(dfasdl.id, "id"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id2")),
                                        List(ElementReference(dfasdl.id, "id2"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id3")),
                                        List(ElementReference(dfasdl.id, "id3"))),
                  MappingTransformation(List(ElementReference(dfasdl.id, "id4")),
                                        List(ElementReference(dfasdl.id, "id4")))
                )
              )
            )
            val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
            val source = new ConnectionInformation(
              uri = new URI(sourceDatabaseConnection.getMetaData.getURL),
              dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id))
            )
            val dataTree = TestActorRef(
              DataTreeDocument.props(
                dfasdl,
                Option("DatabaseParserTest"),
                Set.empty[String]
              )
            )
            val dbParser = TestActorRef(
              DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
            )
            dbParser ! BaseParserMessages.Start

            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))
            val count1 = expectMsgType[SequenceRowCount]
            count1.rows.getOrElse(0L) should be(2)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts2"))
            val count2 = expectMsgType[SequenceRowCount]
            count2.rows.getOrElse(0L) should be(0)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts3"))
            val count3 = expectMsgType[SequenceRowCount]
            count3.rows.getOrElse(0L) should be(2)

            dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts4"))
            val count4 = expectMsgType[SequenceRowCount]
            count4.rows.getOrElse(0L) should be(0)

            dataTree ! ReturnData("name", Option(0L))
            val name = expectMsgType[DataTreeNodeMessages.Content]
            name.data.size should be(1)
            name.data.head.data should be(ByteString("Max Mustermann"))

            dataTree ! ReturnData("name3", Option(0L))
            val name3 = expectMsgType[DataTreeNodeMessages.Content]
            name3.data.size should be(1)
            name3.data.head.data should be(ByteString("Friedrich Ferdinand"))
          }
        }
      }
    }

    describe("Testing a SQLite database") {
      describe("Create a simple database and parse the content") {
        it("should work") {
          val sourceDatabaseConnectionSqlite =
            java.sql.DriverManager.getConnection(s"jdbc:sqlite:$sqliteDbPath1")
          val stm = sourceDatabaseConnectionSqlite.createStatement()

          val sql0 =
            """
              |CREATE TABLE persons(name varchar(254), place varchar(200));
            """.stripMargin
          val sql1 =
            """
              |INSERT INTO persons values('Max Mustermann', 'Berlin');
            """.stripMargin
          val sql2 =
            """
              |INSERT INTO persons values('Frau Mustermann', 'Hamburg');
            """.stripMargin

          stm.execute(sql0)
          stm.execute(sql1)
          stm.execute(sql2)
          stm.close()

          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-09.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val recipes = List(
            Recipe(
              "DUMMY",
              Recipe.MapOneToOne,
              List(
                MappingTransformation(
                  List(ElementReference(dfasdl.id, "name"), ElementReference(dfasdl.id, "place")),
                  List(ElementReference(dfasdl.id, "name"), ElementReference(dfasdl.id, "place"))
                )
              )
            )
          )
          val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
          val source = new ConnectionInformation(
            uri = new URI(sourceDatabaseConnectionSqlite.getMetaData.getURL),
            dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id))
          )
          val dataTree = TestActorRef(
            DataTreeDocument.props(
              dfasdl,
              Option("DatabaseParserTest"),
              Set.empty[String]
            )
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
          )
          dbParser ! BaseParserMessages.Start

          val response = expectMsgType[ParserStatusMessage]
          response.status should be(ParserStatus.COMPLETED)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "persons"))

          val count = expectMsgType[SequenceRowCount]
          count.rows.getOrElse(0L) should be(2)

          dataTree ! ReturnData("name", Option(0L))
          val row1column1 = expectMsgType[DataTreeNodeMessages.Content]
          row1column1.data.size should be(1)
          row1column1.data.head.data should be(ByteString("Max Mustermann"))
          dataTree ! ReturnData("name", Option(1L))
          val row2column1 = expectMsgType[DataTreeNodeMessages.Content]
          row2column1.data.size should be(1)
          row2column1.data.head.data should be(ByteString("Frau Mustermann"))

          dataTree ! ReturnData("place", Option(0L))
          val birthday1 = expectMsgType[DataTreeNodeMessages.Content]
          birthday1.data.size should be(1)
          birthday1.data.head.data should be(ByteString("Berlin"))
          dataTree ! ReturnData("place", Option(1L))
          val birthday2 = expectMsgType[DataTreeNodeMessages.Content]
          birthday2.data.size should be(1)
          birthday2.data.head.data should be(ByteString("Hamburg"))

          sourceDatabaseConnectionSqlite.close()
        }
      }

      describe("Create a more complex database and parse the content") {
        it("should work") {
          val sourceDatabaseConnectionSqlite =
            java.sql.DriverManager.getConnection(s"jdbc:sqlite:$sqliteDbPath1")
          val stm = sourceDatabaseConnectionSqlite.createStatement()

          val sql0 =
            s"""
               |CREATE TABLE ${tableNames.head} (
               |  id BIGINT,
               |  name VARCHAR(254),
               |  description VARCHAR,
               |  birthday DATE,
               |  salary DOUBLE
               |);
         """.stripMargin

          val sql1 =
            s"""
               |CREATE TABLE ${tableNames(1)} (
               |  id BIGINT,
               |  name VARCHAR(254),
               |  description VARCHAR,
               |  birthday DATE,
               |  salary DOUBLE
               |);
         """.stripMargin

          val sql2 =
            s"""
               |CREATE TABLE ${tableNames(2)} (
               |  id BIGINT,
               |  name VARCHAR(254),
               |  description VARCHAR,
               |  birthday DATE,
               |  salary DOUBLE
               |);
         """.stripMargin

          val sql3 =
            s"""
               |CREATE TABLE ${tableNames(3)} (
               |  id BIGINT,
               |  name VARCHAR(254),
               |  description VARCHAR,
               |  birthday DATE,
               |  salary DOUBLE
               |);
         """.stripMargin

          val sql4 =
            s"""
               |INSERT INTO ${tableNames.head} VALUES (1, 'Max Mustermann', 'Vorstand', '1963-01-01', '1500000.83');
         """.stripMargin

          val sql5 =
            s"""
               |INSERT INTO ${tableNames.head} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
         """.stripMargin

          val sql6 =
            s"""
               |INSERT INTO ${tableNames(2)} VALUES (1, 'Friedrich Ferdinand', 'Vorstand', '1963-01-01', '1500000.83');
         """.stripMargin

          val sql7 =
            s"""
               |INSERT INTO ${tableNames(2)} VALUES (2, 'Eva Musterfrau', 'Aufsichtsrat', '1968-01-01', '2800000.00');
         """.stripMargin

          stm.execute(sql0)
          stm.execute(sql1)
          stm.execute(sql2)
          stm.execute(sql3)
          stm.execute(sql4)
          stm.execute(sql5)
          stm.execute(sql6)
          stm.execute(sql7)
          stm.close()

          val dfasdlFile = "/com/wegtam/tensei/agent/parsers/DatabaseParsers/simple-08.xml"
          val xml =
            scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
          val dfasdl = new DFASDL("SIMPLE-01", xml)
          val recipes = List(
            Recipe(
              "DUMMY",
              Recipe.MapOneToOne,
              List(
                MappingTransformation(List(ElementReference(dfasdl.id, "id")),
                                      List(ElementReference(dfasdl.id, "id"))),
                MappingTransformation(List(ElementReference(dfasdl.id, "id2")),
                                      List(ElementReference(dfasdl.id, "id2"))),
                MappingTransformation(List(ElementReference(dfasdl.id, "id3")),
                                      List(ElementReference(dfasdl.id, "id3"))),
                MappingTransformation(List(ElementReference(dfasdl.id, "id4")),
                                      List(ElementReference(dfasdl.id, "id4")))
              )
            )
          )
          val cookbook = new Cookbook("TEST", List(dfasdl), Option(dfasdl), recipes)
          val source = new ConnectionInformation(
            uri = new URI(sourceDatabaseConnectionSqlite.getMetaData.getURL),
            dfasdlRef = Option(DFASDLReference(cookbook.id, dfasdl.id))
          )
          val dataTree = TestActorRef(
            DataTreeDocument.props(
              dfasdl,
              Option("DatabaseParserTest"),
              Set.empty[String]
            )
          )
          val dbParser = TestActorRef(
            DatabaseParser.props(source, cookbook, dataTree, Option("DatabaseParserTest"))
          )
          dbParser ! BaseParserMessages.Start

          val response = expectMsgType[ParserStatusMessage]
          response.status should be(ParserStatus.COMPLETED)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts"))
          val count1 = expectMsgType[SequenceRowCount]
          count1.rows.getOrElse(0L) should be(2)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts2"))
          val count2 = expectMsgType[SequenceRowCount]
          count2.rows.getOrElse(0L) should be(0)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts3"))
          val count3 = expectMsgType[SequenceRowCount]
          count3.rows.getOrElse(0L) should be(2)

          dataTree ! GetSequenceRowCount(ElementReference(dfasdl.id, "accounts4"))
          val count4 = expectMsgType[SequenceRowCount]
          count4.rows.getOrElse(0L) should be(0)

          dataTree ! ReturnData("name", Option(0L))
          val name = expectMsgType[DataTreeNodeMessages.Content]
          name.data.size should be(1)
          name.data.head.data should be(ByteString("Max Mustermann"))

          dataTree ! ReturnData("name3", Option(0L))
          val name3 = expectMsgType[DataTreeNodeMessages.Content]
          name3.data.size should be(1)
          name3.data.head.data should be(ByteString("Friedrich Ferdinand"))

          dataTree ! ReturnData("birthday", Option(0L))
          val birthday1 = expectMsgType[DataTreeNodeMessages.Content]
          birthday1.data.size should be(1)
          birthday1.data.head.data should be(java.time.LocalDate.parse("1963-01-01"))

          sourceDatabaseConnectionSqlite.close()
        }
      }

    }
  }

}
