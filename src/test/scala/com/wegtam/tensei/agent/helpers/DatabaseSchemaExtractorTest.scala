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

package com.wegtam.tensei.agent.helpers

import java.io.{ File, InputStream }
import java.net.URI
import javax.xml.parsers.DocumentBuilderFactory

import akka.event.LoggingAdapter
import com.wegtam.tensei.agent.SchemaExtractor.ExtractorMetaData
import com.wegtam.tensei.agent.{ DatabaseSpec, XmlTestHelpers }

import scalaz._

/**
  * @todo Move to integration tests!
  */
class DatabaseSchemaExtractorTest
    extends DatabaseSpec
    with DatabaseSchemaExtractor
    with XmlHelpers
    with XmlTestHelpers {
  describe("DatabaseSchemaExtractor") {
    describe("getTables") {
      describe("using H2") {
        it("should work") {
          val connection = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:test")
          val statement  = connection.createStatement()

          statement.execute("CREATE TABLE accounts (id DOUBLE)")
          statement.execute("CREATE TABLE accounts2 (id DOUBLE)")
          statement.execute("CREATE TABLE customer (id DOUBLE)")
          statement.execute("CREATE TABLE person (id DOUBLE)")

          val uri = new URI(connection.getMetaData.getURL)
          val databaseDriver: SupportedDatabase = extractSupportedDatabaseFromUri(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }
          val databaseName: String = extractDatabaseNameFromURI(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }

          val s       = prepareGetAllTablesStatement(connection, databaseDriver)
          val results = getTables(s, databaseName)

          statement.execute("SHUTDOWN")
          connection.close()

          val expected = Vector(Table("accounts", List.empty),
                                Table("accounts2", List.empty),
                                Table("customer", List.empty),
                                Table("person", List.empty))

          results should be(expected)
        }
      }

      describe("using sqlite") {
        it("should work") {
          val tempFile   = File.createTempFile("tensei-agent", "testSqlite.db")
          val sqliteFile = tempFile.getAbsolutePath.replace("\\", "/")

          val sourceDatabaseConnectionSqlite =
            java.sql.DriverManager.getConnection(s"jdbc:sqlite:$sqliteFile")
          val stm = sourceDatabaseConnectionSqlite.createStatement()

          stm.execute("CREATE TABLE accounts (id DOUBLE)")
          stm.execute("CREATE TABLE accounts2 (id DOUBLE)")
          stm.execute("CREATE TABLE customer (id DOUBLE)")
          stm.execute("CREATE TABLE person (id DOUBLE)")

          val uri = new URI(sourceDatabaseConnectionSqlite.getMetaData.getURL)
          val databaseDriver: SupportedDatabase = extractSupportedDatabaseFromUri(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }
          val databaseName: String = extractDatabaseNameFromURI(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }

          val s       = prepareGetAllTablesStatement(sourceDatabaseConnectionSqlite, databaseDriver)
          val results = getTables(s, databaseName)

          stm.close()
          sourceDatabaseConnectionSqlite.close()
          tempFile.delete()

          val expected = Vector(Table("accounts", List.empty),
                                Table("accounts2", List.empty),
                                Table("customer", List.empty),
                                Table("person", List.empty))

          results should be(expected)
        }
      }
    }

    describe("createTables") {
      describe("using H2") {
        it("should work") {
          val connection = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:test")
          val statement  = connection.createStatement()

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/databaseSchemaExtractor/create-tables-h2.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          statement.execute(
            "CREATE TABLE accounts (id BIGINT(17), name VARCHAR(254), description CLOB(2147483647), birthday DATE(8), salary DOUBLE(17), points DECIMAL(4,2))"
          )

          val uri = new URI(connection.getMetaData.getURL)
          val databaseDriver: SupportedDatabase = extractSupportedDatabaseFromUri(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }
          val databaseName: String = extractDatabaseNameFromURI(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }

          val s      = prepareGetAllTablesStatement(connection, databaseDriver)
          val tables = getTables(s, databaseName)

          val expected = Vector(Table("accounts", List.empty))

          tables should be(expected)

          val factory  = DocumentBuilderFactory.newInstance()
          val loader   = factory.newDocumentBuilder()
          val document = loader.newDocument()
          val tableElements =
            describeTables(connection, databaseDriver, databaseName, tables, document)
          tableElements.foreach(element => document.appendChild(element))

          statement.execute("SHUTDOWN")
          connection.close()

          prettifyXml(document).replaceAll("\\s+", "") should be(
            xmlExpected.replaceAll("\\s+", "")
          )
        }
      }

      describe("using Sqlite") {
        it("should work") {
          val tempFile   = File.createTempFile("tensei-agent", "testSqlite.db")
          val sqliteFile = tempFile.getAbsolutePath.replace("\\", "/")

          val sourceDatabaseConnectionSqlite =
            java.sql.DriverManager.getConnection(s"jdbc:sqlite:$sqliteFile")
          val stm = sourceDatabaseConnectionSqlite.createStatement()

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/databaseSchemaExtractor/create-tables-sqlite.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          stm.execute(
            "CREATE TABLE accounts (id BIGINT(17), name VARCHAR(254) default foo, description CLOB(2147483647), birthday DATE(8), salary DOUBLE(17), points DECIMAL(4,2))"
          )

          val uri = new URI(sourceDatabaseConnectionSqlite.getMetaData.getURL)
          val databaseDriver: SupportedDatabase = extractSupportedDatabaseFromUri(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }
          val databaseName: String = extractDatabaseNameFromURI(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }

          val s      = prepareGetAllTablesStatement(sourceDatabaseConnectionSqlite, databaseDriver)
          val tables = getTables(s, databaseName)

          val expected = Vector(Table("accounts", List.empty))

          tables should be(expected)

          val factory  = DocumentBuilderFactory.newInstance()
          val loader   = factory.newDocumentBuilder()
          val document = loader.newDocument()
          val tableElements = describeTables(sourceDatabaseConnectionSqlite,
                                             databaseDriver,
                                             databaseName,
                                             tables,
                                             document)

          sourceDatabaseConnectionSqlite.close()
          tempFile.delete()

          tableElements.foreach(element => document.appendChild(element))
          prettifyXml(document).replaceAll("\\s+", "") should be(
            xmlExpected.replaceAll("\\s+", "")
          )
        }
      }
    }

    describe("extract") {
      describe("using h2") {
        it("should work") {
          val connection = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:test")
          val statement  = connection.createStatement()

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/databaseSchemaExtractor/extract-h2.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          statement.execute(
            "CREATE TABLE accounts (id DOUBLE(17), name VARCHAR(254), description CLOB(2147483647), birthday DATE(8), salary DOUBLE(17), points DECIMAL(4,2))"
          )
          statement.execute(
            "CREATE TABLE person (id DOUBLE(17), name VARCHAR(254), vorname VARCHAR(100), birthday DATE(8))"
          )

          val dfasdl =
            extractFromDatabase(connection, ExtractorMetaData(dfasdlNamePart = "CHANGEME"))
          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))

          statement.execute("SHUTDOWN")
          connection.close()
        }
      }

      describe("using Sqlite") {
        it("should work") {
          val tempFile   = File.createTempFile("tensei-agent", "testSqlite.db")
          val sqliteFile = tempFile.getAbsolutePath.replace("\\", "/")

          val sourceDatabaseConnectionSqlite =
            java.sql.DriverManager.getConnection(s"jdbc:sqlite:$sqliteFile")
          val stm = sourceDatabaseConnectionSqlite.createStatement()

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/databaseSchemaExtractor/extract-sqlite.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          stm.execute(
            "CREATE TABLE accounts (id DOUBLE(17), name VARCHAR(254), description CLOB(2147483647), birthday DATE(8), salary DOUBLE(17), points DECIMAL(4,2))"
          )
          stm.execute(
            "CREATE TABLE person (id DOUBLE(17), name VARCHAR(254), vorname VARCHAR(100), birthday DATE(8))"
          )

          val dfasdl = extractFromDatabase(sourceDatabaseConnectionSqlite,
                                           ExtractorMetaData(dfasdlNamePart = "CHANGEME"))
          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))

          sourceDatabaseConnectionSqlite.close()
          tempFile.delete()
        }
      }

      describe("with primary key, foreign key, auto increment using H2") {
        it("should work") {
          val connection = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:test")
          val statement  = connection.createStatement()

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/databaseSchemaExtractor/extract-tables-h2-pk-fk-ai.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          statement.execute(
            "CREATE TABLE t1 (id BIGINT(19) AUTO_INCREMENT, name VARCHAR(254), PRIMARY KEY(id))"
          )
          statement.execute(
            "CREATE TABLE t2 (id2 BIGINT(19) AUTO_INCREMENT, fkid BIGINT(19), FOREIGN KEY(fkid) REFERENCES t1(id))"
          )

          val uri = new URI(connection.getMetaData.getURL)
          val databaseDriver: SupportedDatabase = extractSupportedDatabaseFromUri(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }
          val databaseName: String = extractDatabaseNameFromURI(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }

          val s      = prepareGetAllTablesStatement(connection, databaseDriver)
          val tables = getTables(s, databaseName)

          val expected = Vector(Table("t1", List("id")), Table("t2", List.empty))

          tables should be(expected)

          val dfasdl =
            extractFromDatabase(connection, ExtractorMetaData(dfasdlNamePart = "CHANGEME"))
          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))

          statement.execute("SHUTDOWN")
          connection.close()
        }
      }

      describe("with multiple primary keys using H2") {
        it("should work") {
          val connection = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:test")
          val statement  = connection.createStatement()

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/databaseSchemaExtractor/extract-tables-h2-multiple-pk.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          statement.execute(
            "CREATE TABLE t3 (id BIGINT(19) not null, id2 BIGINT(19) not null, name VARCHAR(254))"
          )
          statement.execute("ALTER TABLE t3 ADD PRIMARY KEY(id, id2)")

          val uri = new URI(connection.getMetaData.getURL)
          val databaseDriver: SupportedDatabase = extractSupportedDatabaseFromUri(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }
          val databaseName: String = extractDatabaseNameFromURI(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }

          val s        = prepareGetAllTablesStatement(connection, databaseDriver)
          val tables   = getTables(s, databaseName)
          val expected = Vector(Table("t3", List("id", "id2")))

          tables should be(expected)

          val dfasdl =
            extractFromDatabase(connection, ExtractorMetaData(dfasdlNamePart = "CHANGEME"))
          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))

          statement.execute("SHUTDOWN")
          connection.close()
        }
      }

      describe("with primary key and foreign key using H2") {
        it("should work") {
          val connection = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:test")
          val statement  = connection.createStatement()

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/databaseSchemaExtractor/extract-tables-h2-pk-fk.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          statement.execute("CREATE TABLE t4 (id BIGINT(19), name VARCHAR(254), PRIMARY KEY(id))")
          statement.execute(
            "CREATE TABLE t5 (id2 BIGINT(19), fkid BIGINT(19), FOREIGN KEY(fkid) REFERENCES t4(id))"
          )

          val uri = new URI(connection.getMetaData.getURL)
          val databaseDriver: SupportedDatabase = extractSupportedDatabaseFromUri(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }
          val databaseName: String = extractDatabaseNameFromURI(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }

          val s      = prepareGetAllTablesStatement(connection, databaseDriver)
          val tables = getTables(s, databaseName)

          val expected = Vector(Table("t4", List("id")), Table("t5", List.empty))

          tables should be(expected)

          val dfasdl =
            extractFromDatabase(connection, ExtractorMetaData(dfasdlNamePart = "CHANGEME"))
          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))

          statement.execute("SHUTDOWN")
          connection.close()
        }
      }

      describe("with auto increment using H2") {
        it("should work") {
          val connection = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:test")
          val statement  = connection.createStatement()

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/databaseSchemaExtractor/extract-tables-h2-ai.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          statement.execute("CREATE TABLE t6 (id BIGINT(19) AUTO_INCREMENT, name VARCHAR(254))")

          val uri = new URI(connection.getMetaData.getURL)
          val databaseDriver: SupportedDatabase = extractSupportedDatabaseFromUri(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }
          val databaseName: String = extractDatabaseNameFromURI(uri) match {
            case -\/(failure) => fail(failure)
            case \/-(success) => success
          }

          val s      = prepareGetAllTablesStatement(connection, databaseDriver)
          val tables = getTables(s, databaseName)

          val expected = Vector(Table("t6", List()))

          tables should be(expected)

          val dfasdl =
            extractFromDatabase(connection, ExtractorMetaData(dfasdlNamePart = "CHANGEME"))
          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))

          statement.execute("SHUTDOWN")
          connection.close()
        }
      }
    }

    describe("createDatabaseName") {
      describe("with no special characters") {
        it("should return the input string") {
          val result = sanitizeGeneratedDatabaseName("foo")
          result should be("foo")
        }
      }

      describe("with special characters") {
        it("should replace the special characters") {
          val result = sanitizeGeneratedDatabaseName("foo-bar/signs.txt")
          result should be("foo-barsignstxt")
        }
      }

      describe("with numbers and no special characters") {
        it("should return the input string") {
          val result = sanitizeGeneratedDatabaseName("foo99")
          result should be("foo99")
        }
      }

      describe("with special characters and numbers") {
        it("should replace the special characters") {
          val result = sanitizeGeneratedDatabaseName("foo-bar99/signs.txt")
          result should be("foo-bar99signstxt")
        }
      }
    }
  }

  /**
    * Require a logging adapter that must be provided by the class
    * that uses the trait or one of the mixed in traits.
    *
    * @return An akka logging adapter.
    */
  override def log: LoggingAdapter =
    throw new RuntimeException("This method should not be called from a test!")

}
