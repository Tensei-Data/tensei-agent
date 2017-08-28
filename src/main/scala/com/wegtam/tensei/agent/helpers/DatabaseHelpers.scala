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

import java.sql.PreparedStatement
import java.time.LocalDateTime
import java.util.{ Locale, Properties }

import akka.event.LoggingAdapter
import akka.util.ByteString
import com.wegtam.tensei.adt.ConnectionInformation
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages
import org.dfasdl.utils.{ AttributeNames, ElementNames }
import org.w3c.dom.Element

import scalaz._

/**
  * A trait containing helper functions for database operations.
  */
trait DatabaseHelpers {

  /**
    * Require a logging adapter that must be provided by the class
    * that uses the trait or one of the mixed in traits.
    *
    * @return An akka logging adapter.
    */
  def log: LoggingAdapter

  /**
    * Try to connect to the database described in the connection information and return a `scalaz.Validation`.
    *
    * @param info The connection information data.
    * @return A validation that holds the connection or the error messages.
    */
  def connect(info: ConnectionInformation): Throwable \/ java.sql.Connection =
    \/.fromTryCatch {
      val props = new Properties()
      if (info.username.isDefined) props.setProperty("user", info.username.get)
      if (info.password.isDefined) props.setProperty("password", info.password.get)
      java.sql.DriverManager.getConnection(info.uri.toString, props)
    }

  /**
    * Create an ANSI SQL SELECT COUNT(*) statement the check if an entry exists within
    * the database.
    *
    * @param tableName   The name of the database table.
    * @param columnNames A list of column names that should be used in the `WHERE` clause.
    * @return A string holding the select statement.
    * @throws IllegalArgumentException If one of the parameters if empty.
    */
  def createCountStatement(tableName: String, columnNames: List[String]): String = {
    if (tableName.isEmpty) throw new IllegalArgumentException("No table name given!")
    if (columnNames.isEmpty) throw new IllegalArgumentException("No column names given!")

    s"SELECT COUNT(*) FROM $tableName WHERE ${columnNames.map(col => s"$col = ?").mkString(" AND ")}"
  }

  /**
    * Create an ANSI SQL INSERT statement from the given parameters.
    *
    * @param tableName         The name of the database table.
    * @param columnNames       A list of column names.
    * @param autoIncrementColumnNames A list of column names that are auto increment columns.
    * @return A string holding the insert statement.
    * @throws IllegalArgumentException If one of the parameters is empty.
    */
  def createInsertStatement(
      tableName: String,
      columnNames: List[String],
      autoIncrementColumnNames: List[String] = List.empty[String]
  ): String = {
    if (tableName.isEmpty) throw new IllegalArgumentException("No table name given!")
    if (columnNames.isEmpty) throw new IllegalArgumentException("No column names given!")

    val columnPlaceHolders =
      columnNames.map(c => if (autoIncrementColumnNames.contains(c)) "DEFAULT" else "?")
    s"INSERT INTO $tableName (${columnNames.mkString(", ")}) VALUES(${columnPlaceHolders.mkString(", ")})"
  }

  /**
    * Create an ANSI SQL UPDATE statement from the given parameters.
    *
    * @param tableName         The name of the database table.
    * @param columnNames       A list of column names.
    * @param primaryKeyColumns A list of primary key column names.
    * @return A string holding the update statement.
    * @throws IllegalArgumentException If one of the parameters is empty.
    */
  def createUpdateStatement(tableName: String,
                            columnNames: List[String],
                            primaryKeyColumns: List[String]): String = {
    if (tableName.isEmpty) throw new IllegalArgumentException("No table name given!")
    if (columnNames.isEmpty) throw new IllegalArgumentException("No column names given!")
    if (primaryKeyColumns.isEmpty)
      throw new IllegalArgumentException("No primary key columns given!")

    s"""UPDATE $tableName SET ${columnNames
      .map(col => s"$col = ?")
      .mkString(", ")} WHERE ${primaryKeyColumns.map(col => s"$col = ?").mkString(" AND ")}"""
  }

  /**
    * Extract the driver name from the given URI.
    *
    * @param uri An URI describing a JDBC connection.
    * @return Either the database driver name or an error.
    */
  def extractDatabaseDriverNameFromUri(uri: java.net.URI): Throwable \/ String =
    \/.fromTryCatch {
      val driverUri = new java.net.URI(uri.getSchemeSpecificPart)
      if (driverUri.getScheme.isEmpty)
        throw new IllegalArgumentException(
          s"Unable to extract database driver name from uri: $uri"
        )

      driverUri.getScheme
    }

  /**
    * Extract the supported database type from the given URI.
    *
    * @param uri A jdbc connection uri.
    * @return Either the supported database type or an error.
    */
  def extractSupportedDatabaseFromUri(uri: java.net.URI): Throwable \/ SupportedDatabase =
    \/.fromTryCatch {
      val driverUri = new java.net.URI(uri.getSchemeSpecificPart)
      if (driverUri.getScheme.isEmpty)
        throw new IllegalArgumentException(
          s"Unable to extract database driver name from uri: $uri"
        )

      toSupportedDatabase(driverUri.getScheme)
    }

  /**
    * Generate a `SupportedDatabase` type from the given string which should be
    * a jdbc driver name.
    *
    * @param s A string holding jdbc driver name.
    * @throws IllegalArgumentException If the given string does not contain a supported database.
    * @return The supported database type if possible.
    */
  @throws[IllegalArgumentException]
  def toSupportedDatabase(s: String): SupportedDatabase =
    s match {
      case "derby"       => Derby
      case "firebirdsql" => Firebird
      case "h2"          => H2
      case "hsqldb"      => HyperSql
      case "mariadb"     => MariaDb
      case "mysql"       => MySql
      case "oracle"      => Oracle
      case "postgresql"  => PostgreSql
      case "sqlite"      => SQLite
      case "sqlserver"   => SqlServer
      case _             => throw new IllegalArgumentException(s"Unsupported database: $s")
    }

  /**
    * Tries to extract the database name from the given URI which must be a JDBC URL.
    * <br/>
    * If the database name contains forward slashes (`/`) then it is split up and the
    * part after the last slash is returned.
    *
    * @param uri An URI describing a JDBC connection.
    * @return A validation that holds the database name or the error messages.
    */
  def extractDatabaseNameFromURI(uri: java.net.URI): Throwable \/ String =
    \/.fromTryCatch {
      val driverURI = new java.net.URI(uri.getSchemeSpecificPart)
      extractDatabaseDriverNameFromUri(uri) match {
        case -\/(failure) => throw failure
        case \/-(success) =>
          success match {
            case "inetdae" =>
              driverURI.getSchemeSpecificPart
                .split("\\?")
                .filter(_.startsWith("database"))
                .head
                .split("=")
                .takeRight(1)
                .head
            case "sqlserver" =>
              driverURI.getSchemeSpecificPart
                .split(";")
                .filter(_.startsWith("databaseName"))
                .head
                .split("=")
                .takeRight(1)
                .head
            case "rmi" =>
              extractDatabaseNameFromURI(new java.net.URI(driverURI.getPath.substring(1))) match {
                case -\/(f) => throw f
                case \/-(s) => s.split("\\/").lastOption.getOrElse(s)
              }
            case _ =>
              val name =
                if (driverURI.getPath == null)
                  driverURI.getSchemeSpecificPart.split(":").takeRight(1).head
                else
                  driverURI.getPath
              if (name.startsWith("/") || name.startsWith("@"))
                name.substring(1).split("\\/").lastOption.getOrElse(name.substring(1))
              else
                name.split("\\/").lastOption.getOrElse(name)
          }
      }
    }

  /**
    * Return the database column name of the given element. This is specified
    * by the attribute `DB_COLUMN_NAME`. If the attribute is missing then the
    * id of the element is used as database column name.
    *
    * @param e A dfasdl element description.
    * @return The column name of the element in a database.
    */
  def getDatabaseColumnName(e: Element): String =
    if (e.hasAttribute(AttributeNames.DB_COLUMN_NAME))
      e.getAttribute(AttributeNames.DB_COLUMN_NAME)
    else
      e.getAttribute("id")

  /**
    * A wrapper for compatibility reasons that can be remove in a future release.
    *
    * @param e                   DFASDL element that represents a column.
    * @param databaseDriverName  The name of the database driver.
    * @return The name of the column type.
    * @throws IllegalArgumentException If the database driver or the element is not supported.
    * @deprecated Please use the properly typed function.
    */
  def getDatabaseColumnType(e: Element, databaseDriverName: String): String =
    getDatabaseColumnType(e, toSupportedDatabase(databaseDriverName), "UTF-8")

  /**
    * Get the column type for the given DFASDL element and database driver.
    *
    * @param e                  DFASDL element that represents a column.
    * @param databaseDriverName The name of the database driver.
    * @param defaultEncoding    The default encoding to be used if the element has none set.
    * @return The name of the column type.
    * @throws IllegalArgumentException If the database driver or the element is not supported.
    */
  def getDatabaseColumnType(e: Element,
                            databaseDriverName: SupportedDatabase,
                            defaultEncoding: String): String = {
    val nodeName = e.getNodeName
    val encoding =
      if (e.hasAttribute(AttributeNames.ENCODING))
        e.getAttribute(AttributeNames.ENCODING)
      else
        defaultEncoding

    databaseDriverName match {
      case Derby =>
        nodeName match {
          case ElementNames.BINARY | ElementNames.BINARY_64 | ElementNames.BINARY_HEX =>
            "BLOB"
          case ElementNames.DATE =>
            "DATE"
          case ElementNames.DATETIME =>
            "TIMESTAMP"
          case ElementNames.FORMATTED_NUMBER =>
            "DOUBLE PRECISION"
          case ElementNames.FORMATTED_STRING | ElementNames.STRING =>
            if (e.hasAttribute(AttributeNames.LENGTH))
              s"CHAR(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.MAX_LENGTH))
              s"VARCHAR(${e.getAttribute(AttributeNames.MAX_LENGTH)})"
            else
              "CLOB"
          case ElementNames.FORMATTED_TIME | ElementNames.TIME =>
            "TIME"
          case ElementNames.NUMBER =>
            if (e.hasAttribute(AttributeNames.LENGTH) && e.hasAttribute(AttributeNames.PRECISION))
              s"DECIMAL(${e.getAttribute(AttributeNames.LENGTH)},${e.getAttribute(AttributeNames.PRECISION)})"
            else if (e.hasAttribute(AttributeNames.LENGTH))
              s"DECIMAL(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                       AttributeNames.DB_AUTO_INCREMENT
                     ) == "true")
              "BIGINT GENERATED ALWAYS AS IDENTITY"
            else
              "BIGINT"
          case _ =>
            throw new IllegalArgumentException(s"Illegal data element name: $nodeName!")
        }
      case Firebird =>
        nodeName match {
          case ElementNames.BINARY | ElementNames.BINARY_64 | ElementNames.BINARY_HEX =>
            "BLOB"
          case ElementNames.DATE =>
            "DATE"
          case ElementNames.DATETIME =>
            "TIMESTAMP"
          case ElementNames.FORMATTED_NUMBER =>
            "DOUBLE PRECISION"
          case ElementNames.FORMATTED_STRING | ElementNames.STRING =>
            if (e.hasAttribute(AttributeNames.LENGTH) && e
                  .getAttribute(AttributeNames.LENGTH)
                  .toLong < 32767)
              s"CHAR(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.MAX_LENGTH) && e
                       .getAttribute(AttributeNames.MAX_LENGTH)
                       .toLong < 32767)
              s"VARCHAR(${e.getAttribute(AttributeNames.MAX_LENGTH)})"
            else
              "BLOB SUB_TYPE TEXT"
          case ElementNames.FORMATTED_TIME | ElementNames.TIME =>
            "TIME"
          case ElementNames.NUMBER =>
            if (e.hasAttribute(AttributeNames.LENGTH) && e.hasAttribute(AttributeNames.PRECISION))
              s"DECIMAL(${e.getAttribute(AttributeNames.LENGTH)},${e.getAttribute(AttributeNames.PRECISION)})"
            else if (e.hasAttribute(AttributeNames.LENGTH))
              s"DECIMAL(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                       AttributeNames.DB_AUTO_INCREMENT
                     ) == "true")
              "BIGINT" // FIXME: AUTOINC
            else
              "BIGINT"
          case _ =>
            throw new IllegalArgumentException(s"Illegal data element name: $nodeName!")
        }
      case H2 =>
        nodeName match {
          case ElementNames.BINARY | ElementNames.BINARY_64 | ElementNames.BINARY_HEX =>
            "BLOB"
          case ElementNames.DATE =>
            "DATE"
          case ElementNames.DATETIME =>
            "TIMESTAMP"
          case ElementNames.FORMATTED_NUMBER =>
            if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                  AttributeNames.DB_AUTO_INCREMENT
                ) == "true")
              "DOUBLE IDENTITY"
            else
              "DOUBLE"
          case ElementNames.FORMATTED_STRING | ElementNames.STRING =>
            if (e.hasAttribute(AttributeNames.LENGTH))
              s"NCHAR(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.MAX_LENGTH))
              s"NVARCHAR2(${e.getAttribute(AttributeNames.MAX_LENGTH)})"
            else
              "NCLOB"
          case ElementNames.FORMATTED_TIME | ElementNames.TIME =>
            "TIME"
          case ElementNames.NUMBER =>
            if (e.hasAttribute(AttributeNames.LENGTH) && e.hasAttribute(AttributeNames.PRECISION))
              s"NUMBER(${e.getAttribute(AttributeNames.LENGTH)},${e.getAttribute(AttributeNames.PRECISION)})"
            else if (e.hasAttribute(AttributeNames.LENGTH))
              if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                    AttributeNames.DB_AUTO_INCREMENT
                  ) == "true")
                s"NUMBER(${e.getAttribute(AttributeNames.LENGTH)}) IDENTITY"
              else
                s"NUMBER(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                       AttributeNames.DB_AUTO_INCREMENT
                     ) == "true")
              "DOUBLE IDENTITY"
            else
              "DOUBLE"
          case _ =>
            throw new IllegalArgumentException(s"Illegal data element name: $nodeName!")
        }
      case HyperSql =>
        nodeName match {
          case ElementNames.BINARY | ElementNames.BINARY_64 | ElementNames.BINARY_HEX =>
            "BLOB"
          case ElementNames.DATE =>
            "DATE"
          case ElementNames.DATETIME =>
            "TIMESTAMP"
          case ElementNames.FORMATTED_NUMBER =>
            "DOUBLE"
          case ElementNames.FORMATTED_STRING | ElementNames.STRING =>
            if (e.hasAttribute(AttributeNames.LENGTH))
              s"CHAR(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.MAX_LENGTH))
              s"LONGVARCHAR(${e.getAttribute(AttributeNames.MAX_LENGTH)})"
            else
              "LONGVARCHAR"
          case ElementNames.FORMATTED_TIME | ElementNames.TIME =>
            "TIME"
          case ElementNames.NUMBER =>
            if (e.hasAttribute(AttributeNames.LENGTH) && e.hasAttribute(AttributeNames.PRECISION))
              s"DECIMAL(${e.getAttribute(AttributeNames.LENGTH)},${e.getAttribute(AttributeNames.PRECISION)})"
            else if (e.hasAttribute(AttributeNames.LENGTH))
              s"DECIMAL(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                       AttributeNames.DB_AUTO_INCREMENT
                     ) == "true")
              "BIGINT GENERATED ALWAYS AS IDENTITY"
            else
              "BIGINT"
          case _ =>
            throw new IllegalArgumentException(s"Illegal data element name: $nodeName!")
        }
      // microsoft
      case SqlServer =>
        nodeName match {
          case ElementNames.BINARY | ElementNames.BINARY_64 | ElementNames.BINARY_HEX =>
            "VARBINARY"
          case ElementNames.DATE =>
            "DATE"
          case ElementNames.DATETIME =>
            "DATETIME2"
          case ElementNames.FORMATTED_NUMBER =>
            "FLOAT"
          case ElementNames.FORMATTED_STRING | ElementNames.STRING =>
            if (e.hasAttribute(AttributeNames.LENGTH))
              s"NCHAR(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.MAX_LENGTH))
              s"NVARCHAR(${e.getAttribute(AttributeNames.MAX_LENGTH)})"
            else
              "NTEXT"
          case ElementNames.FORMATTED_TIME | ElementNames.TIME =>
            "TIME"
          case ElementNames.NUMBER =>
            if (e.hasAttribute(AttributeNames.LENGTH) && e.hasAttribute(AttributeNames.PRECISION))
              s"DECIMAL(${e.getAttribute(AttributeNames.LENGTH)},${e.getAttribute(AttributeNames.PRECISION)})"
            else if (e.hasAttribute(AttributeNames.LENGTH))
              s"DECIMAL(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                       AttributeNames.DB_AUTO_INCREMENT
                     ) == "true")
              "BIGINT IDENTITY"
            else
              "BIGINT"
          case _ =>
            throw new IllegalArgumentException(s"Illegal data element name: $nodeName!")
        }
      case MariaDb =>
        nodeName match {
          case ElementNames.BINARY | ElementNames.BINARY_64 | ElementNames.BINARY_HEX =>
            "LONGBLOB"
          case ElementNames.DATE =>
            "DATE"
          case ElementNames.DATETIME =>
            "DATETIME"
          case ElementNames.FORMATTED_NUMBER =>
            "DOUBLE"
          case ElementNames.FORMATTED_STRING | ElementNames.STRING =>
            val colType =
              if (e.hasAttribute(AttributeNames.LENGTH) && e
                    .getAttribute(AttributeNames.LENGTH)
                    .toLong < 256)
                s"CHAR(${e.getAttribute(AttributeNames.LENGTH)})"
              else if (e.hasAttribute(AttributeNames.MAX_LENGTH) && e
                         .getAttribute(AttributeNames.MAX_LENGTH)
                         .toLong < 65536)
                s"VARCHAR(${e.getAttribute(AttributeNames.MAX_LENGTH)})"
              else
                "LONGTEXT"
            s"$colType CHARACTER SET ${getMySQLCharacterSet(encoding)}"
          case ElementNames.FORMATTED_TIME | ElementNames.TIME =>
            "TIME"
          case ElementNames.NUMBER =>
            if (e.hasAttribute(AttributeNames.LENGTH) && e.hasAttribute(AttributeNames.PRECISION))
              s"NUMERIC(${e.getAttribute(AttributeNames.LENGTH)},${e.getAttribute(AttributeNames.PRECISION)})"
            else if (e.hasAttribute(AttributeNames.LENGTH))
              s"NUMERIC(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                       AttributeNames.DB_AUTO_INCREMENT
                     ) == "true")
              "BIGINT AUTO_INCREMENT"
            else
              "BIGINT"
          case _ =>
            throw new IllegalArgumentException(s"Illegal data element name: $nodeName!")
        }
      case MySql =>
        nodeName match {
          case ElementNames.BINARY | ElementNames.BINARY_64 | ElementNames.BINARY_HEX =>
            "LONGBLOB"
          case ElementNames.DATE =>
            "DATE"
          case ElementNames.DATETIME =>
            "DATETIME"
          case ElementNames.FORMATTED_NUMBER =>
            "DOUBLE"
          case ElementNames.FORMATTED_STRING | ElementNames.STRING =>
            val colType =
              if (e.hasAttribute(AttributeNames.LENGTH) && e
                    .getAttribute(AttributeNames.LENGTH)
                    .toLong < 256)
                s"CHAR(${e.getAttribute(AttributeNames.LENGTH)})"
              else if (e.hasAttribute(AttributeNames.MAX_LENGTH) && e
                         .getAttribute(AttributeNames.MAX_LENGTH)
                         .toLong < 65536)
                s"VARCHAR(${e.getAttribute(AttributeNames.MAX_LENGTH)})"
              else
                "LONGTEXT"
            s"$colType CHARACTER SET ${getMySQLCharacterSet(encoding)}"
          case ElementNames.FORMATTED_TIME | ElementNames.TIME =>
            "TIME"
          case ElementNames.NUMBER =>
            if (e.hasAttribute(AttributeNames.LENGTH) && e.hasAttribute(AttributeNames.PRECISION))
              s"NUMERIC(${e.getAttribute(AttributeNames.LENGTH)},${e.getAttribute(AttributeNames.PRECISION)})"
            else if (e.hasAttribute(AttributeNames.LENGTH))
              s"NUMERIC(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                       AttributeNames.DB_AUTO_INCREMENT
                     ) == "true")
              "BIGINT AUTO_INCREMENT"
            else
              "BIGINT"
          case _ =>
            throw new IllegalArgumentException(s"Illegal data element name: $nodeName!")
        }
      case Oracle =>
        nodeName match {
          case ElementNames.BINARY | ElementNames.BINARY_64 | ElementNames.BINARY_HEX =>
            "BLOB"
          case ElementNames.DATE =>
            "DATE"
          case ElementNames.DATETIME =>
            "TIMESTAMP WITH TIME ZONE"
          case ElementNames.FORMATTED_NUMBER =>
            if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                  AttributeNames.DB_AUTO_INCREMENT
                ) == "true")
              "NUMBER GENERATED BY DEFAULT AS IDENTITY" // IDENTITY only works in Oracle database 12c and upwards!
            else
              "NUMBER"
          case ElementNames.FORMATTED_STRING | ElementNames.STRING =>
            if (e.hasAttribute(AttributeNames.LENGTH) && e
                  .getAttribute(AttributeNames.LENGTH)
                  .toLong <= 2000)
              s"NCHAR(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.MAX_LENGTH) && e
                       .getAttribute(AttributeNames.MAX_LENGTH)
                       .toLong <= 4000)
              s"NVARCHAR2(${e.getAttribute(AttributeNames.MAX_LENGTH)})"
            else
              "NCLOB"
          case ElementNames.FORMATTED_TIME =>
            "NVARCHAR2(2000)" // a length value is required for this data type
          case ElementNames.TIME =>
            "TIMESTAMP"
          case ElementNames.NUMBER =>
            if (e.hasAttribute(AttributeNames.LENGTH) && e.hasAttribute(AttributeNames.PRECISION))
              s"NUMBER(${e.getAttribute(AttributeNames.LENGTH)},${e.getAttribute(AttributeNames.PRECISION)})"
            else if (e.hasAttribute(AttributeNames.LENGTH))
              if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                    AttributeNames.DB_AUTO_INCREMENT
                  ) == "true")
                s"NUMBER(${e.getAttribute(AttributeNames.LENGTH)}) GENERATED BY DEFAULT AS IDENTITY" // IDENTITY only works in Oracle database 12c and upwards!
              else
                s"NUMBER(${e.getAttribute(AttributeNames.LENGTH)})"
            else if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                       AttributeNames.DB_AUTO_INCREMENT
                     ) == "true")
              "NUMBER GENERATED BY DEFAULT AS IDENTITY" // IDENTITY only works in Oracle database 12c and upwards!
            else
              "NUMBER"
          case _ =>
            throw new IllegalArgumentException(s"Illegal data element name: $nodeName!")
        }
      case PostgreSql =>
        nodeName match {
          case ElementNames.BINARY | ElementNames.BINARY_64 | ElementNames.BINARY_HEX =>
            "BYTEA"
          case ElementNames.DATE =>
            "DATE"
          case ElementNames.DATETIME =>
            "TIMESTAMPTZ"
          case ElementNames.FORMATTED_NUMBER =>
            // FIXME Decimals should only be produced if a concrete decimal DFASDL type is present!
            if (e.hasAttribute(AttributeNames.MAX_DIGITS) && e.hasAttribute(
                  AttributeNames.MAX_PRECISION
                ))
              s"DECIMAL(${e.getAttribute(AttributeNames.MAX_DIGITS)},${e.getAttribute(AttributeNames.MAX_PRECISION)})"
            else if (e.hasAttribute(AttributeNames.MAX_DIGITS))
              s"DECIMAL(${e.getAttribute(AttributeNames.MAX_DIGITS)})"
            else
              "DOUBLE PRECISION"
          case ElementNames.FORMATTED_STRING | ElementNames.STRING =>
            if (e.hasAttribute(AttributeNames.LENGTH) && e
                  .getAttribute(AttributeNames.LENGTH)
                  .toLong < 256)
              s"CHARACTER (${e.getAttribute(AttributeNames.LENGTH)})"
            // If the MAX_LENGTH value is bigger than 65535, we use a TEXT field -> size depends on the system and installation
            else if (e.hasAttribute(AttributeNames.MAX_LENGTH) && e
                       .getAttribute(AttributeNames.MAX_LENGTH)
                       .toLong < 65536)
              s"CHARACTER VARYING (${e.getAttribute(AttributeNames.MAX_LENGTH)})"
            else
              "TEXT"
          case ElementNames.FORMATTED_TIME =>
            "TIMESTAMPTZ"
          case ElementNames.TIME =>
            "TIMETZ"
          case ElementNames.NUMBER =>
            if (e.hasAttribute(AttributeNames.LENGTH) && e.hasAttribute(AttributeNames.PRECISION))
              s"DECIMAL(${e.getAttribute(AttributeNames.LENGTH)},${e.getAttribute(AttributeNames.PRECISION)})"
            else if (e.hasAttribute(AttributeNames.PRECISION))
              if (e.getAttribute(AttributeNames.PRECISION) == "0")
                if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                      AttributeNames.DB_AUTO_INCREMENT
                    ) == "true")
                  "BIGSERIAL"
                else
                  "BIGINT"
              else
                s"DECIMAL(${e.getAttribute(AttributeNames.PRECISION)})"
            else if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                       AttributeNames.DB_AUTO_INCREMENT
                     ) == "true")
              "BIGSERIAL"
            else
              "BIGINT"
          case _ =>
            throw new IllegalArgumentException(s"Illegal data element name: $nodeName!")
        }
      case SQLite =>
        nodeName match {
          case ElementNames.BINARY | ElementNames.BINARY_64 | ElementNames.BINARY_HEX =>
            "BLOB"
          case ElementNames.DATE =>
            "TEXT"
          case ElementNames.DATETIME =>
            "TEXT"
          case ElementNames.FORMATTED_NUMBER =>
            "REAL"
          case ElementNames.FORMATTED_STRING | ElementNames.STRING =>
            "TEXT"
          case ElementNames.FORMATTED_TIME | ElementNames.TIME =>
            "TEXT"
          case ElementNames.NUMBER =>
            if (e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e.getAttribute(
                  AttributeNames.DB_AUTO_INCREMENT
                ) == "true")
              "INTEGER PRIMARY KEY AUTOINCREMENT"
            else
              "INTEGER"
          case _ =>
            throw new IllegalArgumentException(s"Illegal data element name: $nodeName!")
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported database type: $databaseDriverName!")
    }
  }

  /**
    * Map the DFASDL encoding to the MySQL character encodings.
    *
    * @param encoding Encoding string defined in the DFASDL.
    * @return The MySQL character encoding name.
    * @throws IllegalArgumentException If the encoding is not supported.
    */
  def getMySQLCharacterSet(encoding: String): String =
    // FIXME add additional encodings
    encoding.toLowerCase(Locale.ROOT) match {
      case "utf-8" => "utf8"
      case _ =>
        throw new IllegalArgumentException(s"Unsupported character encoding: $encoding")
    }

  /**
    * This helper function sets a parameter for a prepared statement.
    * '''Although this function returns the statement it operates directly upon it!'''
    *
    * @param statement The prepared statement.
    * @param index     The index of the parameter. Remember that the JDBC column index starts at `1`!!
    * @param message   The writer message that contains the data for the column.
    * @param database  The type of the database.
    */
  def setStatementParameter(
      statement: PreparedStatement,
      index: Int,
      message: BaseWriterMessages.WriteData
  )(implicit database: SupportedDatabase): PreparedStatement =
    database match {
      case SQLite =>
        message.data match {
          case data: Array[Byte] =>
            statement.setBytes(index, data) // TODO We must put binary data into a blob!
          case data: java.math.BigDecimal => statement.setString(index, data.toString)
          case data: ByteString =>
            val s = data.utf8String
            if (s == "NULL") {
              log.debug("Setting string column with data value '{}' to NULL!", data)
              setNull(statement, index)
            } else
              statement.setString(index, s)
          case data: Long => statement.setLong(index, data)
          case data: Int  => statement.setInt(index, data)
          case data: String =>
            if (data == "NULL") {
              log.debug("Setting string column with data value '{}' to NULL!", data)
              setNull(statement, index)
            } else
              statement.setString(index, data)
          case data: java.sql.Date =>
            statement.setString(index, new java.sql.Timestamp(data.getTime).toString)
          case data: java.sql.Time =>
            statement.setString(index, new java.sql.Timestamp(data.getTime).toString)
          case data: java.sql.Timestamp => statement.setString(index, data.toString)
          case data: LocalDateTime =>
            statement.setString(index, java.sql.Timestamp.valueOf(data).toString)
          case None => setNull(statement, index)
          case _    =>
            // We assume that the value is an empty column here!
            log.warning("Unexpected data type '{}' from {}, setting column {} to null!",
                        message.data.getClass,
                        message.metaData,
                        index)
            log.debug("Illegal data type: '{}' ({})", message.data, message.data.getClass)
            setNull(statement, index)
        }
        statement
      case _ =>
        message.data match {
          case data: Array[Byte] =>
            statement.setBytes(index, data) // TODO We must put binary data into a blob!
          case data: java.math.BigDecimal => statement.setBigDecimal(index, data)
          case data: ByteString =>
            val s = data.utf8String
            if (s == "NULL") {
              log.debug("Setting string column with data value '{}' to NULL!", data)
              setNull(statement, index)
            } else
              statement.setString(index, s)
          case data: Long => statement.setLong(index, data)
          case data: Int  => statement.setInt(index, data)
          case data: String =>
            if (data == "NULL") {
              log.debug("Setting string column with data value '{}' to NULL!", data)
              setNull(statement, index)
            } else
              statement.setString(index, data)
          case data: java.sql.Date      => statement.setDate(index, data)
          case data: java.sql.Time      => statement.setTime(index, data)
          case data: java.sql.Timestamp => statement.setTimestamp(index, data)
          case data: LocalDateTime =>
            statement.setTimestamp(index, java.sql.Timestamp.valueOf(data))
          case None => setNull(statement, index)
          case _    =>
            // We assume that the value is an empty column here!
            log.warning("Unexpected data type '{}' from {}, setting column {} to null!",
                        message.data.getClass,
                        message.metaData,
                        index)
            log.debug("Illegal data type: '{}' ({})", message.data, message.data.getClass)
            setNull(statement, index)
        }
        statement
    }

  /**
    * This function sets a `null` value for the given parameter index in the given prepared statement.
    *
    * @param statement The prepared statement.
    * @param index     The index of the parameter. Remember that the JDBC column index starts at `1`!!
    * @param database  The type of the database.
    */
  def setNull(statement: PreparedStatement,
              index: Int)(implicit database: SupportedDatabase): PreparedStatement = {
    database match {
      case Oracle =>
        log.debug(
          "Using workaround to set prepared statement column {} to 'null' for Oracle JDBC driver!",
          index
        )
        statement.setObject(index, null)
      case _ =>
        val columnType = statement.getParameterMetaData.getParameterType(index)
        statement.setNull(index, columnType)
    }
    statement
  }
}
