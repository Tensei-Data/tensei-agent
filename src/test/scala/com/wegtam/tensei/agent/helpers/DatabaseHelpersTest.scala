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

import java.net.URI

import akka.event.LoggingAdapter
import com.wegtam.tensei.adt.ConnectionInformation
import com.wegtam.tensei.agent.{ DatabaseSpec, XmlTestHelpers }
import org.dfasdl.utils.{ AttributeNames, ElementNames }

import scalaz._

/**
  * @todo Move to integration tests!
  */
class DatabaseHelpersTest extends DatabaseSpec with XmlTestHelpers with DatabaseHelpers {
  describe("DatabaseHelpers") {
    describe("connect") {
      describe("failures") {
        describe("when called with a non-existing database driver") {
          it("should return failures") {
            val source =
              new ConnectionInformation(new URI("jdbc:theUnknownDatabaseDriver://localhost/my-db"),
                                        None)
            connect(source) match {
              case \/-(_) => fail("This database connection should be impossible!")
              case -\/(f) => f.getMessage should startWith("No suitable driver found")
            }
          }
        }

        describe("using an existing database driver") {
          describe("when given a wrong hostname") {
            it("should return failures") {
              val source = new ConnectionInformation(
                new URI("jdbc:postgresql://this.hostname.does.hopefully.not.exist/my-db"),
                None
              )
              connect(source) match {
                case \/-(_) => fail("This database connection should be impossible!")
                case -\/(f) => // A failure is expected.
              }
            }
          }

          describe("when given a wrong port") {
            it("should return failures") {
              val source =
                new ConnectionInformation(new URI("jdbc:postgresql://localhost:65535/my-db"), None)
              connect(source) match {
                case \/-(_) => fail("This database connection should be impossible!")
                case -\/(f) => f.getMessage should include("refused")
              }
            }
          }

          describe("when given an illegal port") {
            it("should return failures") {
              val source =
                new ConnectionInformation(new URI("jdbc:postgresql://localhost:65536/my-db"), None)
              connect(source) match {
                case \/-(_) => fail("This database connection should be impossible!")
                case -\/(f) => f.getCause.getMessage should include("port out of range")
              }
            }
          }

          describe("when given wrong credentials") {
            it("should return failures") {
              val source =
                new ConnectionInformation(new URI("jdbc:postgresql://localhost/my-db"), None)
              connect(source) match {
                case \/-(_) => fail("This database connection should be impossible!")
                case -\/(f) => // A failure is expected.
              }
            }
          }

          describe("when given wrong filename (derby or h2)") {
            it("should return failures") {
              val source =
                new ConnectionInformation(new URI("jdbc:h2://this.path.should.not.be.accessible"),
                                          None)
              connect(source) match {
                case \/-(_) => fail("This database connection should be impossible!")
                case -\/(f) => // A failure is expected.
              }
            }
          }
        }
      }

      describe("success") {
        describe("using an in memory db") {
          describe("without authorisation") {
            val db     = createTestDatabase("my-test-db")
            val source = new ConnectionInformation(new URI("jdbc:h2:mem:my-test-db"), None)
            connect(source) match {
              case \/-(c) =>
                c.close()
                db.close()
              case -\/(f) =>
                fail(f.getMessage)
            }
          }
        }
      }
    }

    describe("createCountStatement") {
      describe("given no table name") {
        it("should throw an exception") {
          an[IllegalArgumentException] should be thrownBy createCountStatement("", List("foo"))
        }
      }

      describe("given no columns") {
        it("should throw an exception") {
          an[IllegalArgumentException] should be thrownBy createCountStatement("foo",
                                                                               List.empty[String])
        }
      }

      describe("given valid parameters") {
        it("should create the count statement") {
          val s = createCountStatement("TESTTABLE", List("A", "B"))
          s shouldEqual "SELECT COUNT(*) FROM TESTTABLE WHERE A = ? AND B = ?"
        }
      }
    }

    describe("createInsertStatement") {
      describe("given no table name") {
        it("should throw an exception") {
          an[IllegalArgumentException] should be thrownBy createInsertStatement("", List("foo"))
        }
      }

      describe("given no columns") {
        it("should throw an exception") {
          an[IllegalArgumentException] should be thrownBy createInsertStatement("foo",
                                                                                List.empty[String])
        }
      }

      describe("given valid parameters") {
        it("should create the insert statement") {
          val s = createInsertStatement("TESTTABLE", List("A", "B", "C", "D"))
          s shouldEqual "INSERT INTO TESTTABLE (A, B, C, D) VALUES(?, ?, ?, ?)"
        }
      }
    }

    describe("createUpdateStatement") {
      describe("given no table name") {
        it("should throw an exception") {
          an[IllegalArgumentException] should be thrownBy createUpdateStatement("",
                                                                                List("foo"),
                                                                                List("bar"))
        }
      }

      describe("given no columns") {
        it("should throw an exception") {
          an[IllegalArgumentException] should be thrownBy createUpdateStatement("foo",
                                                                                List.empty[String],
                                                                                List("bar"))
        }
      }

      describe("given no primary key columns") {
        it("should throw an exception") {
          an[IllegalArgumentException] should be thrownBy createUpdateStatement("foo",
                                                                                List("bar"),
                                                                                List.empty[String])
        }
      }

      describe("given a simple primary key") {
        it("should create the update statement") {
          val s = createUpdateStatement("TESTTABLE", List("A", "B", "C", "D"), List("A"))
          s shouldEqual "UPDATE TESTTABLE SET A = ?, B = ?, C = ?, D = ? WHERE A = ?"
        }
      }

      describe("given a complex primary key") {
        it("should create the update statement") {
          val s =
            createUpdateStatement("TESTTABLE", List("A", "B", "C", "D", "E"), List("A", "C", "E"))
          s shouldEqual "UPDATE TESTTABLE SET A = ?, B = ?, C = ?, D = ?, E = ? WHERE A = ? AND C = ? AND E = ?"
        }
      }
    }

    describe("toSupportedDatabase") {
      it("should map supported databases") {
        val e = Map(
          "derby"       -> Derby,
          "firebirdsql" -> Firebird,
          "h2"          -> H2,
          "hsqldb"      -> HyperSql,
          "mariadb"     -> MariaDb,
          "mysql"       -> MySql,
          "oracle"      -> Oracle,
          "postgresql"  -> PostgreSql,
          "sqlite"      -> SQLite,
          "sqlserver"   -> SqlServer
        )

        e.keySet.foreach { key =>
          withClue(s"Database $key should map correctly!")(
            toSupportedDatabase(key) should be(e(key))
          )
        }
      }

      it("should thrown an exception for unknown databases") {
        an[IllegalArgumentException] should be thrownBy toSupportedDatabase("I am no database!")
      }
    }

    describe("extractDatabaseNameFromURI") {
      describe("given an invalid JDBC URI") {
        it("should return a failure") {
          val uri = new URI("jdbc://is-this-even-a-valid-uri?possibly=not")
          extractDatabaseNameFromURI(uri) match {
            case -\/(failure) => // We expect a failure here.
            case \/-(success) => fail(s"The uri $uri should not be parsable!")
          }
        }
      }

      describe("given a valid JDBC URI") {
        val expectedDbName = "my-database"
        val uris = List(
          s"jdbc:db2://localhost:12345/$expectedDbName",
          s"jdbc:odbc:$expectedDbName",
          s"jdbc:oracle:thin:@localhost:12345:$expectedDbName",
          s"jdbc:oracle:thin:@localhost:$expectedDbName",
          s"jdbc:oracle:thin:@$expectedDbName",
          s"jdbc:pointbase://embedded:12345/$expectedDbName",
          s"jdbc:cloudscape:$expectedDbName",
          s"jdbc:rmi://localhost:12345/jdbc:cloudscape:$expectedDbName",
          s"jdbc:firebirdsql://localhost:12345/$expectedDbName",
          s"jdbc:firebirdsql://localhost/$expectedDbName",
          s"jdbc:firebirdsql:$expectedDbName",
          s"jdbc:interbase://localhost/$expectedDbName",
          s"jdbc:HypersonicSQL:$expectedDbName",
          s"jdbc:JTurbo://localhost:12345/$expectedDbName",
          s"jdbc:inetdae:localhost:12345?database=$expectedDbName",
          s"jdbc:sqlserver://localhost:12345;databaseName=$expectedDbName",
          s"jdbc:mysql://localhost:12345/$expectedDbName",
          s"jdbc:oracle:oci8:@$expectedDbName",
          s"jdbc:oracle:oci:@$expectedDbName",
          s"jdbc:postgresql://localhost:12345/$expectedDbName"
        )

        uris foreach { u =>
          describe(s"given the uri: $u") {
            it(s"should return Success($expectedDbName)") {
              val uri = new URI(u)
              extractDatabaseNameFromURI(uri) match {
                case -\/(failure) => fail(failure)
                case \/-(success) => success should equal(expectedDbName)
              }
            }
          }
        }
      }
    }

    describe("getDatabaseColumnType") {
      describe("for database derby") {
        val databaseDriverName = "derby"
        val doc                = createTestDocumentBuilder().newDocument()

        describe("for binary elements") {
          it("should return the correct binary column type") {
            val columnType = "BLOB"
            val types      = List(ElementNames.BINARY, ElementNames.BINARY_64, ElementNames.BINARY_HEX)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for date and time elements") {
          it("should return the correct date column type") {
            val columnType = "DATE"
            val types      = List(ElementNames.DATE)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct datetime column type") {
            val columnType = "TIMESTAMP"
            val types      = List(ElementNames.DATETIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct time column type") {
            val columnType = "TIME"
            val types      = List(ElementNames.FORMATTED_TIME, ElementNames.TIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for numeric elements") {
          describe("without auto-increment") {
            it("should return the correct number column type") {
              val columnType = "DECIMAL"

              val fnum = doc.createElement(ElementNames.FORMATTED_NUMBER)
              withClue(
                s"Element ${ElementNames.FORMATTED_NUMBER} should map to DOUBLE PRECISION!"
              )(getDatabaseColumnType(fnum, databaseDriverName) should be("DOUBLE PRECISION"))

              val num0 = doc.createElement(ElementNames.NUMBER)
              withClue(s"Element ${ElementNames.NUMBER} should map to BIGINT!")(
                getDatabaseColumnType(num0, databaseDriverName) should be("BIGINT")
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.LENGTH, "10")
              num1.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)},${num1.getAttribute(AttributeNames.PRECISION)})"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.LENGTH, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.LENGTH)})"
                )
              }
            }
          }

          describe("with auto-increment") {
            it("should return the correct number column type") {
              val columnType = "DECIMAL"

              val fnum = doc.createElement(ElementNames.FORMATTED_NUMBER)
              fnum.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              withClue(
                s"Element ${ElementNames.FORMATTED_NUMBER} should map to DOUBLE PRECISION!"
              )(getDatabaseColumnType(fnum, databaseDriverName) should be("DOUBLE PRECISION"))

              val num0 = doc.createElement(ElementNames.NUMBER)
              num0.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              withClue(s"Element ${ElementNames.NUMBER} should map to BIGINT!")(
                getDatabaseColumnType(num0, databaseDriverName) should be(
                  "BIGINT GENERATED ALWAYS AS IDENTITY"
                )
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num1.setAttribute(AttributeNames.LENGTH, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)})"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num2.setAttribute(AttributeNames.LENGTH, "8")
              num2.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.LENGTH)},${num2.getAttribute(AttributeNames.PRECISION)})"
                )
              }
            }
          }
        }

        describe("for string elements") {
          describe("without attributes") {
            it("should return the correct string column type") {
              val columnType = "CLOB"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach(
                t =>
                  withClue(s"Element $t should map to $columnType!")(
                    getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                      columnType
                    )
                )
              )
            }
          }

          describe(s"with ${AttributeNames.LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "CHAR"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(s"Element $t with ${AttributeNames.LENGTH} should map to $columnType!") {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.LENGTH, s"${scala.util.Random.nextInt(32) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType(${e.getAttribute(AttributeNames.LENGTH)})"
                  )
                }
              }
            }
          }

          describe(s"with ${AttributeNames.MAX_LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "VARCHAR"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(
                  s"Element $t with ${AttributeNames.MAX_LENGTH} should map to $columnType!"
                ) {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.MAX_LENGTH,
                                 s"${scala.util.Random.nextInt(250) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType(${e.getAttribute(AttributeNames.MAX_LENGTH)})"
                  )
                }
              }
            }
          }
        }
      }

      describe("for database firebird") {
        val databaseDriverName = "firebirdsql"
        val doc                = createTestDocumentBuilder().newDocument()

        describe("for binary elements") {
          it("should return the correct binary column type") {
            val columnType = "BLOB"
            val types      = List(ElementNames.BINARY, ElementNames.BINARY_64, ElementNames.BINARY_HEX)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for date and time elements") {
          it("should return the correct date column type") {
            val columnType = "DATE"
            val types      = List(ElementNames.DATE)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct datetime column type") {
            val columnType = "TIMESTAMP"
            val types      = List(ElementNames.DATETIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct time column type") {
            val columnType = "TIME"
            val types      = List(ElementNames.FORMATTED_TIME, ElementNames.TIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for numeric elements") {
          describe("without auto-increment") {
            it("should return the correct number column type") {
              val columnType = "DECIMAL"

              val fnum = doc.createElement(ElementNames.FORMATTED_NUMBER)
              withClue(
                s"Element ${ElementNames.FORMATTED_NUMBER} should map to DOUBLE PRECISION!"
              )(getDatabaseColumnType(fnum, databaseDriverName) should be("DOUBLE PRECISION"))

              val num0 = doc.createElement(ElementNames.NUMBER)
              withClue(s"Element ${ElementNames.NUMBER} should map to BIGINT!")(
                getDatabaseColumnType(num0, databaseDriverName) should be("BIGINT")
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.LENGTH, "10")
              num1.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)},${num1.getAttribute(AttributeNames.PRECISION)})"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.LENGTH, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.LENGTH)})"
                )
              }
            }
          }

          describe("with auto-increment") {
            it("should return the correct number column type") {
              val columnType = "DECIMAL"

              val fnum = doc.createElement(ElementNames.FORMATTED_NUMBER)
              fnum.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              withClue(
                s"Element ${ElementNames.FORMATTED_NUMBER} should map to DOUBLE PRECISION!"
              )(getDatabaseColumnType(fnum, databaseDriverName) should be("DOUBLE PRECISION"))

              val num0 = doc.createElement(ElementNames.NUMBER)
              num0.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              withClue(s"Element ${ElementNames.NUMBER} should map to BIGINT!")(
                getDatabaseColumnType(num0, databaseDriverName) should be("BIGINT")
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num1.setAttribute(AttributeNames.LENGTH, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)})"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num2.setAttribute(AttributeNames.LENGTH, "8")
              num2.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.LENGTH)},${num2.getAttribute(AttributeNames.PRECISION)})"
                )
              }
            }
          }
        }

        describe("for string elements") {
          describe("without attributes") {
            it("should return the correct string column type") {
              val columnType = "BLOB SUB_TYPE TEXT"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach(
                t =>
                  withClue(s"Element $t should map to $columnType!")(
                    getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                      columnType
                    )
                )
              )
            }
          }

          describe(s"with ${AttributeNames.LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "CHAR"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(s"Element $t with ${AttributeNames.LENGTH} should map to $columnType!") {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.LENGTH, s"${scala.util.Random.nextInt(32) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType(${e.getAttribute(AttributeNames.LENGTH)})"
                  )
                }
              }
            }
          }

          describe(s"with ${AttributeNames.MAX_LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "VARCHAR"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(
                  s"Element $t with ${AttributeNames.MAX_LENGTH} should map to $columnType!"
                ) {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.MAX_LENGTH,
                                 s"${scala.util.Random.nextInt(250) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType(${e.getAttribute(AttributeNames.MAX_LENGTH)})"
                  )
                }
              }
            }
          }
        }
      }

      describe("for database h2") {
        val databaseDriverName = "h2"
        val doc                = createTestDocumentBuilder().newDocument()

        describe("for binary elements") {
          it("should return the correct binary column type") {
            val columnType = "BLOB"
            val types      = List(ElementNames.BINARY, ElementNames.BINARY_64, ElementNames.BINARY_HEX)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for date and time elements") {
          it("should return the correct date column type") {
            val columnType = "DATE"
            val types      = List(ElementNames.DATE)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct datetime column type") {
            val columnType = "TIMESTAMP"
            val types      = List(ElementNames.DATETIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct time column type") {
            val columnType = "TIME"
            val types      = List(ElementNames.FORMATTED_TIME, ElementNames.TIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for numeric elements") {
          describe("without auto-increment") {
            it("should return the correct number column type") {
              val columnType = "NUMBER"
              val types      = List(ElementNames.FORMATTED_NUMBER, ElementNames.NUMBER)

              types.foreach(
                t =>
                  withClue(s"Element $t should map to $columnType!")(
                    getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                      "DOUBLE"
                    )
                )
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.LENGTH, "10")
              num1.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)},${num1.getAttribute(AttributeNames.PRECISION)})"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.LENGTH, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.LENGTH)})"
                )
              }
            }
          }

          describe("with auto-increment") {
            it("should return the correct number column type") {
              val columnType = "NUMBER"
              val types      = List(ElementNames.FORMATTED_NUMBER, ElementNames.NUMBER)

              types.foreach(
                t =>
                  withClue(s"Element $t should map to DOUBLE!") {
                    val e = doc.createElement(t)
                    e.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
                    getDatabaseColumnType(e, databaseDriverName) should be(s"DOUBLE IDENTITY")
                }
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num1.setAttribute(AttributeNames.LENGTH, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)}) IDENTITY"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num2.setAttribute(AttributeNames.LENGTH, "8")
              num2.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.LENGTH)},${num2.getAttribute(AttributeNames.PRECISION)})"
                )
              }
            }
          }
        }

        describe("for string elements") {
          describe("without attributes") {
            it("should return the correct string column type") {
              val columnType = "NCLOB"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach(
                t =>
                  withClue(s"Element $t should map to $columnType!")(
                    getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                      columnType
                    )
                )
              )
            }
          }

          describe(s"with ${AttributeNames.LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "NCHAR"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(s"Element $t with ${AttributeNames.LENGTH} should map to $columnType!") {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.LENGTH, s"${scala.util.Random.nextInt(32) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType(${e.getAttribute(AttributeNames.LENGTH)})"
                  )
                }
              }
            }
          }

          describe(s"with ${AttributeNames.MAX_LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "NVARCHAR2"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(
                  s"Element $t with ${AttributeNames.MAX_LENGTH} should map to $columnType!"
                ) {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.MAX_LENGTH,
                                 s"${scala.util.Random.nextInt(250) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType(${e.getAttribute(AttributeNames.MAX_LENGTH)})"
                  )
                }
              }
            }
          }
        }
      }

      describe("for database sqlserver") {
        val databaseDriverName = "sqlserver"
        val doc                = createTestDocumentBuilder().newDocument()

        describe("for binary elements") {
          it("should return the correct binary column type") {
            val columnType = "VARBINARY"
            val types      = List(ElementNames.BINARY, ElementNames.BINARY_64, ElementNames.BINARY_HEX)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for date and time elements") {
          it("should return the correct date column type") {
            val columnType = "DATE"
            val types      = List(ElementNames.DATE)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct datetime column type") {
            val columnType = "DATETIME2"
            val types      = List(ElementNames.DATETIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct time column type") {
            val columnType = "TIME"
            val types      = List(ElementNames.FORMATTED_TIME, ElementNames.TIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for numeric elements") {
          describe("without auto-increment") {
            it("should return the correct number column type") {
              val columnType = "DECIMAL"

              val fnum0 = doc.createElement(ElementNames.FORMATTED_NUMBER)
              withClue(s"${ElementNames.FORMATTED_NUMBER} should map to FLOAT") {
                getDatabaseColumnType(fnum0, databaseDriverName) should be("FLOAT")
              }

              val num0 = doc.createElement(ElementNames.NUMBER)
              withClue(s"${ElementNames.NUMBER} should map to BIGINT") {
                getDatabaseColumnType(num0, databaseDriverName) should be("BIGINT")
              }
              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.LENGTH, "10")
              num1.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)},${num1.getAttribute(AttributeNames.PRECISION)})"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.LENGTH, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.LENGTH)})"
                )
              }
            }
          }

          describe("with auto-increment") {
            it("should return the correct number column type") {
              val columnType = "DECIMAL"

              val fnum0 = doc.createElement(ElementNames.FORMATTED_NUMBER)
              fnum0.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              withClue(s"${ElementNames.FORMATTED_NUMBER} should map to FLOAT") {
                getDatabaseColumnType(fnum0, databaseDriverName) should be("FLOAT")
              }

              val num0 = doc.createElement(ElementNames.NUMBER)
              num0.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              withClue(s"${ElementNames.NUMBER} should map to BIGINT") {
                getDatabaseColumnType(num0, databaseDriverName) should be("BIGINT IDENTITY")
              }

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num1.setAttribute(AttributeNames.LENGTH, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)})"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num2.setAttribute(AttributeNames.LENGTH, "8")
              num2.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.LENGTH)},${num2.getAttribute(AttributeNames.PRECISION)})"
                )
              }
            }
          }
        }

        describe("for string elements") {
          describe("without attributes") {
            it("should return the correct string column type") {
              val columnType = "NTEXT"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach(
                t =>
                  withClue(s"Element $t should map to $columnType!")(
                    getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                      columnType
                    )
                )
              )
            }
          }

          describe(s"with ${AttributeNames.LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "NCHAR"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(s"Element $t with ${AttributeNames.LENGTH} should map to $columnType!") {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.LENGTH, s"${scala.util.Random.nextInt(32) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType(${e.getAttribute(AttributeNames.LENGTH)})"
                  )
                }
              }
            }
          }

          describe(s"with ${AttributeNames.MAX_LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "NVARCHAR"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(
                  s"Element $t with ${AttributeNames.MAX_LENGTH} should map to $columnType!"
                ) {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.MAX_LENGTH,
                                 s"${scala.util.Random.nextInt(250) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType(${e.getAttribute(AttributeNames.MAX_LENGTH)})"
                  )
                }
              }
            }
          }
        }
      }

      describe("for database mysql") {
        val databaseDriverName = "mysql"
        val doc                = createTestDocumentBuilder().newDocument()

        describe("for binary elements") {
          it("should return the correct binary column type") {
            val columnType = "LONGBLOB"
            val types      = List(ElementNames.BINARY, ElementNames.BINARY_64, ElementNames.BINARY_HEX)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for date and time elements") {
          it("should return the correct date column type") {
            val columnType = "DATE"
            val types      = List(ElementNames.DATE)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct datetime column type") {
            val columnType = "DATETIME"
            val types      = List(ElementNames.DATETIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct time column type") {
            val columnType = "TIME"
            val types      = List(ElementNames.FORMATTED_TIME, ElementNames.TIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for numeric elements") {
          describe("without auto-increment") {
            it("should return the correct number column type") {
              val columnType = "NUMERIC"

              val fnum = doc.createElement(ElementNames.FORMATTED_NUMBER)
              fnum.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              withClue(s"Element ${ElementNames.FORMATTED_NUMBER} should map to DOUBLE!")(
                getDatabaseColumnType(fnum, databaseDriverName) should be("DOUBLE")
              )

              val num0 = doc.createElement(ElementNames.NUMBER)
              withClue(s"Element ${ElementNames.NUMBER} should map to BIGINT!")(
                getDatabaseColumnType(num0, databaseDriverName) should be("BIGINT")
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.LENGTH, "10")
              num1.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)},${num1.getAttribute(AttributeNames.PRECISION)})"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.LENGTH, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.LENGTH)})"
                )
              }
            }
          }

          describe("with auto-increment") {
            it("should return the correct number column type") {
              val columnType = "NUMERIC"

              val fnum = doc.createElement(ElementNames.FORMATTED_NUMBER)
              fnum.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              withClue(s"Element ${ElementNames.FORMATTED_NUMBER} should map to DOUBLE!")(
                getDatabaseColumnType(fnum, databaseDriverName) should be("DOUBLE")
              )

              val num0 = doc.createElement(ElementNames.NUMBER)
              num0.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              withClue(s"Element ${ElementNames.NUMBER} should map to BIGINT!")(
                getDatabaseColumnType(num0, databaseDriverName) should be("BIGINT AUTO_INCREMENT")
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num1.setAttribute(AttributeNames.LENGTH, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)})"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num2.setAttribute(AttributeNames.LENGTH, "8")
              num2.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.LENGTH)},${num2.getAttribute(AttributeNames.PRECISION)})"
                )
              }
            }
          }
        }

        describe("for string elements") {
          describe("without attributes") {
            it("should return the correct string column type") {
              val columnType = "LONGTEXT CHARACTER SET utf8"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach(
                t =>
                  withClue(s"Element $t should map to $columnType!")(
                    getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                      columnType
                    )
                )
              )
            }
          }

          describe(s"with ${AttributeNames.LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "CHAR"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(s"Element $t with ${AttributeNames.LENGTH} should map to $columnType!") {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.LENGTH, s"${scala.util.Random.nextInt(32) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType(${e.getAttribute(AttributeNames.LENGTH)}) CHARACTER SET utf8"
                  )
                }
              }
            }
          }

          describe(s"with ${AttributeNames.MAX_LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "VARCHAR"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(
                  s"Element $t with ${AttributeNames.MAX_LENGTH} should map to $columnType!"
                ) {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.MAX_LENGTH,
                                 s"${scala.util.Random.nextInt(250) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType(${e.getAttribute(AttributeNames.MAX_LENGTH)}) CHARACTER SET utf8"
                  )
                }
              }
            }
          }
        }
      }

      describe("for database oracle") {
        val databaseDriverName = "oracle"
        val doc                = createTestDocumentBuilder().newDocument()

        describe("for binary elements") {
          it("should return the correct binary column type") {
            val columnType = "BLOB"
            val types      = List(ElementNames.BINARY, ElementNames.BINARY_64, ElementNames.BINARY_HEX)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for date and time elements") {
          it("should return the correct date column type") {
            val columnType = "DATE"
            val types      = List(ElementNames.DATE)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct datetime column type") {
            val columnType = "TIMESTAMP WITH TIME ZONE"
            val types      = List(ElementNames.DATETIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct time column type") {
            val columnType = "TIMESTAMP"
            val types      = List(ElementNames.TIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct formatted time column type") {
            val columnType = "NVARCHAR2(2000)"
            val types      = List(ElementNames.FORMATTED_TIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for numeric elements") {
          describe("without auto-increment") {
            it("should return the correct number column type") {
              val columnType = "NUMBER"
              val types      = List(ElementNames.FORMATTED_NUMBER, ElementNames.NUMBER)

              types.foreach(
                t =>
                  withClue(s"Element $t should map to $columnType!")(
                    getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                      columnType
                    )
                )
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.LENGTH, "10")
              num1.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)},${num1.getAttribute(AttributeNames.PRECISION)})"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.LENGTH, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.LENGTH)})"
                )
              }
            }
          }

          describe("with auto-increment") {
            it("should return the correct number column type") {
              val columnType = "NUMBER"
              val types      = List(ElementNames.FORMATTED_NUMBER, ElementNames.NUMBER)

              types.foreach(
                t =>
                  withClue(s"Element $t should map to $columnType!") {
                    val e = doc.createElement(t)
                    e.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
                    getDatabaseColumnType(e, databaseDriverName) should be(
                      s"$columnType GENERATED BY DEFAULT AS IDENTITY"
                    )
                }
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num1.setAttribute(AttributeNames.LENGTH, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)}) GENERATED BY DEFAULT AS IDENTITY"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num2.setAttribute(AttributeNames.LENGTH, "8")
              num2.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.LENGTH)},${num2.getAttribute(AttributeNames.PRECISION)})"
                )
              }
            }
          }
        }

        describe("for string elements") {
          describe("without attributes") {
            it("should return the correct string column type") {
              val columnType = "NCLOB"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach(
                t =>
                  withClue(s"Element $t should map to $columnType!")(
                    getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                      columnType
                    )
                )
              )
            }
          }

          describe(s"with ${AttributeNames.LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "NCHAR"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(s"Element $t with ${AttributeNames.LENGTH} should map to $columnType!") {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.LENGTH, s"${scala.util.Random.nextInt(32) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType(${e.getAttribute(AttributeNames.LENGTH)})"
                  )
                }
              }
            }
          }

          describe(s"with ${AttributeNames.MAX_LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "NVARCHAR2"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(
                  s"Element $t with ${AttributeNames.MAX_LENGTH} should map to $columnType!"
                ) {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.MAX_LENGTH,
                                 s"${scala.util.Random.nextInt(250) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType(${e.getAttribute(AttributeNames.MAX_LENGTH)})"
                  )
                }
              }
            }
          }
        }
      }

      describe("for database postgresql") {
        val databaseDriverName = "postgresql"
        val doc                = createTestDocumentBuilder().newDocument()

        describe("for binary elements") {
          it("should return the correct binary column type") {
            val columnType = "BYTEA"
            val types      = List(ElementNames.BINARY, ElementNames.BINARY_64, ElementNames.BINARY_HEX)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for date and time elements") {
          it("should return the correct date column type") {
            val columnType = "DATE"
            val types      = List(ElementNames.DATE)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct datetime column type") {
            val columnType = "TIMESTAMPTZ"
            val types      = List(ElementNames.DATETIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct time column type") {
            val columnType = "TIMETZ"
            val types      = List(ElementNames.TIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct formatted time column type") {
            val columnType = "TIMESTAMPTZ"
            val types      = List(ElementNames.FORMATTED_TIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for numeric elements") {
          describe("without auto-increment") {
            it("should return the correct number column type") {
              val columnType = "DECIMAL"

              val num0 = doc.createElement(ElementNames.NUMBER)
              withClue(s"Element ${ElementNames.NUMBER} should map to BIGINT!")(
                getDatabaseColumnType(num0, databaseDriverName) should be("BIGINT")
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.LENGTH, "10")
              num1.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)},${num1.getAttribute(AttributeNames.PRECISION)})"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.PRECISION, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.PRECISION)})"
                )
              }

              val num3 = doc.createElement(ElementNames.NUMBER)
              num3.setAttribute(AttributeNames.PRECISION, "0")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.PRECISION} set to 0 should map correctly."
              ) {
                getDatabaseColumnType(num3, databaseDriverName) should be("BIGINT")
              }

              val fcolumnType = "DOUBLE PRECISION"
              val fnum0       = doc.createElement(ElementNames.FORMATTED_NUMBER)
              withClue(s"Element ${ElementNames.FORMATTED_NUMBER} should map to $fcolumnType!")(
                getDatabaseColumnType(fnum0, databaseDriverName) should be(fcolumnType)
              )
            }
          }

          describe("with auto-increment") {
            it("should return the correct number column type") {
              val columnType = "DECIMAL"

              val num0 = doc.createElement(ElementNames.NUMBER)
              num0.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              withClue(s"Element ${ElementNames.NUMBER} should map to BIGSERIAL!")(
                getDatabaseColumnType(num0, databaseDriverName) should be("BIGSERIAL")
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num1.setAttribute(AttributeNames.LENGTH, "10")
              num1.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType(${num1.getAttribute(AttributeNames.LENGTH)},${num1.getAttribute(AttributeNames.PRECISION)})"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num2.setAttribute(AttributeNames.PRECISION, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType(${num2.getAttribute(AttributeNames.PRECISION)})"
                )
              }

              val num3 = doc.createElement(ElementNames.NUMBER)
              num3.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num3.setAttribute(AttributeNames.PRECISION, "0")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.PRECISION} set to 0 should map correctly."
              ) {
                getDatabaseColumnType(num3, databaseDriverName) should be("BIGSERIAL")
              }

              val fcolumnType = "DOUBLE PRECISION"
              val fnum0       = doc.createElement(ElementNames.FORMATTED_NUMBER)
              fnum0.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              withClue(s"Element ${ElementNames.FORMATTED_NUMBER} should map to $fcolumnType!")(
                getDatabaseColumnType(fnum0, databaseDriverName) should be(fcolumnType)
              )
            }
          }
        }

        describe("for string elements") {
          describe("without attributes") {
            it("should return the correct string column type") {
              val columnType = "TEXT"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach(
                t =>
                  withClue(s"Element $t should map to $columnType!")(
                    getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                      columnType
                    )
                )
              )
            }
          }

          describe(s"with ${AttributeNames.LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "CHARACTER"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(s"Element $t with ${AttributeNames.LENGTH} should map to $columnType!") {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.LENGTH, s"${scala.util.Random.nextInt(32) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType (${e.getAttribute(AttributeNames.LENGTH)})"
                  )
                }
              }
            }
          }

          describe(s"with ${AttributeNames.MAX_LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "CHARACTER VARYING"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(
                  s"Element $t with ${AttributeNames.MAX_LENGTH} should map to $columnType!"
                ) {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.MAX_LENGTH,
                                 s"${scala.util.Random.nextInt(250) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(
                    s"$columnType (${e.getAttribute(AttributeNames.MAX_LENGTH)})"
                  )
                }
              }
            }
          }
        }
      }

      describe("for database sqlite") {
        val databaseDriverName = "sqlite"
        val doc                = createTestDocumentBuilder().newDocument()

        describe("for binary elements") {
          it("should return the correct binary column type") {
            val columnType = "BLOB"
            val types      = List(ElementNames.BINARY, ElementNames.BINARY_64, ElementNames.BINARY_HEX)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for date and time elements") {
          it("should return the correct date column type") {
            val columnType = "TEXT"
            val types      = List(ElementNames.DATE)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct datetime column type") {
            val columnType = "TEXT"
            val types      = List(ElementNames.DATETIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }

          it("should return the correct time column type") {
            val columnType = "TEXT"
            val types      = List(ElementNames.FORMATTED_TIME, ElementNames.TIME)

            types.foreach(
              t =>
                withClue(s"Element $t should map to $columnType!")(
                  getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                    columnType
                  )
              )
            )
          }
        }

        describe("for numeric elements") {
          describe("without auto-increment") {
            it("should return the correct number column type") {
              val columnType = "INTEGER"

              val num0 = doc.createElement(ElementNames.NUMBER)
              withClue(s"Element ${ElementNames.NUMBER} should map to $columnType!")(
                getDatabaseColumnType(num0, databaseDriverName) should be(columnType)
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.LENGTH, "10")
              num1.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(columnType)
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.PRECISION, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(columnType)
              }

              val fcolumnType = "REAL"
              val fnum0       = doc.createElement(ElementNames.FORMATTED_NUMBER)
              withClue(s"Element ${ElementNames.FORMATTED_NUMBER} should map to $fcolumnType!")(
                getDatabaseColumnType(fnum0, databaseDriverName) should be(fcolumnType)
              )
            }
          }

          describe("with auto-increment") {
            it("should return the correct number column type") {
              val columnType = "INTEGER"

              val num0 = doc.createElement(ElementNames.NUMBER)
              num0.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              withClue(s"Element ${ElementNames.NUMBER} should map to $columnType!")(
                getDatabaseColumnType(num0, databaseDriverName) should be(
                  s"$columnType PRIMARY KEY AUTOINCREMENT"
                )
              )

              val num1 = doc.createElement(ElementNames.NUMBER)
              num1.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num1.setAttribute(AttributeNames.LENGTH, "10")
              num1.setAttribute(AttributeNames.PRECISION, "4")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.LENGTH} and ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num1, databaseDriverName) should be(
                  s"$columnType PRIMARY KEY AUTOINCREMENT"
                )
              }

              val num2 = doc.createElement(ElementNames.NUMBER)
              num2.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              num2.setAttribute(AttributeNames.PRECISION, "8")
              withClue(
                s"Element ${ElementNames.NUMBER} with ${AttributeNames.PRECISION} should map correctly."
              ) {
                getDatabaseColumnType(num2, databaseDriverName) should be(
                  s"$columnType PRIMARY KEY AUTOINCREMENT"
                )
              }

              val fcolumnType = "REAL"
              val fnum0       = doc.createElement(ElementNames.FORMATTED_NUMBER)
              fnum0.setAttribute(AttributeNames.DB_AUTO_INCREMENT, "true")
              withClue(s"Element ${ElementNames.FORMATTED_NUMBER} should map to $fcolumnType!")(
                getDatabaseColumnType(fnum0, databaseDriverName) should be(fcolumnType)
              )
            }
          }
        }

        describe("for string elements") {
          describe("without attributes") {
            it("should return the correct string column type") {
              val columnType = "TEXT"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach(
                t =>
                  withClue(s"Element $t should map to $columnType!")(
                    getDatabaseColumnType(doc.createElement(t), databaseDriverName) should be(
                      columnType
                    )
                )
              )
            }
          }

          describe(s"with ${AttributeNames.LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "TEXT"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(s"Element $t with ${AttributeNames.LENGTH} should map to $columnType!") {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.LENGTH, s"${scala.util.Random.nextInt(32) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(columnType)
                }
              }
            }
          }

          describe(s"with ${AttributeNames.MAX_LENGTH} attribute") {
            it("should return the correct string column type") {
              val columnType = "TEXT"
              val types      = List(ElementNames.FORMATTED_STRING, ElementNames.STRING)

              types.foreach { t =>
                withClue(
                  s"Element $t with ${AttributeNames.MAX_LENGTH} should map to $columnType!"
                ) {
                  val e = doc.createElement(t)
                  e.setAttribute(AttributeNames.MAX_LENGTH,
                                 s"${scala.util.Random.nextInt(250) + 1}")
                  getDatabaseColumnType(e, databaseDriverName) should be(columnType)
                }
              }
            }
          }
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
