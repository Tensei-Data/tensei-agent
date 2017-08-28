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

package databases.mysql

import java.net.URI

import akka.testkit.{ TestActorRef, TestFSMRef }
import com.wegtam.scalatest.tags.{ DbTest, DbTestMysql }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.{ ActorSpec, DummyActor, TenseiAgent }
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.duration._

class NormalisationTest extends ActorSpec with BeforeAndAfterEach {

  val databaseHost = testConfig.getString("mysql.host")
  val databasePort = testConfig.getInt("mysql.port")
  val databaseName = testConfig.getString("mysql.target-db.name")
  val databaseUser = testConfig.getString("mysql.target-db.user")
  val databasePass = testConfig.getString("mysql.target-db.pass")

  override protected def beforeEach(): Unit = {
    // The database connection.
    val connection = java.sql.DriverManager
      .getConnection(s"jdbc:mysql://$databaseHost:$databasePort/", databaseUser, databasePass)
    val s = connection.createStatement()
    s.execute(s"DROP DATABASE IF EXISTS $databaseName")
    s.execute(s"CREATE DATABASE $databaseName")
    s.close()
    connection.close()
    createSourceData()
    createTargetStructure()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    // The database connection.
    val connection = java.sql.DriverManager
      .getConnection(s"jdbc:mysql://$databaseHost:$databasePort/", databaseUser, databasePass)
    val s = connection.createStatement()
    s.execute(s"DROP DATABASE IF EXISTS $databaseName")
    s.close()
    connection.close()
    super.afterEach()
  }

  private def createSourceData(): Unit = {
    val c = java.sql.DriverManager.getConnection(
      s"jdbc:mysql://$databaseHost:$databasePort/$databaseName",
      databaseUser,
      databasePass
    )
    val s = c.createStatement()
    s.execute("""
        |CREATE TABLE employees (
        |  id BIGINT PRIMARY KEY,
        |  firstname VARCHAR(254),
        |  lastname VARCHAR(254),
        |  birthday DATE,
        |  century VARCHAR(16)
        |)
      """.stripMargin)
    s.execute("""
        |CREATE TABLE salary (
        |  employee_id BIGINT REFERENCES EMPLOYEES (id),
        |  amount DECIMAL(10,2)
        |)
      """.stripMargin)
    s.execute(
      """INSERT INTO employees (id, firstname, lastname, birthday, century) VALUES(123, 'Albert', 'Einstein', '1879-03-14', '19th')"""
    )
    s.execute("""INSERT INTO salary (employee_id, amount) VALUES(123, 3.14)""")
    s.execute(
      """INSERT INTO employees (id, firstname, lastname, birthday, century) VALUES(456, 'Bernhard', 'Riemann', '1826-09-17', '19th')"""
    )
    s.execute("""INSERT INTO salary (employee_id, amount) VALUES(456, 6.28)""")
    s.execute(
      """INSERT INTO employees (id, firstname, lastname, birthday, century) VALUES(789, 'Johann Carl Friedrich', 'Gauß', '1777-04-30', '18th')"""
    )
    s.execute("""INSERT INTO salary (employee_id, amount) VALUES(789, 12.56)""")
    s.execute(
      """INSERT INTO employees (id, firstname, lastname, birthday, century) VALUES(5, 'Johann Benedict', 'Listing', '1808-07-25', '19th')"""
    )
    s.execute("""INSERT INTO salary (employee_id, amount) VALUES(5, 25.12)""")
    s.execute(
      """INSERT INTO employees (id, firstname, lastname, birthday, century) VALUES(8, 'Gottfried Wilhelm', 'Leibnitz', '1646-07-01', '17th')"""
    )
    s.execute("""INSERT INTO salary (employee_id, amount) VALUES(8, 50.24)""")
    s.close()
  }

  private def createTargetStructure(): Unit = {
    val c = java.sql.DriverManager.getConnection(
      s"jdbc:mysql://$databaseHost:$databasePort/$databaseName",
      databaseUser,
      databasePass
    )
    val s = c.createStatement()
    s.execute("""
        |CREATE TABLE t_centuries (
        |  id BIGINT AUTO_INCREMENT PRIMARY KEY,
        |  name VARCHAR(16),
        |  UNIQUE(name)
        |)
      """.stripMargin)
    s.execute("""
        |CREATE TABLE t_employees (
        |  id BIGINT AUTO_INCREMENT PRIMARY KEY,
        |  firstname VARCHAR(254),
        |  lastname VARCHAR(254),
        |  birthday DATE,
        |  century_id BIGINT REFERENCES T_CENTURIES (id)
        |)
      """.stripMargin)
    s.execute("""
        |CREATE TABLE t_salary (
        |  employee_id BIGINT REFERENCES T_EMPLOYEES (id),
        |  amount DECIMAL(10,2)
        |)
      """.stripMargin)
    s.close()
  }

  describe("Normalisation by creating foreign keys") {
    describe("using mysql") {
      describe("using one to one mappings") {
        describe("with single mappings") {
          it("should replace changed auto-increment values", DbTest, DbTestMysql) {
            val connection = java.sql.DriverManager.getConnection(
              s"jdbc:mysql://$databaseHost:$databasePort/$databaseName",
              databaseUser,
              databasePass
            )

            val sourceDfasdl = new DFASDL(
              id = "SRC",
              content = scala.io.Source
                .fromInputStream(
                  getClass
                    .getResourceAsStream("/databases/generic/Normalisation/source-dfasdl.xml")
                )
                .mkString
            )
            val targetDfasdl = new DFASDL(
              id = "DST",
              content = scala.io.Source
                .fromInputStream(
                  getClass
                    .getResourceAsStream("/databases/generic/Normalisation/target-dfasdl.xml")
                )
                .mkString
            )

            val cookbook: Cookbook = Cookbook(
              id = "COOKBOOK",
              sources = List(sourceDfasdl),
              target = Option(targetDfasdl),
              recipes = List(
                Recipe(
                  id = "ExtractCenturies",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_century")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "centuries_row_id")
                      ),
                      transformations = List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String]))
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_century")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "centuries_row_name")
                      )
                    )
                  )
                ),
                Recipe(
                  id = "CopyEmployees",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_id")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_id")
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_firstname")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_firstname")
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_lastname")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_lastname")
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_birthday")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_birthday")
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_century")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_century_id")
                      )
                    )
                  )
                ),
                Recipe(
                  id = "CopySalaries",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "salary_row_employee_id")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "salary_row_employee_id")
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "salary_row_amount")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "salary_row_amount")
                      )
                    )
                  )
                )
              )
            )

            val source = ConnectionInformation(
              uri = new URI(connection.getMetaData.getURL),
              dfasdlRef =
                Option(DFASDLReference(cookbookId = cookbook.id, dfasdlId = sourceDfasdl.id)),
              username = Option(databaseUser),
              password = Option(databasePass)
            )
            val target = ConnectionInformation(
              uri = new URI(connection.getMetaData.getURL),
              dfasdlRef =
                Option(DFASDLReference(cookbookId = cookbook.id, dfasdlId = targetDfasdl.id)),
              username = Option(databaseUser),
              password = Option(databasePass)
            )

            val dummy  = TestActorRef(DummyActor.props())
            val client = system.actorSelection(dummy.path)
            val agent  = TestFSMRef(new TenseiAgent("TEST-AGENT", client))

            val msg = AgentStartTransformationMessage(
              sources = List(source),
              target = target,
              cookbook = cookbook,
              uniqueIdentifier = Option("NORMALISATION-TEST-OneToOne")
            )

            agent ! msg

            expectMsgType[GlobalMessages.TransformationStarted](FiniteDuration(5, SECONDS))
            expectMsgType[GlobalMessages.TransformationCompleted](FiniteDuration(7, SECONDS))

            val s = connection.createStatement()
            withClue("Written data should be correct!") {
              val expectedData = Map(
                "Einstein" -> List(new java.math.BigDecimal("3.14"), "19th"),
                "Riemann"  -> List(new java.math.BigDecimal("6.28"), "19th"),
                "Gauß"     -> List(new java.math.BigDecimal("12.56"), "18th"),
                "Listing"  -> List(new java.math.BigDecimal("25.12"), "19th"),
                "Leibnitz" -> List(new java.math.BigDecimal("50.24"), "17th")
              )
              val r = s.executeQuery(
                "SELECT t_employees.id AS id, t_employees.lastname AS name, t_salary.amount AS amount, t_centuries.name AS century FROM t_employees JOIN t_salary ON t_employees.id = t_salary.employee_id JOIN t_centuries ON t_employees.century_id = t_centuries.id"
              )
              if (r.next()) {
                r.getBigDecimal("amount") should be(expectedData(r.getString("name")).head)
                r.getString("century") should be(expectedData(r.getString("name"))(1))

                while (r.next()) {
                  r.getBigDecimal("amount") should be(expectedData(r.getString("name")).head)
                  r.getString("century") should be(expectedData(r.getString("name"))(1))
                }
              } else
                fail("No results found in database!")
            }
            s.close()
          }

          it("should consider existing unique values", DbTest, DbTestMysql) {
            val connection = java.sql.DriverManager.getConnection(
              s"jdbc:mysql://$databaseHost:$databasePort/$databaseName",
              databaseUser,
              databasePass
            )
            connection.createStatement().execute("INSERT INTO t_centuries (name) VALUES('17th')")

            val sourceDfasdl = new DFASDL(
              id = "SRC",
              content = scala.io.Source
                .fromInputStream(
                  getClass
                    .getResourceAsStream("/databases/generic/Normalisation/source-dfasdl.xml")
                )
                .mkString
            )
            val targetDfasdl = new DFASDL(
              id = "DST",
              content = scala.io.Source
                .fromInputStream(
                  getClass
                    .getResourceAsStream("/databases/generic/Normalisation/target-dfasdl.xml")
                )
                .mkString
            )

            val cookbook: Cookbook = Cookbook(
              id = "COOKBOOK",
              sources = List(sourceDfasdl),
              target = Option(targetDfasdl),
              recipes = List(
                Recipe(
                  id = "ExtractCenturies",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_century")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "centuries_row_id")
                      ),
                      transformations = List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String]))
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_century")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "centuries_row_name")
                      )
                    )
                  )
                ),
                Recipe(
                  id = "CopyEmployees",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_id")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_id")
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_firstname")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_firstname")
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_lastname")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_lastname")
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_birthday")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_birthday")
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_century")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_century_id")
                      )
                    )
                  )
                ),
                Recipe(
                  id = "CopySalaries",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "salary_row_employee_id")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "salary_row_employee_id")
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "salary_row_amount")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "salary_row_amount")
                      )
                    )
                  )
                )
              )
            )

            val source = ConnectionInformation(
              uri = new URI(connection.getMetaData.getURL),
              dfasdlRef =
                Option(DFASDLReference(cookbookId = cookbook.id, dfasdlId = sourceDfasdl.id)),
              username = Option(databaseUser),
              password = Option(databasePass)
            )
            val target = ConnectionInformation(
              uri = new URI(connection.getMetaData.getURL),
              dfasdlRef =
                Option(DFASDLReference(cookbookId = cookbook.id, dfasdlId = targetDfasdl.id)),
              username = Option(databaseUser),
              password = Option(databasePass)
            )

            val dummy  = TestActorRef(DummyActor.props())
            val client = system.actorSelection(dummy.path)
            val agent  = TestFSMRef(new TenseiAgent("TEST-AGENT", client))

            val msg = AgentStartTransformationMessage(
              sources = List(source),
              target = target,
              cookbook = cookbook,
              uniqueIdentifier = Option("NORMALISATION-TEST-OneToOne")
            )

            agent ! msg

            expectMsgType[GlobalMessages.TransformationStarted](FiniteDuration(5, SECONDS))
            expectMsgType[GlobalMessages.TransformationCompleted](FiniteDuration(7, SECONDS))

            val s = connection.createStatement()
            withClue("Written data should be correct!") {
              val expectedData = Map(
                "Einstein" -> List(new java.math.BigDecimal("3.14"), "19th"),
                "Riemann"  -> List(new java.math.BigDecimal("6.28"), "19th"),
                "Gauß"     -> List(new java.math.BigDecimal("12.56"), "18th"),
                "Listing"  -> List(new java.math.BigDecimal("25.12"), "19th"),
                "Leibnitz" -> List(new java.math.BigDecimal("50.24"), "17th")
              )
              val r = s.executeQuery(
                "SELECT t_employees.id AS id, t_employees.lastname AS name, t_salary.amount AS amount, t_centuries.name AS century FROM t_employees JOIN t_salary ON t_employees.id = t_salary.employee_id JOIN t_centuries ON t_employees.century_id = t_centuries.id"
              )
              if (r.next()) {
                r.getBigDecimal("amount") should be(expectedData(r.getString("name")).head)
                r.getString("century") should be(expectedData(r.getString("name"))(1))

                while (r.next()) {
                  r.getBigDecimal("amount") should be(expectedData(r.getString("name")).head)
                  r.getString("century") should be(expectedData(r.getString("name"))(1))
                }
              } else
                fail("No results found in database!")
            }
            s.close()
          }
        }

        describe("with bulk mappings") {
          it("should replace changed auto-increment values", DbTest, DbTestMysql) {
            val connection = java.sql.DriverManager.getConnection(
              s"jdbc:mysql://$databaseHost:$databasePort/$databaseName",
              databaseUser,
              databasePass
            )

            val sourceDfasdl = new DFASDL(
              id = "SRC",
              content = scala.io.Source
                .fromInputStream(
                  getClass
                    .getResourceAsStream("/databases/generic/Normalisation/source-dfasdl.xml")
                )
                .mkString
            )
            val targetDfasdl = new DFASDL(
              id = "DST",
              content = scala.io.Source
                .fromInputStream(
                  getClass
                    .getResourceAsStream("/databases/generic/Normalisation/target-dfasdl.xml")
                )
                .mkString
            )

            val cookbook: Cookbook = Cookbook(
              id = "COOKBOOK",
              sources = List(sourceDfasdl),
              target = Option(targetDfasdl),
              recipes = List(
                Recipe(
                  id = "ExtractCenturies",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_century")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "centuries_row_id")
                      ),
                      transformations = List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String]))
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_century")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "centuries_row_name")
                      )
                    )
                  )
                ),
                Recipe(
                  id = "CopyEmployees",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_id"),
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_firstname"),
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_lastname"),
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_birthday"),
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_century")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_id"),
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_firstname"),
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_lastname"),
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_birthday"),
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_century_id")
                      )
                    )
                  )
                ),
                Recipe(
                  id = "CopySalaries",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "salary_row_employee_id"),
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "salary_row_amount")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "salary_row_employee_id"),
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "salary_row_amount")
                      )
                    )
                  )
                )
              )
            )

            val source = ConnectionInformation(
              uri = new URI(connection.getMetaData.getURL),
              dfasdlRef =
                Option(DFASDLReference(cookbookId = cookbook.id, dfasdlId = sourceDfasdl.id)),
              username = Option(databaseUser),
              password = Option(databasePass)
            )
            val target = ConnectionInformation(
              uri = new URI(connection.getMetaData.getURL),
              dfasdlRef =
                Option(DFASDLReference(cookbookId = cookbook.id, dfasdlId = targetDfasdl.id)),
              username = Option(databaseUser),
              password = Option(databasePass)
            )

            val dummy  = TestActorRef(DummyActor.props())
            val client = system.actorSelection(dummy.path)
            val agent  = TestFSMRef(new TenseiAgent("TEST-AGENT", client))

            val msg = AgentStartTransformationMessage(
              sources = List(source),
              target = target,
              cookbook = cookbook,
              uniqueIdentifier = Option("NORMALISATION-KEY-TEST-OneToOne")
            )

            agent ! msg

            expectMsgType[GlobalMessages.TransformationStarted](FiniteDuration(5, SECONDS))
            expectMsgType[GlobalMessages.TransformationCompleted](FiniteDuration(7, SECONDS))

            val s = connection.createStatement()
            withClue("Written data should be correct!") {
              val expectedData = Map(
                "Einstein" -> List(new java.math.BigDecimal("3.14"), "19th"),
                "Riemann"  -> List(new java.math.BigDecimal("6.28"), "19th"),
                "Gauß"     -> List(new java.math.BigDecimal("12.56"), "18th"),
                "Listing"  -> List(new java.math.BigDecimal("25.12"), "19th"),
                "Leibnitz" -> List(new java.math.BigDecimal("50.24"), "17th")
              )
              val r = s.executeQuery(
                "SELECT t_employees.id AS id, t_employees.lastname AS name, t_salary.amount AS amount, t_centuries.name AS century FROM t_employees JOIN t_salary ON t_employees.id = t_salary.employee_id JOIN t_centuries ON t_employees.century_id = t_centuries.id"
              )
              if (r.next()) {
                r.getBigDecimal("amount") should be(expectedData(r.getString("name")).head)
                r.getString("century") should be(expectedData(r.getString("name"))(1))

                while (r.next()) {
                  r.getBigDecimal("amount") should be(expectedData(r.getString("name")).head)
                  r.getString("century") should be(expectedData(r.getString("name"))(1))
                }
              } else
                fail("No results found in database!")
            }
            s.close()
          }

          it("should consider existing unique values", DbTest, DbTestMysql) {
            val connection = java.sql.DriverManager.getConnection(
              s"jdbc:mysql://$databaseHost:$databasePort/$databaseName",
              databaseUser,
              databasePass
            )
            connection.createStatement().execute("INSERT INTO t_centuries (name) VALUES('17th')")

            val sourceDfasdl = new DFASDL(
              id = "SRC",
              content = scala.io.Source
                .fromInputStream(
                  getClass
                    .getResourceAsStream("/databases/generic/Normalisation/source-dfasdl.xml")
                )
                .mkString
            )
            val targetDfasdl = new DFASDL(
              id = "DST",
              content = scala.io.Source
                .fromInputStream(
                  getClass
                    .getResourceAsStream("/databases/generic/Normalisation/target-dfasdl.xml")
                )
                .mkString
            )

            val cookbook: Cookbook = Cookbook(
              id = "COOKBOOK",
              sources = List(sourceDfasdl),
              target = Option(targetDfasdl),
              recipes = List(
                Recipe(
                  id = "ExtractCenturies",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_century")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "centuries_row_id")
                      ),
                      transformations = List(
                        TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                                  TransformerOptions(classOf[String],
                                                                     classOf[String]))
                      )
                    ),
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_century")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "centuries_row_name")
                      )
                    )
                  )
                ),
                Recipe(
                  id = "CopyEmployees",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_id"),
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_firstname"),
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_lastname"),
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_birthday"),
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "employees_row_century")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_id"),
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_firstname"),
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_lastname"),
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_birthday"),
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "employees_row_century_id")
                      )
                    )
                  )
                ),
                Recipe(
                  id = "CopySalaries",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      sources = List(
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "salary_row_employee_id"),
                        ElementReference(dfasdlId = sourceDfasdl.id,
                                         elementId = "salary_row_amount")
                      ),
                      targets = List(
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "salary_row_employee_id"),
                        ElementReference(dfasdlId = targetDfasdl.id,
                                         elementId = "salary_row_amount")
                      )
                    )
                  )
                )
              )
            )

            val source = ConnectionInformation(
              uri = new URI(connection.getMetaData.getURL),
              dfasdlRef =
                Option(DFASDLReference(cookbookId = cookbook.id, dfasdlId = sourceDfasdl.id)),
              username = Option(databaseUser),
              password = Option(databasePass)
            )
            val target = ConnectionInformation(
              uri = new URI(connection.getMetaData.getURL),
              dfasdlRef =
                Option(DFASDLReference(cookbookId = cookbook.id, dfasdlId = targetDfasdl.id)),
              username = Option(databaseUser),
              password = Option(databasePass)
            )

            val dummy  = TestActorRef(DummyActor.props())
            val client = system.actorSelection(dummy.path)
            val agent  = TestFSMRef(new TenseiAgent("TEST-AGENT", client))

            val msg = AgentStartTransformationMessage(
              sources = List(source),
              target = target,
              cookbook = cookbook,
              uniqueIdentifier = Option("NORMALISATION-KEY-TEST-OneToOne")
            )

            agent ! msg

            expectMsgType[GlobalMessages.TransformationStarted](FiniteDuration(5, SECONDS))
            expectMsgType[GlobalMessages.TransformationCompleted](FiniteDuration(7, SECONDS))

            val s = connection.createStatement()
            withClue("Written data should be correct!") {
              val expectedData = Map(
                "Einstein" -> List(new java.math.BigDecimal("3.14"), "19th"),
                "Riemann"  -> List(new java.math.BigDecimal("6.28"), "19th"),
                "Gauß"     -> List(new java.math.BigDecimal("12.56"), "18th"),
                "Listing"  -> List(new java.math.BigDecimal("25.12"), "19th"),
                "Leibnitz" -> List(new java.math.BigDecimal("50.24"), "17th")
              )
              val r = s.executeQuery(
                "SELECT t_employees.id AS id, t_employees.lastname AS name, t_salary.amount AS amount, t_centuries.name AS century FROM t_employees JOIN t_salary ON t_employees.id = t_salary.employee_id JOIN t_centuries ON t_employees.century_id = t_centuries.id"
              )
              if (r.next()) {
                r.getBigDecimal("amount") should be(expectedData(r.getString("name")).head)
                r.getString("century") should be(expectedData(r.getString("name"))(1))

                while (r.next()) {
                  r.getBigDecimal("amount") should be(expectedData(r.getString("name")).head)
                  r.getString("century") should be(expectedData(r.getString("name"))(1))
                }
              } else
                fail("No results found in database!")
            }
            s.close()
          }
        }
      }

      describe("using all to all mappings") {
        it("should replace changed auto-increment values", DbTest, DbTestMysql) {
          val connection = java.sql.DriverManager.getConnection(
            s"jdbc:mysql://$databaseHost:$databasePort/$databaseName",
            databaseUser,
            databasePass
          )

          val sourceDfasdl = new DFASDL(
            id = "SRC",
            content = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/databases/generic/Normalisation/source-dfasdl.xml")
              )
              .mkString
          )
          val targetDfasdl = new DFASDL(
            id = "DST",
            content = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/databases/generic/Normalisation/target-dfasdl.xml")
              )
              .mkString
          )

          val cookbook: Cookbook = Cookbook(
            id = "COOKBOOK",
            sources = List(sourceDfasdl),
            target = Option(targetDfasdl),
            recipes = List(
              Recipe(
                id = "ExtractCenturies",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "employees_row_century")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id, elementId = "centuries_row_id")
                    ),
                    transformations = List(
                      TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                                TransformerOptions(classOf[String],
                                                                   classOf[String]))
                    )
                  ),
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "employees_row_century")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id,
                                       elementId = "centuries_row_name")
                    )
                  )
                )
              ),
              Recipe(
                id = "CopyEmployees",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id, elementId = "employees_row_id")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id, elementId = "employees_row_id")
                    )
                  ),
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "employees_row_firstname")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id,
                                       elementId = "employees_row_firstname")
                    )
                  ),
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "employees_row_lastname")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id,
                                       elementId = "employees_row_lastname")
                    )
                  ),
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "employees_row_birthday")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id,
                                       elementId = "employees_row_birthday")
                    )
                  ),
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "employees_row_century")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id,
                                       elementId = "employees_row_century_id")
                    )
                  )
                )
              ),
              Recipe(
                id = "CopySalaries",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "salary_row_employee_id")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id,
                                       elementId = "salary_row_employee_id")
                    )
                  ),
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id, elementId = "salary_row_amount")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id, elementId = "salary_row_amount")
                    )
                  )
                )
              )
            )
          )

          val source = ConnectionInformation(
            uri = new URI(connection.getMetaData.getURL),
            dfasdlRef =
              Option(DFASDLReference(cookbookId = cookbook.id, dfasdlId = sourceDfasdl.id)),
            username = Option(databaseUser),
            password = Option(databasePass)
          )
          val target = ConnectionInformation(
            uri = new URI(connection.getMetaData.getURL),
            dfasdlRef =
              Option(DFASDLReference(cookbookId = cookbook.id, dfasdlId = targetDfasdl.id)),
            username = Option(databaseUser),
            password = Option(databasePass)
          )

          val dummy  = TestActorRef(DummyActor.props())
          val client = system.actorSelection(dummy.path)
          val agent  = TestFSMRef(new TenseiAgent("TEST-AGENT", client))

          val msg = AgentStartTransformationMessage(
            sources = List(source),
            target = target,
            cookbook = cookbook,
            uniqueIdentifier = Option("NORMALISATION-KEY-TEST-OneToOne")
          )

          agent ! msg

          expectMsgType[GlobalMessages.TransformationStarted](FiniteDuration(5, SECONDS))
          expectMsgType[GlobalMessages.TransformationCompleted](FiniteDuration(7, SECONDS))

          val s = connection.createStatement()
          withClue("Written data should be correct!") {
            val expectedData = Map(
              "Einstein" -> List(new java.math.BigDecimal("3.14"), "19th"),
              "Riemann"  -> List(new java.math.BigDecimal("6.28"), "19th"),
              "Gauß"     -> List(new java.math.BigDecimal("12.56"), "18th"),
              "Listing"  -> List(new java.math.BigDecimal("25.12"), "19th"),
              "Leibnitz" -> List(new java.math.BigDecimal("50.24"), "17th")
            )
            val r = s.executeQuery(
              "SELECT t_employees.id AS id, t_employees.lastname AS name, t_salary.amount AS amount, t_centuries.name AS century FROM t_employees JOIN t_salary ON t_employees.id = t_salary.employee_id JOIN t_centuries ON t_employees.century_id = t_centuries.id"
            )
            if (r.next()) {
              r.getBigDecimal("amount") should be(expectedData(r.getString("name")).head)
              r.getString("century") should be(expectedData(r.getString("name"))(1))

              while (r.next()) {
                r.getBigDecimal("amount") should be(expectedData(r.getString("name")).head)
                r.getString("century") should be(expectedData(r.getString("name"))(1))
              }
            } else
              fail("No results found in database!")
          }
          s.close()
        }

        it("should consider existing unique values", DbTest, DbTestMysql) {
          val connection = java.sql.DriverManager.getConnection(
            s"jdbc:mysql://$databaseHost:$databasePort/$databaseName",
            databaseUser,
            databasePass
          )
          connection.createStatement().execute("INSERT INTO t_centuries (name) VALUES('17th')")

          val sourceDfasdl = new DFASDL(
            id = "SRC",
            content = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/databases/generic/Normalisation/source-dfasdl.xml")
              )
              .mkString
          )
          val targetDfasdl = new DFASDL(
            id = "DST",
            content = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/databases/generic/Normalisation/target-dfasdl.xml")
              )
              .mkString
          )

          val cookbook: Cookbook = Cookbook(
            id = "COOKBOOK",
            sources = List(sourceDfasdl),
            target = Option(targetDfasdl),
            recipes = List(
              Recipe(
                id = "ExtractCenturies",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "employees_row_century")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id, elementId = "centuries_row_id")
                    ),
                    transformations = List(
                      TransformationDescription("com.wegtam.tensei.agent.transformers.Nullify",
                                                TransformerOptions(classOf[String],
                                                                   classOf[String]))
                    )
                  ),
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "employees_row_century")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id,
                                       elementId = "centuries_row_name")
                    )
                  )
                )
              ),
              Recipe(
                id = "CopyEmployees",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id, elementId = "employees_row_id")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id, elementId = "employees_row_id")
                    )
                  ),
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "employees_row_firstname")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id,
                                       elementId = "employees_row_firstname")
                    )
                  ),
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "employees_row_lastname")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id,
                                       elementId = "employees_row_lastname")
                    )
                  ),
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "employees_row_birthday")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id,
                                       elementId = "employees_row_birthday")
                    )
                  ),
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "employees_row_century")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id,
                                       elementId = "employees_row_century_id")
                    )
                  )
                )
              ),
              Recipe(
                id = "CopySalaries",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id,
                                       elementId = "salary_row_employee_id")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id,
                                       elementId = "salary_row_employee_id")
                    )
                  ),
                  MappingTransformation(
                    sources = List(
                      ElementReference(dfasdlId = sourceDfasdl.id, elementId = "salary_row_amount")
                    ),
                    targets = List(
                      ElementReference(dfasdlId = targetDfasdl.id, elementId = "salary_row_amount")
                    )
                  )
                )
              )
            )
          )

          val source = ConnectionInformation(
            uri = new URI(connection.getMetaData.getURL),
            dfasdlRef =
              Option(DFASDLReference(cookbookId = cookbook.id, dfasdlId = sourceDfasdl.id)),
            username = Option(databaseUser),
            password = Option(databasePass)
          )
          val target = ConnectionInformation(
            uri = new URI(connection.getMetaData.getURL),
            dfasdlRef =
              Option(DFASDLReference(cookbookId = cookbook.id, dfasdlId = targetDfasdl.id)),
            username = Option(databaseUser),
            password = Option(databasePass)
          )

          val dummy  = TestActorRef(DummyActor.props())
          val client = system.actorSelection(dummy.path)
          val agent  = TestFSMRef(new TenseiAgent("TEST-AGENT", client))

          val msg = AgentStartTransformationMessage(
            sources = List(source),
            target = target,
            cookbook = cookbook,
            uniqueIdentifier = Option("NORMALISATION-KEY-TEST-OneToOne")
          )

          agent ! msg

          expectMsgType[GlobalMessages.TransformationStarted](FiniteDuration(5, SECONDS))
          expectMsgType[GlobalMessages.TransformationCompleted](FiniteDuration(7, SECONDS))

          val s = connection.createStatement()
          withClue("Written data should be correct!") {
            val expectedData = Map(
              "Einstein" -> List(new java.math.BigDecimal("3.14"), "19th"),
              "Riemann"  -> List(new java.math.BigDecimal("6.28"), "19th"),
              "Gauß"     -> List(new java.math.BigDecimal("12.56"), "18th"),
              "Listing"  -> List(new java.math.BigDecimal("25.12"), "19th"),
              "Leibnitz" -> List(new java.math.BigDecimal("50.24"), "17th")
            )
            val r = s.executeQuery(
              "SELECT t_employees.id AS id, t_employees.lastname AS name, t_salary.amount AS amount, t_centuries.name AS century FROM t_employees JOIN t_salary ON t_employees.id = t_salary.employee_id JOIN t_centuries ON t_employees.century_id = t_centuries.id"
            )
            if (r.next()) {
              r.getBigDecimal("amount") should be(expectedData(r.getString("name")).head)
              r.getString("century") should be(expectedData(r.getString("name"))(1))

              while (r.next()) {
                r.getBigDecimal("amount") should be(expectedData(r.getString("name")).head)
                r.getString("century") should be(expectedData(r.getString("name"))(1))
              }
            } else
              fail("No results found in database!")
          }
          s.close()
        }
      }
    }
  }
}
