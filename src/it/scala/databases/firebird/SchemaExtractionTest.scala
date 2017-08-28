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

package databases.firebird

import akka.testkit.TestActorRef
import com.wegtam.scalatest.tags.{ DbTest, DbTestFirebird }
import com.wegtam.tensei.adt.{ ConnectionInformation, ExtractSchemaOptions, GlobalMessages }
import com.wegtam.tensei.agent.{ ActorSpec, SchemaExtractor }
import org.scalatest.BeforeAndAfterEach

import scalaz._

class SchemaExtractionTest extends ActorSpec with BeforeAndAfterEach {

  import scala.sys.process.Process

  val databaseHost = testConfig.getString("firebird.host")
  val databasePort = testConfig.getInt("firebird.port")
  val databaseName = testConfig.getString("firebird.target-db.name")
  val databaseUser = testConfig.getString("firebird.target-db.user")
  val databasePass = testConfig.getString("firebird.target-db.pass")
  val databasePath = testConfig.getString("firebird.target-db.path")

  override protected def beforeEach(): Unit = {
    // The database connection.
    val sqlScript =
      getClass.getResource("/databases/generic/SqlScripts/Firebird/Database_before_each.sql")
    Process(s"isql-fb -user $databaseUser -p $databasePass -i ${sqlScript.getPath}")
      .run()
      .exitValue() should be(0)
    Thread.sleep(3000)
    super.beforeEach()
  }

  /**
    * Remove the test database.
    */
  override protected def afterEach(): Unit = {
    val sqlScript =
      getClass.getResource("/databases/generic/SqlScripts/Firebird/Database_after_each.sql")
    Process(s"isql-fb -user $databaseUser -p $databasePass -i ${sqlScript.getPath}")
      .run()
      .exitValue() should be(0)
    Thread.sleep(3000)
    super.afterEach()
  }

  /**
    * Create the test tables within the database.
    *
    * @param connection A connection to the database.
    */
  private def createDatabaseTables(connection: java.sql.Connection): Unit = {
    val s = connection.createStatement()
    // INFO : id is auto_increment
    s.execute("""
        |CREATE TABLE salary_groups (
        |  id INT,
        |  name VARCHAR(64) NOT NULL,
        |  min_wage DECIMAL(8,2) NOT NULL,
        |  max_wage DECIMAL(8,2) NOT NULL,
        |  CONSTRAINT pk_salary_groups PRIMARY KEY (id),
        |  CONSTRAINT unq_salary_groups  UNIQUE (name)
        |)
      """.stripMargin)
    // INFO : id is auto_increment
    s.execute(
      """
        |CREATE TABLE employees (
        |  id BIGINT,
        |  firstname VARCHAR(64) NOT NULL,
        |  lastname VARCHAR(64) NOT NULL,
        |  birthday DATE NOT NULL,
        |  notes BLOB SUB_TYPE TEXT,
        |  salary_group INT,
        |  CONSTRAINT pk_employees PRIMARY KEY (id),
        |  CONSTRAINT fk_employees_to_salary_groups FOREIGN KEY (salary_group) REFERENCES salary_groups (id)
        |)
      """.stripMargin
    )
    s.execute(
      """
        |CREATE TABLE wages (
        |  employee_id BIGINT,
        |  salary_group INT,
        |  wage DECIMAL(6,2) NOT NULL,
        |  CONSTRAINT pk_wages PRIMARY KEY (employee_id, salary_group),
        |  CONSTRAINT fk_wages_to_employees FOREIGN KEY (employee_id) REFERENCES employees (id),
        |  CONSTRAINT fk_wages_to_salary_groups FOREIGN KEY (salary_group) REFERENCES salary_groups (id)
        |)
      """.stripMargin
    )
    // INFO : id is auto_increment
    s.execute(
      """
        |CREATE TABLE company_cars (
        |  id INT,
        |  license_plate VARCHAR(16) NOT NULL,
        |  employee_id BIGINT,
        |  seats SMALLINT,
        |  bought DATE,
        |  CONSTRAINT pk_company_cars PRIMARY KEY (id),
        |  CONSTRAINT unq_company_cars UNIQUE (license_plate),
        |  CONSTRAINT fk_company_cars_to_employees FOREIGN KEY (employee_id) REFERENCES employees (id)
        |)
      """.stripMargin
    )
    // INFO : id is auto_increment
    s.execute(
      """
        |CREATE TABLE parking_slots (
        |  id INT,
        |  employee_id BIGINT,
        |  license_plate VARCHAR(16),
        |  CONSTRAINT pk_parking_slots PRIMARY KEY (id),
        |  CONSTRAINT unq_parking_slots UNIQUE (license_plate),
        |  CONSTRAINT unq_parking_slots_to_employees FOREIGN KEY (employee_id) REFERENCES employees (id)
        |)
      """.stripMargin
    )
    s.close()
  }

  val expectedDfasdlContent =
    """
      |<?xml version="1.0" encoding="UTF-8" standalone="no"?>
      |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" semantic="custom">
      |  <seq db-primary-key="id" id="salary_groups">
      |    <elem id="salary_groups_row">
      |      <num db-column-name="id" id="salary_groups_row_id" max-digits="10"/>
      |      <str db-column-name="name" id="salary_groups_row_name" max-length="64"/>
      |      <formatnum db-column-name="min_wage" decimal-separator="." format="(-?\d{0,6}\.\d{0,2})" id="salary_groups_row_min_wage" max-digits="8" max-precision="2"/>
      |      <formatnum db-column-name="max_wage" decimal-separator="." format="(-?\d{0,6}\.\d{0,2})" id="salary_groups_row_max_wage" max-digits="8" max-precision="2"/>
      |    </elem>
      |  </seq>
      |  <seq db-primary-key="id" id="employees">
      |    <elem id="employees_row">
      |      <num db-column-name="id" id="employees_row_id" max-digits="19"/>
      |      <str db-column-name="firstname" id="employees_row_firstname" max-length="64"/>
      |      <str db-column-name="lastname" id="employees_row_lastname" max-length="64"/>
      |      <date db-column-name="birthday" id="employees_row_birthday"/>
      |      <str db-column-name="notes" id="employees_row_notes"/>
      |      <num db-column-name="salary_group" db-foreign-key="salary_groups_row_id" id="employees_row_salary_group" max-digits="10"/>
      |    </elem>
      |  </seq>
      |  <seq db-primary-key="salary_group,employee_id" id="wages">
      |    <elem id="wages_row">
      |      <num db-column-name="employee_id" db-foreign-key="employees_row_id" id="wages_row_employee_id" max-digits="19"/>
      |      <num db-column-name="salary_group" db-foreign-key="salary_groups_row_id" id="wages_row_salary_group" max-digits="10"/>
      |      <formatnum db-column-name="wage" decimal-separator="." format="(-?\d{0,4}\.\d{0,2})" id="wages_row_wage" max-digits="6" max-precision="2"/>
      |    </elem>
      |  </seq>
      |  <seq db-primary-key="id" id="company_cars">
      |    <elem id="company_cars_row">
      |      <num db-column-name="id" id="company_cars_row_id" max-digits="10"/>
      |      <str db-column-name="license_plate" id="company_cars_row_license_plate" max-length="16"/>
      |      <num db-column-name="employee_id" db-foreign-key="employees_row_id" id="company_cars_row_employee_id" max-digits="19"/>
      |      <num db-column-name="seats" id="company_cars_row_seats" max-digits="10"/>
      |      <date db-column-name="bought" id="company_cars_row_bought"/>
      |    </elem>
      |  </seq>
      |  <seq db-primary-key="id" id="parking_slots">
      |    <elem id="parking_slots_row">
      |      <num db-column-name="id" id="parking_slots_row_id" max-digits="10"/>
      |      <num db-column-name="employee_id" db-foreign-key="employees_row_id" id="parking_slots_row_employee_id" max-digits="19"/>
      |      <str db-column-name="license_plate" id="parking_slots_row_license_plate" max-length="16"/>
      |    </elem>
      |  </seq>
      |</dfasdl>
    """.stripMargin

  describe("Database schema extraction") {
    describe("using firebird") {
      it("should extract the correct schema", DbTest, DbTestFirebird) {
        val connection = java.sql.DriverManager.getConnection(
          s"jdbc:firebirdsql://$databaseHost:$databasePort//$databasePath/$databaseName",
          databaseUser,
          databasePass
        )
        createDatabaseTables(connection) // Create tables.
        val src = ConnectionInformation(
          uri = new java.net.URI(connection.getMetaData.getURL),
          dfasdlRef = None,
          username = Option(databaseUser),
          password = Option(databasePass)
        )
        connection.close()

        val extractor = TestActorRef(SchemaExtractor.props(), "SchemaExtractor")

        extractor ! GlobalMessages.ExtractSchema(
          source = src,
          options = ExtractSchemaOptions.createDatabaseOptions()
        )

        val response = expectMsgType[GlobalMessages.ExtractSchemaResult]
        response.source should be(src)
        response.result match {
          case -\/(failure) => fail(failure)
          case \/-(success) => success.content.trim should be(expectedDfasdlContent.trim)
        }
      }
    }
  }
}
