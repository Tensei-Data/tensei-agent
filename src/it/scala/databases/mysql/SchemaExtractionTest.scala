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

import akka.testkit.TestActorRef
import com.wegtam.scalatest.tags.{ DbTest, DbTestMysql }
import com.wegtam.tensei.adt.{ ConnectionInformation, ExtractSchemaOptions, GlobalMessages }
import com.wegtam.tensei.agent.{ ActorSpec, SchemaExtractor }
import org.scalatest.BeforeAndAfterEach
import org.xmlunit.builder.{ DiffBuilder, Input }

import scalaz._
import scala.collection.JavaConverters._

class SchemaExtractionTest extends ActorSpec with BeforeAndAfterEach {

  val databaseHost = testConfig.getString("mysql.host")
  val databasePort = testConfig.getInt("mysql.port")
  val databaseName = testConfig.getString("mysql.target-db.name")
  val databaseUser = testConfig.getString("mysql.target-db.user")
  val databasePass = testConfig.getString("mysql.target-db.pass")

  override protected def beforeEach(): Unit = {
    val connection = java.sql.DriverManager
      .getConnection(s"jdbc:mysql://$databaseHost:$databasePort/", databaseUser, databasePass)
    val s = connection.createStatement()
    s.execute(s"DROP DATABASE IF EXISTS $databaseName")
    s.execute(s"CREATE DATABASE $databaseName")
    s.close()
    connection.close()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    val connection = java.sql.DriverManager
      .getConnection(s"jdbc:mysql://$databaseHost:$databasePort/", databaseUser, databasePass)
    val s = connection.createStatement()
    s.execute(s"DROP DATABASE IF EXISTS $databaseName")
    s.close()
    connection.close()
    super.afterEach()
  }

  /**
    * Create the test tables within the database.
    *
    * @param connection A connection to the database.
    */
  private def createDatabaseTables(connection: java.sql.Connection): Unit = {
    val s = connection.createStatement()
    s.execute("""
        |SET storage_engine=INNODB
      """.stripMargin)
    s.execute("""
        |CREATE TABLE salary_groups (
        |  id INT AUTO_INCREMENT,
        |  name VARCHAR(64) NOT NULL,
        |  min_wage DECIMAL(8,2) NOT NULL,
        |  max_wage DECIMAL(8,2) NOT NULL,
        |  CONSTRAINT PRIMARY KEY (id),
        |  CONSTRAINT UNIQUE (name)
        |)
      """.stripMargin)
    s.execute("""
        |CREATE TABLE employees (
        |  id BIGINT AUTO_INCREMENT,
        |  firstname VARCHAR(64) NOT NULL,
        |  lastname VARCHAR(64) NOT NULL,
        |  birthday DATE NOT NULL,
        |  notes TEXT,
        |  salary_group INT,
        |  CONSTRAINT PRIMARY KEY (id),
        |  CONSTRAINT FOREIGN KEY (salary_group) REFERENCES salary_groups (id)
        |)
      """.stripMargin)
    s.execute("""
        |CREATE TABLE wages (
        |  employee_id BIGINT,
        |  salary_group INT,
        |  wage DECIMAL(6,2) NOT NULL,
        |  CONSTRAINT PRIMARY KEY (employee_id, salary_group),
        |  CONSTRAINT FOREIGN KEY (employee_id) REFERENCES employees (id),
        |  CONSTRAINT FOREIGN KEY (salary_group) REFERENCES salary_groups (id)
        |)
      """.stripMargin)
    s.execute("""
        |CREATE TABLE company_cars (
        |  id INT AUTO_INCREMENT,
        |  license_plate VARCHAR(16) NOT NULL,
        |  employee_id BIGINT,
        |  seats SMALLINT,
        |  bought DATE,
        |  CONSTRAINT PRIMARY KEY (id),
        |  CONSTRAINT UNIQUE (license_plate),
        |  CONSTRAINT FOREIGN KEY (employee_id) REFERENCES employees (id)
        |)
      """.stripMargin)
    s.execute("""
        |CREATE TABLE parking_slots (
        |  id INT AUTO_INCREMENT,
        |  employee_id BIGINT,
        |  license_plate VARCHAR(16),
        |  CONSTRAINT PRIMARY KEY (id),
        |  CONSTRAINT UNIQUE (license_plate),
        |  CONSTRAINT FOREIGN KEY (employee_id) REFERENCES employees (id)
        |)
      """.stripMargin)
    s.close()
  }

  val expectedDfasdlContent =
    """
      |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" semantic="custom">
      |  <seq db-primary-key="id" id="company_cars">
      |    <elem id="company_cars_row">
      |      <num db-auto-inc="true" db-column-name="id" id="company_cars_row_id" max-digits="11"/>
      |      <str db-column-name="license_plate" id="company_cars_row_license_plate" max-length="16"/>
      |      <num db-column-name="employee_id" db-foreign-key="employees_row_id" id="company_cars_row_employee_id" max-digits="20"/>
      |      <num db-column-name="seats" id="company_cars_row_seats" max-digits="6"/>
      |      <date db-column-name="bought" id="company_cars_row_bought"/>
      |    </elem>
      |  </seq>
      |  <seq db-primary-key="id" id="employees">
      |    <elem id="employees_row">
      |      <num db-auto-inc="true" db-column-name="id" id="employees_row_id" max-digits="20"/>
      |      <str db-column-name="firstname" id="employees_row_firstname" max-length="64"/>
      |      <str db-column-name="lastname" id="employees_row_lastname" max-length="64"/>
      |      <date db-column-name="birthday" id="employees_row_birthday"/>
      |      <str db-column-name="notes" id="employees_row_notes" max-length="65535"/>
      |      <num db-column-name="salary_group" db-foreign-key="salary_groups_row_id" id="employees_row_salary_group" max-digits="11"/>
      |    </elem>
      |  </seq>
      |  <seq db-primary-key="id" id="parking_slots">
      |    <elem id="parking_slots_row">
      |      <num db-auto-inc="true" db-column-name="id" id="parking_slots_row_id" max-digits="11"/>
      |      <num db-column-name="employee_id" db-foreign-key="employees_row_id" id="parking_slots_row_employee_id" max-digits="20"/>
      |      <str db-column-name="license_plate" id="parking_slots_row_license_plate" max-length="16"/>
      |    </elem>
      |  </seq>
      |  <seq db-primary-key="id" id="salary_groups">
      |    <elem id="salary_groups_row">
      |      <num db-auto-inc="true" db-column-name="id" id="salary_groups_row_id" max-digits="11"/>
      |      <str db-column-name="name" id="salary_groups_row_name" max-length="64"/>
      |      <formatnum db-column-name="min_wage" decimal-separator="." format="(-?\d{0,6}\.\d{0,2})" id="salary_groups_row_min_wage" max-digits="8" max-precision="2"/>
      |      <formatnum db-column-name="max_wage" decimal-separator="." format="(-?\d{0,6}\.\d{0,2})" id="salary_groups_row_max_wage" max-digits="8" max-precision="2"/>
      |    </elem>
      |  </seq>
      |  <seq db-primary-key="employee_id,salary_group" id="wages">
      |    <elem id="wages_row">
      |      <num db-column-name="employee_id" db-foreign-key="employees_row_id" defaultnum="0" id="wages_row_employee_id" max-digits="20"/>
      |      <num db-column-name="salary_group" db-foreign-key="salary_groups_row_id" defaultnum="0" id="wages_row_salary_group" max-digits="11"/>
      |      <formatnum db-column-name="wage" decimal-separator="." format="(-?\d{0,4}\.\d{0,2})" id="wages_row_wage" max-digits="6" max-precision="2"/>
      |    </elem>
      |  </seq>
      |</dfasdl>
    """.stripMargin

  val expectedDfasdlContentVariant =
    """
      |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" semantic="custom">
      |  <seq db-primary-key="id" id="company_cars">
      |    <elem id="company_cars_row">
      |      <num db-auto-inc="true" db-column-name="id" id="company_cars_row_id" max-digits="11"/>
      |      <str db-column-name="license_plate" id="company_cars_row_license_plate" max-length="16"/>
      |      <num db-column-name="employee_id" db-foreign-key="employees_row_id" id="company_cars_row_employee_id" max-digits="20"/>
      |      <num db-column-name="seats" id="company_cars_row_seats" max-digits="6"/>
      |      <date db-column-name="bought" id="company_cars_row_bought"/>
      |    </elem>
      |  </seq>
      |  <seq db-primary-key="id" id="employees">
      |    <elem id="employees_row">
      |      <num db-auto-inc="true" db-column-name="id" id="employees_row_id" max-digits="20"/>
      |      <str db-column-name="firstname" id="employees_row_firstname" max-length="64"/>
      |      <str db-column-name="lastname" id="employees_row_lastname" max-length="64"/>
      |      <date db-column-name="birthday" id="employees_row_birthday"/>
      |      <str db-column-name="notes" id="employees_row_notes" max-length="65535"/>
      |      <num db-column-name="salary_group" db-foreign-key="salary_groups_row_id" id="employees_row_salary_group" max-digits="11"/>
      |    </elem>
      |  </seq>
      |  <seq db-primary-key="id" id="parking_slots">
      |    <elem id="parking_slots_row">
      |      <num db-auto-inc="true" db-column-name="id" id="parking_slots_row_id" max-digits="11"/>
      |      <num db-column-name="employee_id" db-foreign-key="employees_row_id" id="parking_slots_row_employee_id" max-digits="20"/>
      |      <str db-column-name="license_plate" id="parking_slots_row_license_plate" max-length="16"/>
      |    </elem>
      |  </seq>
      |  <seq db-primary-key="id" id="salary_groups">
      |    <elem id="salary_groups_row">
      |      <num db-auto-inc="true" db-column-name="id" id="salary_groups_row_id" max-digits="11"/>
      |      <str db-column-name="name" id="salary_groups_row_name" max-length="64"/>
      |      <formatnum db-column-name="min_wage" decimal-separator="." format="(-?\d{0,6}\.\d{0,2})" id="salary_groups_row_min_wage" max-digits="8" max-precision="2"/>
      |      <formatnum db-column-name="max_wage" decimal-separator="." format="(-?\d{0,6}\.\d{0,2})" id="salary_groups_row_max_wage" max-digits="8" max-precision="2"/>
      |    </elem>
      |  </seq>
      |  <seq db-primary-key="salary_group,employee_id" id="wages">
      |    <elem id="wages_row">
      |      <num db-column-name="employee_id" db-foreign-key="employees_row_id" defaultnum="0" id="wages_row_employee_id" max-digits="20"/>
      |      <num db-column-name="salary_group" db-foreign-key="salary_groups_row_id" defaultnum="0" id="wages_row_salary_group" max-digits="11"/>
      |      <formatnum db-column-name="wage" decimal-separator="." format="(-?\d{0,4}\.\d{0,2})" id="wages_row_wage" max-digits="6" max-precision="2"/>
      |    </elem>
      |  </seq>
      |</dfasdl>
    """.stripMargin

  describe("Database schema extraction") {
    describe("using mysql") {
      it("should extract the correct schema", DbTest, DbTestMysql) {
        val connection = java.sql.DriverManager.getConnection(
          s"jdbc:mysql://$databaseHost:$databasePort/$databaseName",
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
          case \/-(success) =>
            // Testing this is somewhat tedious because attribute content is sometimes in different order.
            val diff = DiffBuilder
              .compare(Input.fromString(expectedDfasdlContent))
              .withTest(Input.fromString(success.content.trim))
              .ignoreWhitespace()
              .build()
            if (diff.hasDifferences) {
              // Check if the variant differs too.
              val diffVariant = DiffBuilder
                .compare(Input.fromString(expectedDfasdlContentVariant))
                .withTest(Input.fromString(success.content.trim))
                .ignoreWhitespace()
                .build()
              if (diffVariant.hasDifferences) {
                val diffs = diff.getDifferences.asScala.toVector.map(_.toString)
                fail(s"XML was not equal! ${diffs.mkString(", ")}")
              }
            }
        }
      }
    }
  }
}
