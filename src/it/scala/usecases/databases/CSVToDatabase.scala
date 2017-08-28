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

import java.net.URI

import akka.testkit.{ TestActorRef, TestFSMRef }
import com.wegtam.scalatest.tags.{ DbTest, DbTestH2 }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.adt.Recipe.MapOneToOne
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages.{
  GetSequenceRowCount,
  SequenceRowCount
}
import com.wegtam.tensei.agent.Parser.{ ParserCompletedStatus, ParserMessages }
import com.wegtam.tensei.agent.Processor.ProcessorMessages
import com.wegtam.tensei.agent._
import com.wegtam.tensei.agent.adt.ParserStatus
import org.scalatest.BeforeAndAfterEach

import scala.collection.mutable.ListBuffer

class CSVToDatabase extends XmlActorSpec with BeforeAndAfterEach {
  val agentRunIdentifier = Option("CSVToDatabaseTest")

  val targetDatabaseName = "testdst"

  describe("CSVToDatabase") {
    describe("when given an empty csv file") {
      it("should write no data", DbTest, DbTestH2) {
        val data = getClass.getResource("/usecases/csvtodatabase/source-empty.csv").toURI
        val sourceDfasdl = DFASDL(
          "XML-SOURCE-DATA",
          scala.io.Source
            .fromInputStream(getClass.getResourceAsStream("/usecases/csvtodatabase/source-01.xml"))
            .mkString
        )
        val targetDfasdl = DFASDL(
          "DB-TARGET-DATA",
          scala.io.Source
            .fromInputStream(getClass.getResourceAsStream("/usecases/csvtodatabase/source-01.xml"))
            .mkString
        )

        val customersRecipe = Recipe(
          "MapColumns",
          MapOneToOne,
          List(
            MappingTransformation(
              List(
                ElementReference(sourceDfasdl.id, "firstname"),
                ElementReference(sourceDfasdl.id, "lastname"),
                ElementReference(sourceDfasdl.id, "email")
              ),
              List(
                ElementReference(targetDfasdl.id, "firstname"),
                ElementReference(targetDfasdl.id, "lastname"),
                ElementReference(targetDfasdl.id, "email")
              )
            )
          )
        )

        val cookbook = Cookbook("EmptySourceTest",
                                List(sourceDfasdl),
                                Option(targetDfasdl),
                                List(customersRecipe))

        val sourceConnection =
          ConnectionInformation(data, Option(DFASDLReference(cookbook.id, sourceDfasdl.id)))
        val targetDatabase =
          java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")
        val targetConnection =
          ConnectionInformation(new URI(targetDatabase.getMetaData.getURL),
                                Option(DFASDLReference(cookbook.id, targetDfasdl.id)))

        val agentStartTransformationMessage =
          AgentStartTransformationMessage(List(sourceConnection),
                                          targetConnection,
                                          cookbook,
                                          agentRunIdentifier)

        val dataTree = TestActorRef(
          DataTreeDocument.props(sourceDfasdl, Option("EmptySourceTest"), Set.empty[String])
        )
        val parser = TestFSMRef(new Parser(agentRunIdentifier))

        parser ! ParserMessages.StartParsing(agentStartTransformationMessage,
                                             Map(sourceDfasdl.hashCode() -> dataTree))
        val parserResponse = expectMsgType[ParserCompletedStatus]
        parserResponse.statusMessages.foreach(status => status should be(ParserStatus.COMPLETED))

        dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "rows"))
        val count = expectMsgType[SequenceRowCount]
        count.rows.getOrElse(0L) should be(0)

        val processor = TestFSMRef(new Processor(agentRunIdentifier))
        processor ! ProcessorMessages.StartProcessingMessage(agentStartTransformationMessage,
                                                             List(dataTree))
        expectMsg(ProcessorMessages.Completed)

        val stm     = targetDatabase.createStatement()
        val results = stm.executeQuery("SELECT * FROM ROWS")
        var rows    = 0
        while (results.next()) {
          rows += 1
        }
        withClue(s"The database table should have no content!")(rows should be(0))

        val dst = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")
        dst.createStatement().execute("SHUTDOWN")
        dst.close()
      }
    }

    describe("when given a simple csv file") {
      describe("and the same DFASDL structure") {
        it("should write the data into the database", DbTest, DbTestH2) {
          val data = getClass.getResource("/usecases/csvtodatabase/source-01.csv").toURI
          val sourceDfasdl =
            DFASDL("XML-SOURCE-DATA",
                   scala.io.Source
                     .fromInputStream(
                       getClass.getResourceAsStream("/usecases/csvtodatabase/source-01.xml")
                     )
                     .mkString)
          val targetDfasdl =
            DFASDL("DB-TARGET-DATA",
                   scala.io.Source
                     .fromInputStream(
                       getClass.getResourceAsStream("/usecases/csvtodatabase/source-01.xml")
                     )
                     .mkString)

          val customersRecipe = Recipe(
            "MapColumns",
            MapOneToOne,
            List(
              MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "firstname"),
                  ElementReference(sourceDfasdl.id, "lastname"),
                  ElementReference(sourceDfasdl.id, "email")
                ),
                List(
                  ElementReference(targetDfasdl.id, "firstname"),
                  ElementReference(targetDfasdl.id, "lastname"),
                  ElementReference(targetDfasdl.id, "email")
                )
              )
            )
          )

          val cookbook = Cookbook("SimpleSourceTest",
                                  List(sourceDfasdl),
                                  Option(targetDfasdl),
                                  List(customersRecipe))

          val sourceConnection =
            ConnectionInformation(data, Option(DFASDLReference(cookbook.id, sourceDfasdl.id)))
          val targetDatabase =
            java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")
          val targetConnection =
            ConnectionInformation(new URI(targetDatabase.getMetaData.getURL),
                                  Option(DFASDLReference(cookbook.id, targetDfasdl.id)))

          val agentStartTransformationMessage =
            AgentStartTransformationMessage(List(sourceConnection),
                                            targetConnection,
                                            cookbook,
                                            agentRunIdentifier)

          val dataTree = TestActorRef(
            DataTreeDocument.props(sourceDfasdl, Option("SimpleSourceTest"), Set.empty[String])
          )
          val parser = TestFSMRef(new Parser(agentRunIdentifier))

          parser ! ParserMessages.StartParsing(agentStartTransformationMessage,
                                               Map(sourceDfasdl.hashCode() -> dataTree))
          val parserResponse = expectMsgType[ParserCompletedStatus]
          parserResponse.statusMessages.foreach(status => status should be(ParserStatus.COMPLETED))

          dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "rows"))
          val count = expectMsgType[SequenceRowCount]
          count.rows.getOrElse(0L) should be(3)

          val processor = TestFSMRef(new Processor(agentRunIdentifier))
          processor ! ProcessorMessages.StartProcessingMessage(agentStartTransformationMessage,
                                                               List(dataTree))
          expectMsg(ProcessorMessages.Completed)

          val actualData = {
            val stm      = targetDatabase.createStatement()
            val results  = stm.executeQuery("SELECT * FROM ROWS")
            val rows     = new ListBuffer[String]
            var rowCount = 0
            while (results.next()) {
              rowCount += 1
              rows += s"${results.getString("firstname")},${results.getString("lastname")},${results
                .getString("email")}"
            }
            withClue(s"The database table should have 3 rows!")(rowCount should be(3))
            rows.toList.mkString(";")
          }
          val expectedData =
            """John,Doe,john.doe@example.com;Jane,Doe,jane.doe@example.com;Max,Mustermann,max.mustermann@example.com"""
          withClue(s"The database table should have the proper content!")(
            actualData should be(expectedData)
          )

          val dst = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")
          dst.createStatement().execute("SHUTDOWN")
          dst.close()
        }
      }

      describe("and a different target DFASDL structure") {
        it("should write the data into the database", DbTest, DbTestH2) {
          val data = getClass.getResource("/usecases/csvtodatabase/source-01.csv").toURI
          val sourceDfasdl =
            DFASDL("XML-SOURCE-DATA",
                   scala.io.Source
                     .fromInputStream(
                       getClass.getResourceAsStream("/usecases/csvtodatabase/source-01.xml")
                     )
                     .mkString)
          val targetDfasdl =
            DFASDL("DB-TARGET-DATA",
                   scala.io.Source
                     .fromInputStream(
                       getClass.getResourceAsStream("/usecases/csvtodatabase/target-01.xml")
                     )
                     .mkString)

          val customersRecipe = Recipe(
            "MapColumns",
            MapOneToOne,
            List(
              MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "email"),
                  ElementReference(sourceDfasdl.id, "firstname"),
                  ElementReference(sourceDfasdl.id, "lastname")
                ),
                List(
                  ElementReference(targetDfasdl.id, "email"),
                  ElementReference(targetDfasdl.id, "firstname"),
                  ElementReference(targetDfasdl.id, "lastname")
                )
              )
            )
          )

          val cookbook = Cookbook("SimpleSourceTest",
                                  List(sourceDfasdl),
                                  Option(targetDfasdl),
                                  List(customersRecipe))

          val sourceConnection =
            ConnectionInformation(data, Option(DFASDLReference(cookbook.id, sourceDfasdl.id)))
          val targetDatabase =
            java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")
          val targetConnection =
            ConnectionInformation(new URI(targetDatabase.getMetaData.getURL),
                                  Option(DFASDLReference(cookbook.id, targetDfasdl.id)))

          val agentStartTransformationMessage =
            AgentStartTransformationMessage(List(sourceConnection),
                                            targetConnection,
                                            cookbook,
                                            agentRunIdentifier)

          val dataTree = TestActorRef(
            DataTreeDocument.props(sourceDfasdl, Option("SimpleSourceTest"), Set.empty[String])
          )
          val parser = TestFSMRef(new Parser(agentRunIdentifier))

          parser ! ParserMessages.StartParsing(agentStartTransformationMessage,
                                               Map(sourceDfasdl.hashCode() -> dataTree))
          val parserResponse = expectMsgType[ParserCompletedStatus]
          parserResponse.statusMessages.foreach(status => status should be(ParserStatus.COMPLETED))

          dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "rows"))
          val count = expectMsgType[SequenceRowCount]
          count.rows.getOrElse(0L) should be(3)

          val processor = TestFSMRef(new Processor(agentRunIdentifier))
          processor ! ProcessorMessages.StartProcessingMessage(agentStartTransformationMessage,
                                                               List(dataTree))
          expectMsg(ProcessorMessages.Completed)

          val actualData = {
            val stm      = targetDatabase.createStatement()
            val results  = stm.executeQuery("SELECT * FROM ROWS")
            val rows     = new ListBuffer[String]
            var rowCount = 0
            while (results.next()) {
              rowCount += 1
              rows += s"${results.getString("firstname")},${results.getString("lastname")},${results
                .getString("email")}"
            }
            withClue(s"The database table should have 3 rows!")(rowCount should be(3))
            rows.toList.mkString(";")
          }
          val expectedData =
            """John,Doe,john.doe@example.com;Jane,Doe,jane.doe@example.com;Max,Mustermann,max.mustermann@example.com"""

          withClue(s"The database table should have the proper content!")(
            actualData should be(expectedData)
          )

          val dst = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")
          dst.createStatement().execute("SHUTDOWN")
          dst.close()
        }
      }
    }

    describe("when given another simple CSV file") {
      describe("with 2 empty lines at the end of the file") {
        it("should write the data into the database", DbTest, DbTestH2) {
          val data = getClass.getResource("/usecases/csvtodatabase/source-02.csv").toURI
          val sourceDfasdl =
            DFASDL("XML-SOURCE-DATA",
                   scala.io.Source
                     .fromInputStream(
                       getClass.getResourceAsStream("/usecases/csvtodatabase/source-02.xml")
                     )
                     .mkString)
          val targetDfasdl =
            DFASDL("DB-TARGET-DATA",
                   scala.io.Source
                     .fromInputStream(
                       getClass.getResourceAsStream("/usecases/csvtodatabase/target-02.xml")
                     )
                     .mkString)

          val customersRecipe = Recipe(
            "MapColumns",
            MapOneToOne,
            List(
              MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "lastname"),
                  ElementReference(sourceDfasdl.id, "firstname"),
                  ElementReference(sourceDfasdl.id, "email"),
                  ElementReference(sourceDfasdl.id, "birthday"),
                  ElementReference(sourceDfasdl.id, "phone"),
                  ElementReference(sourceDfasdl.id, "division")
                ),
                List(
                  ElementReference(targetDfasdl.id, "employee_row_lastname"),
                  ElementReference(targetDfasdl.id, "employee_row_firstname"),
                  ElementReference(targetDfasdl.id, "employee_row_email"),
                  ElementReference(targetDfasdl.id, "employee_row_birthday"),
                  ElementReference(targetDfasdl.id, "employee_row_phone"),
                  ElementReference(targetDfasdl.id, "employee_row_department")
                )
              ),
              MappingTransformation(
                List(
                  ElementReference(sourceDfasdl.id, "lastname")
                ),
                List(
                  ElementReference(targetDfasdl.id, "employee_row_id")
                ),
                List(
                  TransformationDescription(
                    transformerClassName = "com.wegtam.tensei.agent.transformers.Nullify",
                    options = TransformerOptions(classOf[String], classOf[String])
                  )
                )
              )
            )
          )

          val cookbook = Cookbook("SimpleSourceTest",
                                  List(sourceDfasdl),
                                  Option(targetDfasdl),
                                  List(customersRecipe))

          val sourceConnection =
            ConnectionInformation(data, Option(DFASDLReference(cookbook.id, sourceDfasdl.id)))
          val targetDatabase =
            java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")
          val targetConnection =
            ConnectionInformation(new URI(targetDatabase.getMetaData.getURL),
                                  Option(DFASDLReference(cookbook.id, targetDfasdl.id)))

          val agentStartTransformationMessage =
            AgentStartTransformationMessage(List(sourceConnection),
                                            targetConnection,
                                            cookbook,
                                            agentRunIdentifier)

          val dataTree = TestActorRef(
            DataTreeDocument.props(sourceDfasdl, Option("SimpleSourceTest"), Set.empty[String])
          )
          val parser = TestFSMRef(new Parser(agentRunIdentifier))

          parser ! ParserMessages.StartParsing(agentStartTransformationMessage,
                                               Map(sourceDfasdl.hashCode() -> dataTree))
          val parserResponse = expectMsgType[ParserCompletedStatus]
          parserResponse.statusMessages.foreach(status => status should be(ParserStatus.COMPLETED))

          dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "persons"))
          val count = expectMsgType[SequenceRowCount]
          withClue(s"The parser should read 3 rows!")(count.rows.getOrElse(0L) should be(3))

          val processor = TestFSMRef(new Processor(agentRunIdentifier))
          processor ! ProcessorMessages.StartProcessingMessage(agentStartTransformationMessage,
                                                               List(dataTree))
          expectMsg(ProcessorMessages.Completed)

          val actualData = {
            val stm      = targetDatabase.createStatement()
            val results  = stm.executeQuery("SELECT * FROM EMPLOYEE")
            val rows     = new ListBuffer[String]
            var rowCount = 0
            while (results.next()) {
              rowCount += 1
              rows += s"${results.getString("firstname")},${results.getString("lastname")},${results
                .getString("email")}"
            }
            withClue(s"The database table should have 3 rows!")(rowCount should be(3))
            rows.toList.mkString(";")
          }
          val expectedData =
            """John,Doe,john.doe@example.com;Jane,Doe,jane.doe@example.com;Jake,Doe,jake.doe@example.com"""
          withClue(s"The database table should have the proper content!")(
            actualData should be(expectedData)
          )

          val dst = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")
          dst.createStatement().execute("SHUTDOWN")
          dst.close()
        }
      }
    }
  }
}
