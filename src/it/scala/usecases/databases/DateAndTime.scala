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
import com.wegtam.tensei.adt.Recipe.MapOneToOne
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages.{
  GetSequenceRowCount,
  SequenceRowCount
}
import com.wegtam.tensei.agent.Parser.{ ParserCompletedStatus, ParserMessages }
import com.wegtam.tensei.agent.Processor.ProcessorMessages
import com.wegtam.tensei.agent.adt.ParserStatus
import com.wegtam.tensei.agent._
import org.scalatest.BeforeAndAfterEach

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

/**
  * Created by andre on 02.02.16.
  */
class DateAndTime extends XmlActorSpec with BeforeAndAfterEach {
  val agentRunIdentifier = Option("DateAndTime")

  val targetDatabaseName = "testdst"

  describe("non-iso format") {
    describe("non-iso date to date field") {
      it("should work", DbTest, DbTestH2) {
        val data = getClass.getResource("/usecases/dateAndTime/non-iso-date.csv").toURI
        val sourceDfasdl =
          DFASDL("XML-SOURCE-DATA",
                 scala.io.Source
                   .fromInputStream(
                     getClass.getResourceAsStream("/usecases/dateAndTime/non-iso-date.xml")
                   )
                   .mkString)
        val targetDfasdl =
          DFASDL("DB-TARGET-DATA",
                 scala.io.Source
                   .fromInputStream(
                     getClass.getResourceAsStream("/usecases/dateAndTime/non-iso-date-db.xml")
                   )
                   .mkString)

        val customersRecipe = Recipe(
          "MapColumns",
          MapOneToOne,
          List(
            MappingTransformation(
              List(
                ElementReference(sourceDfasdl.id, "entry-0")
              ),
              List(
                ElementReference(targetDfasdl.id, "test_row_active")
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

        dataTree ! GetSequenceRowCount(ElementReference(sourceDfasdl.id, "lines"))
        val count = expectMsgType[SequenceRowCount]
        count.rows.getOrElse(0L) should be(3)

        val processor = TestFSMRef(new Processor(agentRunIdentifier))
        processor ! ProcessorMessages.StartProcessingMessage(agentStartTransformationMessage,
                                                             List(dataTree))
        expectMsg(ProcessorMessages.Completed)

        val actualData = {
          val stm      = targetDatabase.createStatement()
          val results  = stm.executeQuery("SELECT * FROM TEST")
          val rows     = new ListBuffer[String]
          var rowCount = 0
          while (results.next()) {
            rowCount += 1
            rows += s"${results.getString("active")}"
          }
          withClue(s"The database table should have 3 rows!")(rowCount should be(3))
          rows.toList.mkString(";")
        }
        val expectedData = """2004-12-23;2001-01-11;2008-07-11"""

        withClue(s"The database table should have the proper content!")(
          actualData should be(expectedData)
        )

        val dst = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$targetDatabaseName")
        dst.createStatement().execute("SHUTDOWN")
        dst.close()
      }
    }

    describe("non-iso date to date field with a complete Agent") {
      it("should work", DbTest, DbTestH2) {
        val data = getClass.getResource("/usecases/dateAndTime/non-iso-date.csv").toURI
        val sourceDfasdl =
          DFASDL("XML-SOURCE-DATA",
                 scala.io.Source
                   .fromInputStream(
                     getClass.getResourceAsStream("/usecases/dateAndTime/non-iso-date.xml")
                   )
                   .mkString)
        val targetDfasdl =
          DFASDL("DB-TARGET-DATA",
                 scala.io.Source
                   .fromInputStream(
                     getClass.getResourceAsStream("/usecases/dateAndTime/non-iso-date-db.xml")
                   )
                   .mkString)

        val customersRecipe = Recipe(
          "MapColumns",
          MapOneToOne,
          List(
            MappingTransformation(
              List(
                ElementReference(sourceDfasdl.id, "entry-0")
              ),
              List(
                ElementReference(targetDfasdl.id, "test_row_active")
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

        val dummy  = TestActorRef(DummyActor.props())
        val client = system.actorSelection(dummy.path)
        val agent  = TestFSMRef(new TenseiAgent("TEST-AGENT", client))

        agent ! agentStartTransformationMessage

        expectMsgType[GlobalMessages.TransformationStarted](FiniteDuration(5, SECONDS))

        expectMsgType[GlobalMessages.TransformationCompleted](FiniteDuration(7, SECONDS))

        val actualData = {
          val stm      = targetDatabase.createStatement()
          val results  = stm.executeQuery("SELECT * FROM TEST")
          val rows     = new ListBuffer[String]
          var rowCount = 0
          while (results.next()) {
            rowCount += 1
            rows += s"${results.getString("active")}"
          }
          withClue(s"The database table should have 3 rows!")(rowCount should be(3))
          rows.toList.mkString(";")
        }
        val expectedData = """2004-12-23;2001-01-11;2008-07-11"""

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
