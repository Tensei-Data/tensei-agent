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

package com.wegtam.tensei.agent.writers

import java.net.URI

import akka.testkit.{ EventFilter, TestFSMRef }
import com.wegtam.tensei.adt.{ ConnectionInformation, DFASDL, DFASDLReference }
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages._
import com.wegtam.tensei.agent.writers.BaseWriter._

/**
  * @todo Move to integration tests!
  */
class DatabaseWriterActorTest extends ActorSpec {
  describe("DatabaseWriterActor") {
    describe("buffer ready to work requests") {
      it("should answer after it has initialized") {
        val connection = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:test")

        val dfasdlFile = "/com/wegtam/tensei/agent/writers/DatabaseWriter/simple-01.xml"
        val xml =
          scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
        val dfasdl = new DFASDL("SIMPLE-01", xml)
        val target = new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                               dfasdlRef =
                                                 Option(DFASDLReference("TEST", "SIMPLE-01")))

        val writer =
          TestFSMRef(new DatabaseWriterActor(target, dfasdl, Option("DatabaseWriterActorTest")))
        writer.stateName should be(BaseWriter.State.Initializing)
        writer ! AreYouReady
        writer ! BaseWriterMessages.InitializeTarget
        val expectedMsg = ReadyToWork
        expectMsg(expectedMsg)

        val statement = connection.createStatement()
        statement.execute("SHUTDOWN")
        connection.close()
      }
    }

    describe("should throw an exception if") {
      it("an sql exception occurs") {
        val connection = java.sql.DriverManager.getConnection(s"jdbc:h2:mem:test")

        val dfasdlFile = "/com/wegtam/tensei/agent/writers/DatabaseWriter/simple-01.xml"
        val xml =
          scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
        val dfasdl = new DFASDL("SIMPLE-01", xml)
        val target = new ConnectionInformation(uri = new URI(connection.getMetaData.getURL),
                                               dfasdlRef =
                                                 Option(DFASDLReference("TEST", "SIMPLE-01")))

        val writer =
          TestFSMRef(new DatabaseWriterActor(target, dfasdl, Option("DatabaseWriterActorTest")))
        writer.stateName should be(BaseWriter.State.Initializing)
        writer ! AreYouReady
        writer ! BaseWriterMessages.InitializeTarget
        val expectedMsg = ReadyToWork
        expectMsg(expectedMsg)

        val batch: List[WriteData] = List(
          WriteData(
            number = 1L,
            data = None,
            metaData = Option(WriterMessageMetaData(id = "id"))
          ),
          WriteData(
            number = 2L,
            data = "A name field.",
            metaData = Option(WriterMessageMetaData(id = "name"))
          ),
          WriteData(
            number = 3L,
            data = "Some description...",
            metaData = Option(WriterMessageMetaData(id = "description"))
          ),
          WriteData(
            number = 4L,
            data = "This should produce an error!",
            metaData = Option(WriterMessageMetaData(id = "birthday"))
          ),
          WriteData(
            number = 5L,
            data = "This should produce an error!",
            metaData = Option(WriterMessageMetaData(id = "salary"))
          )
        )

        EventFilter[RuntimeException](
          source = writer.path.toString,
          occurrences = 1,
          message = "SQL exception while writing sequence 'accounts'! See log for details."
        ) intercept {
          writer ! WriteBatchData(batch)
          writer ! CloseWriter
        }

        val statement = connection.createStatement()
        statement.execute("SHUTDOWN")
        connection.close()
      }
    }
  }
}
