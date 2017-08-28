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

import java.math.BigDecimal

import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages.{
  AreYouReady,
  ReadyToWork,
  WriteBatchData,
  WriteData
}
import com.wegtam.tensei.agent.writers.FileWriterActor.FileWriterData
import org.dfasdl.utils.{ AttributeNames, ElementHelpers }
import akka.testkit.TestFSMRef
import java.net.URI
import java.io.File
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.writers.BaseWriter._
import com.wegtam.tensei.adt.{ ConnectionInformation, DFASDL, DFASDLReference }

import scalaz._, Scalaz._

class FileWriterActorTest extends ActorSpec with ElementHelpers {
  private def initializeWriter(
      con: ConnectionInformation,
      dfasdl: Option[DFASDL] = None
  ): TestFSMRef[BaseWriter.State, FileWriterData, FileWriterActor] = {
    val targetDfasdl = dfasdl.getOrElse(
      DFASDL(
        "TEST-DFASDL",
        """<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem"></dfasdl>"""
      )
    )
    val writer = TestFSMRef(new FileWriterActor(con, targetDfasdl, Option("FileWriterTest")))
    writer.stateName should be(BaseWriter.State.Initializing)
    writer ! BaseWriterMessages.InitializeTarget
    writer ! AreYouReady
    val expectedMsg = ReadyToWork
    expectMsg(expectedMsg)
    writer
  }

  describe("FileWriterActor") {
    describe("buffer ready to work requests") {
      it("should answer after it has initialized") {
        val tmpFile        = File.createTempFile("fileWriterTest", "test")
        val targetFilePath = tmpFile.getAbsolutePath.replace("\\", "/")
        tmpFile.delete()
        val con = new ConnectionInformation(new URI(s"$targetFilePath"), None)
        val writer =
          TestFSMRef(new FileWriterActor(con, DFASDL("TEST", ""), Option("FileWriterTest")))
        writer.stateName should be(BaseWriter.State.Initializing)
        writer ! AreYouReady
        writer ! BaseWriterMessages.InitializeTarget
        val expectedMsg = ReadyToWork
        expectMsg(expectedMsg)
      }
    }

    describe("initialize") {
      it("should create an empty file") {
        val tmpFile        = File.createTempFile("fileWriterTest", "test")
        val targetFilePath = tmpFile.getAbsolutePath.replace("\\", "/")
        tmpFile.delete()
        val con = new ConnectionInformation(new URI(s"$targetFilePath"), None)

        initializeWriter(con)

        val targetFile = new File(targetFilePath)
        targetFile.exists() should be(true)
        targetFile.length() should be(0)

        targetFile.delete()
      }
    }

    describe("when receiving a write request") {
      it("should write the data to the file") {
        val tmpFile        = File.createTempFile("fileWriterTest", "test")
        val targetFilePath = tmpFile.getAbsolutePath.replace("\\", "/")
        tmpFile.delete()
        val con = new ConnectionInformation(new URI(s"$targetFilePath"), None)

        val fileWriter = initializeWriter(con)
        val targetFile = new File(targetFilePath)
        fileWriter ! new WriteData(number = 1,
                                   data = "Max Mustermann",
                                   metaData = Option(WriterMessageMetaData(id = "ID")))
        fileWriter ! BaseWriterMessages.CloseWriter
        expectMsg(BaseWriterMessages.WriterClosed("".right[String]))

        val expectedContent = scala.io.Source
          .fromInputStream(
            getClass
              .getResourceAsStream("/com/wegtam/tensei/agent/writers/FileWriter/simple-01.txt")
          )
          .mkString
        val actualContent = scala.io.Source.fromFile(targetFile).mkString

        expectedContent shouldEqual actualContent

        targetFile.delete()
      }
    }

    describe("when receiving two write request") {
      it("should write the data to the file") {
        val tmpFile        = File.createTempFile("fileWriterTest", "test")
        val targetFilePath = tmpFile.getAbsolutePath.replace("\\", "/")
        tmpFile.delete()
        val con = new ConnectionInformation(new URI(s"$targetFilePath"), None)

        val fileWriter = initializeWriter(con)
        val targetFile = new File(targetFilePath)
        fileWriter ! new WriteData(number = 1,
                                   data = "Max Mustermann",
                                   options = List(("skip-stop-sign", "true")),
                                   metaData = Option(WriterMessageMetaData(id = "ID")))
        fileWriter ! new WriteData(number = 2,
                                   data = ";Musterhausen",
                                   metaData = Option(WriterMessageMetaData(id = "ID")))
        fileWriter ! BaseWriterMessages.CloseWriter
        expectMsg(BaseWriterMessages.WriterClosed("".right[String]))

        val expectedContent = scala.io.Source
          .fromInputStream(
            getClass
              .getResourceAsStream("/com/wegtam/tensei/agent/writers/FileWriter/simple-02.txt")
          )
          .mkString
        val actualContent = scala.io.Source.fromFile(targetFile).mkString

        expectedContent shouldEqual actualContent

        targetFile.delete()
      }
    }

    describe(s"when receiving two write request including a ${AttributeNames.STOP_SIGN}") {
      it(s"should write the data to the file and include the ${AttributeNames.STOP_SIGN}") {
        val tmpFile        = File.createTempFile("fileWriterTest", "test")
        val targetFilePath = tmpFile.getAbsolutePath.replace("\\", "/")
        tmpFile.delete()
        val con = new ConnectionInformation(new URI(s"$targetFilePath"), None)

        val fileWriter = initializeWriter(con)
        val targetFile = new File(targetFilePath)
        fileWriter ! new WriteData(number = 1,
                                   data = "Max Mustermann",
                                   options = List((AttributeNames.STOP_SIGN, ";")),
                                   metaData = Option(WriterMessageMetaData(id = "ID")))
        fileWriter ! new WriteData(number = 2,
                                   data = "Musterhausen",
                                   metaData = Option(WriterMessageMetaData(id = "ID")))
        fileWriter ! BaseWriterMessages.CloseWriter
        expectMsg(BaseWriterMessages.WriterClosed("".right[String]))

        val expectedContent = scala.io.Source
          .fromInputStream(
            getClass
              .getResourceAsStream("/com/wegtam/tensei/agent/writers/FileWriter/simple-02.txt")
          )
          .mkString
        val actualContent = scala.io.Source.fromFile(targetFile).mkString

        expectedContent shouldEqual actualContent

        targetFile.delete()
      }
    }

    describe(
      s"when receiving two write request including a ${AttributeNames.STOP_SIGN} with a newline"
    ) {
      it(s"should write the data with to the file and include the ${AttributeNames.STOP_SIGN}") {
        val tmpFile        = File.createTempFile("fileWriterTest", "test")
        val targetFilePath = tmpFile.getAbsolutePath.replace("\\", "/")
        tmpFile.delete()
        val con = new ConnectionInformation(new URI(s"$targetFilePath"), None)

        val fileWriter = initializeWriter(con)
        val targetFile = new File(targetFilePath)
        fileWriter ! new WriteData(number = 1,
                                   data = "Max Mustermann",
                                   options = List((AttributeNames.STOP_SIGN, ";")),
                                   metaData = Option(WriterMessageMetaData(id = "ID")))
        fileWriter ! new WriteData(number = 2,
                                   data = "Musterhausen",
                                   options = List((AttributeNames.STOP_SIGN, "\r\n")),
                                   metaData = Option(WriterMessageMetaData(id = "ID")))
        fileWriter ! new WriteData(number = 3,
                                   data = "Inga Bergmann",
                                   options = List((AttributeNames.STOP_SIGN, ":")),
                                   metaData = Option(WriterMessageMetaData(id = "ID")))
        fileWriter ! BaseWriterMessages.CloseWriter
        expectMsg(BaseWriterMessages.WriterClosed("".right[String]))

        val expectedContent = "Max Mustermann;Musterhausen\r\nInga Bergmann:"
        val actualContent   = scala.io.Source.fromFile(targetFile).mkString

        expectedContent shouldEqual actualContent

        targetFile.delete()
      }
    }

    describe("when retrieving ordered column data") {
      it("should write the columns in correct order") {
        val dfasdlFile = "/com/wegtam/tensei/agent/writers/FileWriter/column-test-01.xml"
        val xml =
          scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFile)).mkString
        val dfasdl = new DFASDL("SIMPLE-01", xml)

        val tmpFile        = File.createTempFile("fileWriterTest", "test")
        val targetFilePath = tmpFile.getAbsolutePath.replace("\\", "/")
        tmpFile.delete()
        val con = new ConnectionInformation(new URI(s"$targetFilePath"),
                                            Option(DFASDLReference("TEST", "SIMPLE-01")))

        val fileWriter = initializeWriter(con, Option(dfasdl))
        val targetFile = new File(targetFilePath)
        val messages = new WriteBatchData(
          List(
            new WriteData(number = 1,
                          data = 1,
                          options = List((AttributeNames.STOP_SIGN, ",")),
                          metaData = Option(WriterMessageMetaData(id = "ID"))),
            new WriteData(number = 2,
                          data = "Max Mustermann",
                          options = List((AttributeNames.STOP_SIGN, ",")),
                          metaData = Option(WriterMessageMetaData(id = "ID"))),
            new WriteData(
              number = 3,
              data =
                "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua.",
              options = List((AttributeNames.STOP_SIGN, ",")),
              metaData = Option(WriterMessageMetaData(id = "ID"))
            ),
            new WriteData(number = 4,
                          data = java.sql.Date.valueOf("1923-04-01"),
                          options = List((AttributeNames.STOP_SIGN, ",")),
                          metaData = Option(WriterMessageMetaData(id = "ID"))),
            new WriteData(number = 5,
                          data = new BigDecimal("2345.67"),
                          options = List((AttributeNames.STOP_SIGN, "\r\n")),
                          metaData = Option(WriterMessageMetaData(id = "ID")))
          )
        )
        fileWriter ! messages
        fileWriter ! BaseWriterMessages.CloseWriter
        expectMsg(BaseWriterMessages.WriterClosed("".right[String]))

        val expectedContent =
          "1,Max Mustermann,Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua.,1923-04-01,2345.67\r\n"
        val actualContent = scala.io.Source.fromFile(targetFile).mkString

        actualContent shouldEqual expectedContent

        targetFile.delete()
      }
    }
  }
}
