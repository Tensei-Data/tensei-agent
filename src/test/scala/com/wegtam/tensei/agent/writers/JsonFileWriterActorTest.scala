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

import java.io.File
import java.math.BigDecimal
import java.net.URI

import argonaut._

import akka.testkit.TestFSMRef
import com.wegtam.tensei.adt.{ ConnectionInformation, DFASDL }
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.writers.BaseWriter.{ BaseWriterMessages, WriterMessageMetaData }
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages.{
  AreYouReady,
  ReadyToWork,
  WriteData
}
import com.wegtam.tensei.agent.writers.JsonFileWriterActor.JsonFileWriterData
import org.dfasdl.utils.ElementHelpers

import scalaz._, Scalaz._

class JsonFileWriterActorTest extends ActorSpec with ElementHelpers {
  private def initializeWriter(
      con: ConnectionInformation,
      dfasdl: Option[DFASDL] = None
  ): TestFSMRef[BaseWriter.State, JsonFileWriterData, JsonFileWriterActor] = {
    val targetDfasdl = dfasdl.getOrElse(DFASDL("TEST-DFASDL", ""))
    val writer = TestFSMRef(
      new JsonFileWriterActor(con, targetDfasdl, Option("JsonFileWriterTest"))
    )
    writer.stateName should be(BaseWriter.State.Initializing)
    writer ! BaseWriterMessages.InitializeTarget
    writer ! AreYouReady
    val expectedMsg = ReadyToWork
    expectMsg(expectedMsg)
    writer
  }

  describe("JsonFileWriterActor") {
    describe("buffer ready to work requests") {
      it("should answer after it has initialized") {
        val tmpFile        = File.createTempFile("jsonFileWriterTest", "test")
        val targetFilePath = tmpFile.getAbsolutePath.replace("\\", "/")
        tmpFile.delete()
        val con = new ConnectionInformation(new URI(s"$targetFilePath"), None)
        val writer = TestFSMRef(
          new JsonFileWriterActor(con, DFASDL("TEST", ""), Option("JsonFileWriterTest"))
        )
        writer.stateName should be(BaseWriter.State.Initializing)
        writer ! AreYouReady
        writer ! BaseWriterMessages.InitializeTarget
        val expectedMsg = ReadyToWork
        expectMsg(expectedMsg)
      }
    }

    describe("initialize") {
      it("should create an empty file") {
        val tmpFile        = File.createTempFile("jsonFileWriterTest", "test")
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
        val tmpFile        = File.createTempFile("jsonFileWriterTest", "test")
        val targetFilePath = tmpFile.getAbsolutePath.replace("\\", "/")
        tmpFile.delete()
        val con = new ConnectionInformation(new URI(s"$targetFilePath"), None)
        val dfasdlContent = scala.io.Source
          .fromInputStream(
            getClass
              .getResourceAsStream("/com/wegtam/tensei/agent/writers/JsonFileWriter/simple-01.xml")
          )
          .mkString

        val dfasdl = DFASDL(
          id = "Json-Simple-01",
          content = dfasdlContent
        )
        val fileWriter = initializeWriter(con, Option(dfasdl))
        val targetFile = new File(targetFilePath)
        fileWriter ! new WriteData(number = 1,
                                   data = "Max Mustermann",
                                   metaData = Option(WriterMessageMetaData(id = "name-value")))
        fileWriter ! BaseWriterMessages.CloseWriter
        expectMsg(BaseWriterMessages.WriterClosed("".right[String]))

        val expectedContent = scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/writers/JsonFileWriter/simple-01.json"
            )
          )
          .mkString
        val actualContent = scala.io.Source.fromFile(targetFile).mkString

        expectedContent shouldEqual actualContent

        targetFile.delete()
      }
    }

    describe("when receiving multiple write requests for complex data") {
      it("should write the data to the file") {
        val tmpFile        = File.createTempFile("jsonFileWriterTest", "test")
        val targetFilePath = tmpFile.getAbsolutePath.replace("\\", "/")
        tmpFile.delete()
        val con = new ConnectionInformation(new URI(s"$targetFilePath"), None)
        val dfasdlContent = scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/writers/JsonFileWriter/complex-01.xml"
            )
          )
          .mkString

        val dfasdl = DFASDL(
          id = "Json-Complex-01",
          content = dfasdlContent
        )
        val fileWriter = initializeWriter(con, Option(dfasdl))
        val targetFile = new File(targetFilePath)
        val messages = List(
          new WriteData(number = 1,
                        data = "Musterstreet",
                        metaData = Option(WriterMessageMetaData(id = "house-street"))),
          new WriteData(number = 2,
                        data = "3",
                        metaData = Option(WriterMessageMetaData(id = "house-number"))),
          new WriteData(number = 3,
                        data = 7L,
                        metaData = Option(WriterMessageMetaData(id = "house-apartments"))),
          new WriteData(number = 4,
                        data = new BigDecimal("2300000.00"),
                        metaData = Option(WriterMessageMetaData(id = "house-value"))),
          new WriteData(
            number = 5,
            data = 15L,
            metaData = Option(WriterMessageMetaData(id = "house-size-seq-row-element"))
          ),
          new WriteData(
            number = 6,
            data = 30L,
            metaData = Option(WriterMessageMetaData(id = "house-size-seq-row-element"))
          ),
          new WriteData(
            number = 7,
            data = 45L,
            metaData = Option(WriterMessageMetaData(id = "house-size-seq-row-element"))
          ),
          new WriteData(number = 8,
                        data = new BigDecimal("15345.55"),
                        metaData = Option(WriterMessageMetaData(id = "house-costs"))),
          new WriteData(
            number = 9,
            data = "Max",
            metaData = Option(WriterMessageMetaData(id = "persons-seq-row-firstname"))
          ),
          new WriteData(number = 10,
                        data = "Mustermann",
                        metaData = Option(WriterMessageMetaData(id = "persons-seq-row-lastname"))),
          new WriteData(number = 11,
                        data = java.sql.Date.valueOf("1997-03-21"),
                        metaData = Option(WriterMessageMetaData(id = "persons-seq-row-birthday"))),
          new WriteData(
            number = 12,
            data = "0176123456",
            metaData = Option(WriterMessageMetaData(id = "persons-seq-row-telephone"))
          ),
          new WriteData(
            number = 13,
            data = 2L,
            metaData = Option(WriterMessageMetaData(id = "persons-seq-row-apartment"))
          ),
          new WriteData(number = 14,
                        data = java.sql.Timestamp.valueOf("2015-11-02 12:34:55"),
                        metaData = Option(WriterMessageMetaData(id = "persons-seq-row-lastpay"))),
          new WriteData(
            number = 15,
            data = "parking slot",
            metaData = Option(WriterMessageMetaData(id = "persons-seq-row-other-seq-row-element"))
          ),
          new WriteData(
            number = 16,
            data = "extra room",
            metaData = Option(WriterMessageMetaData(id = "persons-seq-row-other-seq-row-element"))
          ),
          new WriteData(
            number = 17,
            data = "Eva",
            metaData = Option(WriterMessageMetaData(id = "persons-seq-row-firstname"))
          ),
          new WriteData(number = 18,
                        data = "Musterfrau",
                        metaData = Option(WriterMessageMetaData(id = "persons-seq-row-lastname"))),
          new WriteData(number = 19,
                        data = java.sql.Date.valueOf("1997-04-01"),
                        metaData = Option(WriterMessageMetaData(id = "persons-seq-row-birthday"))),
          new WriteData(
            number = 20,
            data = "0176987654321",
            metaData = Option(WriterMessageMetaData(id = "persons-seq-row-telephone"))
          ),
          new WriteData(
            number = 21,
            data = 4L,
            metaData = Option(WriterMessageMetaData(id = "persons-seq-row-apartment"))
          ),
          new WriteData(number = 22,
                        data = java.sql.Timestamp.valueOf("2015-11-01 12:34:55"),
                        metaData = Option(WriterMessageMetaData(id = "persons-seq-row-lastpay"))),
          new WriteData(
            number = 23,
            data = "extra room",
            metaData = Option(WriterMessageMetaData(id = "persons-seq-row-other-seq-row-element"))
          )
        )
        messages.foreach(m => fileWriter ! m)
        fileWriter ! BaseWriterMessages.CloseWriter
        expectMsg(BaseWriterMessages.WriterClosed("".right[String]))

        val expectedContent = scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/writers/JsonFileWriter/complex-01.json"
            )
          )
          .mkString
        val actualContent = scala.io.Source.fromFile(targetFile).mkString

        Parse.parse(expectedContent) match {
          case -\/(f) => fail(s"Unable to parse expected content: $f")
          case \/-(ec) =>
            Parse.parse(actualContent) match {
              case -\/(fe) =>
                println(actualContent)
                fail(s"Unable to parse generated json file: $fe")
              case \/-(ac) =>
                ac.spaces2 shouldEqual ec.spaces2
            }
        }

        targetFile.delete()
      }
    }
  }
}
