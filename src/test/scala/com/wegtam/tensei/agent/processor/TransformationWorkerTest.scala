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

package com.wegtam.tensei.agent.processor

import java.io.ByteArrayInputStream
import java.nio.charset.Charset

import akka.testkit.{ EventFilter, TestFSMRef }
import com.wegtam.tensei.adt.{ TransformationDescription, TransformerOptions }
import com.wegtam.tensei.agent.adt.ParserDataContainer
import com.wegtam.tensei.agent.processor.TransformationWorker.{
  TransformationWorkerMessages,
  TransformationWorkerState
}
import com.wegtam.tensei.agent.{ ActorSpec, XmlTestHelpers }

class TransformationWorkerTest extends ActorSpec with XmlTestHelpers {
  describe("TransformationWorker") {
    val agentRunIdentifier = Option("TransformationWorker-TEST")

    describe("given no transformations") {
      it("should return the data immediately") {
        val actor = TestFSMRef(new TransformationWorker(agentRunIdentifier))

        actor.stateName should be(TransformationWorkerState.Idle)

        val dfasdl =
          """
            |<dfasdl xmlns="http://www.dfasdl.org/DFASDL"
            |        default-encoding="utf-8" semantic="niem">
            |  <elem id="row">
            |    <str id="ELEMENT-ID"/>
            |  </elem>
            |</dfasdl>
          """.stripMargin
        val doc = createTestDocumentBuilder().parse(
          new ByteArrayInputStream(dfasdl.getBytes(Charset.defaultCharset()))
        )
        val data = ParserDataContainer("I am test data!", "ELEMENT-ID")
        actor ! TransformationWorkerMessages.Start(data,
                                                   doc.getElementById(data.elementId),
                                                   self,
                                                   List.empty[TransformationDescription])
        val c = expectMsgType[ParserDataContainer]
        c.data should be(data.data)
      }
    }

    describe("given a single transformation") {
      it("should apply the transformation") {
        val actor = TestFSMRef(new TransformationWorker(agentRunIdentifier))

        actor.stateName should be(TransformationWorkerState.Idle)

        val dfasdl =
          """
            |<dfasdl xmlns="http://www.dfasdl.org/DFASDL"
            |        default-encoding="utf-8" semantic="niem">
            |  <elem id="row">
            |    <str id="ELEMENT-ID"/>
            |  </elem>
            |</dfasdl>
          """.stripMargin
        val doc = createTestDocumentBuilder().parse(
          new ByteArrayInputStream(dfasdl.getBytes(Charset.defaultCharset()))
        )
        val data         = ParserDataContainer(1449499335L, "ELEMENT-ID")
        val expectedData = ParserDataContainer(1449499335000L, "ELEMENT-ID")
        actor ! TransformationWorkerMessages.Start(
          data,
          doc.getElementById(data.elementId),
          self,
          List(
            TransformationDescription(
              "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
              TransformerOptions(classOf[String], classOf[String])
            )
          )
        )
        val c = expectMsgType[ParserDataContainer]
        c.data should be(expectedData.data)
      }

      it("should log errors and stop itself") {
        val actor = TestFSMRef(new TransformationWorker(agentRunIdentifier))

        actor.stateName should be(TransformationWorkerState.Idle)

        val dfasdl =
          """
            |<dfasdl xmlns="http://www.dfasdl.org/DFASDL"
            |        default-encoding="utf-8" semantic="niem">
            |  <elem id="row">
            |    <str id="ELEMENT-ID"/>
            |  </elem>
            |</dfasdl>
          """.stripMargin
        val doc = createTestDocumentBuilder().parse(
          new ByteArrayInputStream(dfasdl.getBytes(Charset.defaultCharset()))
        )
        val data = ParserDataContainer(1449499335L, "ELEMENT-ID")
        EventFilter.debug(message = "Received stop message in transforming state.",
                          source = actor.path.toString,
                          occurrences = 1) intercept {
          EventFilter[ClassNotFoundException](source = actor.path.toString, occurrences = 1) intercept {
            actor ! TransformationWorkerMessages.Start(
              data,
              doc.getElementById(data.elementId),
              self,
              List(
                TransformationDescription("This is no class name!",
                                          TransformerOptions(classOf[String], classOf[String]))
              )
            )
          }
        }
      }
    }

    describe("given multiple transformations") {
      it("should apply all transformations") {
        val actor = TestFSMRef(new TransformationWorker(agentRunIdentifier))

        actor.stateName should be(TransformationWorkerState.Idle)

        val dfasdl =
          """
            |<dfasdl xmlns="http://www.dfasdl.org/DFASDL"
            |        default-encoding="utf-8" semantic="niem">
            |  <elem id="row">
            |    <str id="ELEMENT-ID"/>
            |  </elem>
            |</dfasdl>
          """.stripMargin
        val doc = createTestDocumentBuilder().parse(
          new ByteArrayInputStream(dfasdl.getBytes(Charset.defaultCharset()))
        )
        val data         = ParserDataContainer(1449499335060L, "ELEMENT-ID")
        val expectedData = ParserDataContainer(List(1449499335000L), "ELEMENT-ID")
        val transformations = List(
          TransformationDescription(
            transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
            options =
              TransformerOptions(classOf[String], classOf[String], List(("perform", "reduce")))
          ),
          TransformationDescription(
            transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
            options =
              TransformerOptions(classOf[String], classOf[String], List(("perform", "add")))
          ),
          TransformationDescription(
            transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.BoxDataIntoList",
            options = TransformerOptions(classOf[String], classOf[String])
          )
        )
        actor ! TransformationWorkerMessages.Start(data,
                                                   doc.getElementById(data.elementId),
                                                   self,
                                                   transformations)
        val c = expectMsgType[ParserDataContainer]
        c.data should be(expectedData.data)
      }
    }

    describe("given multiple transformations on a data list") {
      it("should apply all transformations") {
        val actor = TestFSMRef(new TransformationWorker(agentRunIdentifier))

        actor.stateName should be(TransformationWorkerState.Idle)

        val dfasdl =
          """
            |<dfasdl xmlns="http://www.dfasdl.org/DFASDL"
            |        default-encoding="utf-8" semantic="niem">
            |  <elem id="row">
            |    <str id="ELEMENT-ID"/>
            |  </elem>
            |</dfasdl>
          """.stripMargin
        val doc = createTestDocumentBuilder().parse(
          new ByteArrayInputStream(dfasdl.getBytes(Charset.defaultCharset()))
        )
        val data         = ParserDataContainer(List(1449499335060L, 1449499336060L), "ELEMENT-ID")
        val expectedData = ParserDataContainer(List(1449499335000L, 1449499336000L), "ELEMENT-ID")
        val transformations = List(
          TransformationDescription(
            transformerClassName = "com.wegtam.tensei.agent.transformers.TimestampCalibrate",
            options =
              TransformerOptions(classOf[String], classOf[String], List(("perform", "reduce")))
          ),
          TransformationDescription(
            transformerClassName = "com.wegtam.tensei.agent.transformers.TimestampCalibrate",
            options =
              TransformerOptions(classOf[String], classOf[String], List(("perform", "add")))
          )
        )
        actor ! TransformationWorkerMessages.Start(data,
                                                   doc.getElementById(data.elementId),
                                                   self,
                                                   transformations)
        val c = expectMsgType[ParserDataContainer]
        c.data should be(expectedData.data)
      }
    }
  }
}
