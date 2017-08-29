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

import akka.testkit.{ EventFilter, TestActorRef }
import akka.util.ByteString
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages.SaveData
import com.wegtam.tensei.agent.adt.ParserDataContainer
import com.wegtam.tensei.agent.helpers.GenericHelpers
import com.wegtam.tensei.agent.processor.RecipeWorker.RecipeWorkerMessages
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages.WriteData
import com.wegtam.tensei.agent.{ ActorSpec, DataTreeDocument, XmlTestHelpers }
import org.dfasdl.utils.ElementHelpers
import org.w3c.dom.traversal.{ DocumentTraversal, NodeFilter }

class RecipeWorkerTest
    extends ActorSpec
    with GenericHelpers
    with XmlTestHelpers
    with ElementHelpers {
  describe("RecipeWorker") {
    val agentRunIdentifier = Option("RecipeWorker-TEST")

    describe("using a simple recipe") {
      it("should process the recipe correctly") {
        val xml =
          """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><str id="DATA-1"/><str id="DATA-2"/><str id="DATA-3"/></dfasdl>"""
        val dfasdl = DFASDL("ID", xml)
        val data1 = ParserDataContainer(
          "John",
          "DATA-1",
          Option("ID"),
          -1L,
          Option(calculateDataElementStorageHash("DATA-1", List.empty[(String, Long)]))
        )
        val data2 = ParserDataContainer(
          "Doe",
          "DATA-2",
          Option("ID"),
          -1L,
          Option(calculateDataElementStorageHash("DATA-2", List.empty[(String, Long)]))
        )

        val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))
        dataTreeDocument ! SaveData(data1,
                                    calculateDataElementStorageHash(data1.elementId,
                                                                    List.empty[(String, Long)]))
        dataTreeDocument ! SaveData(data2,
                                    calculateDataElementStorageHash(data2.elementId,
                                                                    List.empty[(String, Long)]))

        val atomicTransformations = List.empty[AtomicTransformationDescription]
        val transformations = List(
          TransformationDescription(
            transformerClassName = "com.wegtam.tensei.agent.transformers.LowerOrUpper",
            options =
              TransformerOptions(classOf[String], classOf[String], List(("perform", "upper")))
          ),
          TransformationDescription(
            transformerClassName = "com.wegtam.tensei.agent.transformers.Concat",
            options = TransformerOptions(classOf[String], classOf[String], List(("separator", " ")))
          )
        )

        val mt = MappingTransformation(
          sources =
            List(ElementReference(dfasdl.id, "DATA-1"), ElementReference(dfasdl.id, "DATA-2")),
          targets = List(ElementReference(dfasdl.id, "DATA-3")),
          transformations = transformations,
          atomicTransformations = atomicTransformations
        )

        val recipe = Recipe(
          id = "TEST-RECIPE",
          mode = Recipe.MapAllToAll,
          mappings = List(mt)
        )

        val doc = createTestDocumentBuilder().parse(
          new ByteArrayInputStream(xml.getBytes(Charset.defaultCharset()))
        )
        val tr = doc.asInstanceOf[DocumentTraversal]
        val walker = tr.createTreeWalker(doc.getDocumentElement,
                                         NodeFilter.SHOW_ELEMENT,
                                         new DataElementFilter(),
                                         true)
        val worker = TestActorRef(RecipeWorker.props(agentRunIdentifier, recipe))

        EventFilter.warning(start = "unhandled message",
                            pattern = "RecipeProcessed\\(1,0\\)",
                            occurrences = 1) intercept {
          worker ! RecipeWorkerMessages.Start(
            lastWriterMessageNumber = 0L,
            sourceDataTrees = List(
              SourceDataTreeListEntry(dfasdlId = dfasdl.id,
                                      document = Option(doc),
                                      actorRef = dataTreeDocument)
            ),
            targetDfasdl = dfasdl,
            targetTreeWalker = Option(walker),
            writer = Option(self)
          )

          val m1 = expectMsgType[WriteData]
          m1.number should be(1L)
          m1.metaData.get.id should be("DATA-3")
          m1.data should be(ByteString("JOHN DOE"))
        }
      }
    }

    describe("using a complex recipe") {
      it("should process the recipe correctly") {
        val xml =
          """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><str id="DATA-1"/><str id="DATA-2"/><str id="DATA-3"/><num id="DATA-4"/><num id="DATA-5"/></dfasdl>"""
        val dfasdl = DFASDL("ID", xml)
        val data1 = ParserDataContainer(
          "John",
          "DATA-1",
          Option("ID"),
          -1L,
          Option(calculateDataElementStorageHash("DATA-1", List.empty[(String, Long)]))
        )
        val data2 = ParserDataContainer(
          "Doe",
          "DATA-2",
          Option("ID"),
          -1L,
          Option(calculateDataElementStorageHash("DATA-2", List.empty[(String, Long)]))
        )
        val data4 = ParserDataContainer(
          1449576653946L,
          "DATA-4",
          Option("ID"),
          -1L,
          Option(calculateDataElementStorageHash("DATA-4", List.empty[(String, Long)]))
        )
        val data5 = ParserDataContainer(
          1449576655060L,
          "DATA-5",
          Option("ID"),
          -1L,
          Option(calculateDataElementStorageHash("DATA-5", List.empty[(String, Long)]))
        )

        val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))
        dataTreeDocument ! SaveData(data1,
                                    calculateDataElementStorageHash(data1.elementId,
                                                                    List.empty[(String, Long)]))
        dataTreeDocument ! SaveData(data2,
                                    calculateDataElementStorageHash(data2.elementId,
                                                                    List.empty[(String, Long)]))
        dataTreeDocument ! SaveData(data4,
                                    calculateDataElementStorageHash(data4.elementId,
                                                                    List.empty[(String, Long)]))
        dataTreeDocument ! SaveData(data5,
                                    calculateDataElementStorageHash(data5.elementId,
                                                                    List.empty[(String, Long)]))

        val mapName = MappingTransformation(
          sources =
            List(ElementReference(dfasdl.id, "DATA-1"), ElementReference(dfasdl.id, "DATA-2")),
          targets = List(ElementReference(dfasdl.id, "DATA-3")),
          transformations = List(
            TransformationDescription(
              transformerClassName = "com.wegtam.tensei.agent.transformers.LowerOrUpper",
              options =
                TransformerOptions(classOf[String], classOf[String], List(("perform", "upper")))
            ),
            TransformationDescription(
              transformerClassName = "com.wegtam.tensei.agent.transformers.Concat",
              options =
                TransformerOptions(classOf[String], classOf[String], List(("separator", " ")))
            )
          ),
          atomicTransformations = List.empty[AtomicTransformationDescription]
        )
        val mapTime = MappingTransformation(
          sources =
            List(ElementReference(dfasdl.id, "DATA-4"), ElementReference(dfasdl.id, "DATA-5")),
          targets = List(ElementReference(dfasdl.id, "DATA-4")),
          transformations = List(
            TransformationDescription(
              transformerClassName = "com.wegtam.tensei.agent.transformers.TimestampCalibrate",
              options =
                TransformerOptions(classOf[String], classOf[String], List(("perform", "add")))
            ),
            TransformationDescription(
              transformerClassName = "com.wegtam.tensei.agent.transformers.ExtractBiggestValue",
              options = TransformerOptions(classOf[String], classOf[String])
            )
          ),
          atomicTransformations = List(
            AtomicTransformationDescription(
              element = ElementReference(dfasdl.id, "DATA-4"),
              transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
              options =
                TransformerOptions(classOf[String], classOf[String], List(("perform", "reduce")))
            ),
            AtomicTransformationDescription(
              element = ElementReference(dfasdl.id, "DATA-5"),
              transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
              options =
                TransformerOptions(classOf[String], classOf[String], List(("perform", "reduce")))
            )
          )
        )

        val recipe = Recipe(
          id = "TEST-RECIPE",
          mode = Recipe.MapAllToAll,
          mappings = List(mapName, mapTime)
        )

        val doc = createTestDocumentBuilder().parse(
          new ByteArrayInputStream(xml.getBytes(Charset.defaultCharset()))
        )
        val tr = doc.asInstanceOf[DocumentTraversal]
        val walker = tr.createTreeWalker(doc.getDocumentElement,
                                         NodeFilter.SHOW_ELEMENT,
                                         new DataElementFilter(),
                                         true)
        val worker = TestActorRef(RecipeWorker.props(agentRunIdentifier, recipe))

        EventFilter.warning(start = "unhandled message",
                            pattern = "RecipeProcessed\\(2,0\\)",
                            occurrences = 1) intercept {
          worker ! RecipeWorkerMessages.Start(
            lastWriterMessageNumber = 0L,
            sourceDataTrees = List(
              SourceDataTreeListEntry(dfasdlId = dfasdl.id,
                                      document = Option(doc),
                                      actorRef = dataTreeDocument)
            ),
            targetDfasdl = dfasdl,
            targetTreeWalker = Option(walker),
            writer = Option(self)
          )

          val m1 = expectMsgType[WriteData]
          m1.number should be(1L)
          m1.metaData.get.id should be("DATA-3")
          m1.data should be(ByteString("JOHN DOE"))
          val m2 = expectMsgType[WriteData]
          m2.number should be(2L)
          m2.metaData.get.id should be("DATA-4")
          m2.data should be(1449576655000L)
        }
      }
    }
  }
}
