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
import com.wegtam.tensei.agent.processor.Fetcher.FetcherMessages
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages.WriteData
import com.wegtam.tensei.agent.writers.BaseWriter.WriterMessageMetaData
import com.wegtam.tensei.agent.{ ActorSpec, DataTreeDocument, XmlTestHelpers }
import org.dfasdl.utils.ElementHelpers
import org.w3c.dom.traversal.{ DocumentTraversal, NodeFilter }

class MappingOneToOneWorkerTest
    extends ActorSpec
    with XmlTestHelpers
    with GenericHelpers
    with ElementHelpers {
  describe("MappingOneToOneWorker") {
    val agentRunIdentifier = Option("MappingOneToOneWorker-TEST")

    describe("for a simple mapping") {
      it("should map all fields correctly") {
        val xml =
          """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><num id="MY-DATA"/></dfasdl>"""
        val dfasdl = DFASDL("ID", xml)
        val data = ParserDataContainer(
          1449576653946L,
          "MY-DATA",
          Option("ID"),
          -1L,
          Option(calculateDataElementStorageHash("MY-DATA", List.empty[(String, Long)]))
        )

        val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))
        dataTreeDocument ! SaveData(data,
                                    calculateDataElementStorageHash(data.elementId,
                                                                    List.empty[(String, Long)]))

        val atomicTransformations = List(
          AtomicTransformationDescription(
            element = ElementReference(dfasdl.id, "MY-DATA"),
            transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
            options =
              TransformerOptions(classOf[String], classOf[String], List(("perform", "reduce")))
          )
        )
        val transformations = List(
          TransformationDescription(
            transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
            options = TransformerOptions(classOf[String], classOf[String], List(("perform", "add")))
          )
        )

        val doc = createTestDocumentBuilder().parse(
          new ByteArrayInputStream(xml.getBytes(Charset.defaultCharset()))
        )
        val tr = doc.asInstanceOf[DocumentTraversal]
        val walker = tr.createTreeWalker(doc.getDocumentElement,
                                         NodeFilter.SHOW_ELEMENT,
                                         new DataElementFilter(),
                                         true)
        val fetcher = TestActorRef(Fetcher.props(agentRunIdentifier))

        fetcher ! FetcherMessages.AreRoutersInitialised
        expectMsg(FetcherMessages.RoutersNotInitialised)

        fetcher ! FetcherMessages.InitialiseRouters(List(dataTreeDocument))
        expectMsg(FetcherMessages.RoutersInitialised)

        val mapper = TestActorRef(
          MappingOneToOneWorker.props(
            agentRunIdentifier = agentRunIdentifier,
            fetcher = fetcher,
            sequenceRow = None,
            sourceDataTrees = List(
              SourceDataTreeListEntry(dfasdlId = dfasdl.id,
                                      document = Option(doc),
                                      actorRef = dataTreeDocument)
            ),
            targetDfasdl = dfasdl,
            targetTree = doc,
            targetTreeWalker = walker,
            writer = self,
            maxLoops = 0L
          )
        )

        val mt = MappingTransformation(
          sources = List(ElementReference(dfasdl.id, "MY-DATA")),
          targets = List(ElementReference(dfasdl.id, "MY-DATA")),
          transformations = transformations,
          atomicTransformations = atomicTransformations
        )

        val expectedData = ParserDataContainer(
          data = 1449576653000L,
          elementId = "MY-DATA",
          dfasdlId = Option("ID"),
          sequenceRowCounter = -1L,
          dataElementHash =
            Option(calculateDataElementStorageHash("MY-DATA", List.empty[(String, Long)]))
        )
        val expectedWriterMessage = WriteData(
          number = 1L,
          data = expectedData.data,
          metaData =
            Option(WriterMessageMetaData(id = expectedData.elementId, charset = Option("utf-8")))
        )

        // A mapping processed message must be send from the mapping worker!
        EventFilter.warning(start = "unhandled message",
                            pattern = "MappingProcessed\\(1\\)",
                            occurrences = 1) intercept {
          mapper ! MapperMessages.ProcessMapping(
            mapping = mt,
            lastWriterMessageNumber = 0L,
            recipeMode = Recipe.MapOneToOne,
            maxLoops = 0L,
            sequenceRow = None
          )

          // The writer must receive the correct data!
          val response = expectMsgType[WriteData]
          response should be(expectedWriterMessage)
        }
      }
    }

    describe("for a simple mapping of multiple fields") {
      it("should map all fields correctly") {
        val xml =
          """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><num id="MY-DATA"/><num id="DATA-2"/></dfasdl>"""
        val dfasdl = DFASDL("ID", xml)
        val data1 = ParserDataContainer(
          1449576653946L,
          "MY-DATA",
          Option("ID"),
          -1L,
          Option(calculateDataElementStorageHash("MY-DATA", List.empty[(String, Long)]))
        )
        val data2 = ParserDataContainer(
          1450190338L,
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

        val atomicTransformations = List(
          AtomicTransformationDescription(
            element = ElementReference(dfasdl.id, "MY-DATA"),
            transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
            options =
              TransformerOptions(classOf[String], classOf[String], List(("perform", "reduce")))
          )
        )
        val transformations = List(
          TransformationDescription(
            transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
            options = TransformerOptions(classOf[String], classOf[String], List(("perform", "add")))
          )
        )

        val doc = createTestDocumentBuilder().parse(
          new ByteArrayInputStream(xml.getBytes(Charset.defaultCharset()))
        )
        val tr = doc.asInstanceOf[DocumentTraversal]
        val walker = tr.createTreeWalker(doc.getDocumentElement,
                                         NodeFilter.SHOW_ELEMENT,
                                         new DataElementFilter(),
                                         true)
        val fetcher = TestActorRef(Fetcher.props(agentRunIdentifier))

        fetcher ! FetcherMessages.AreRoutersInitialised
        expectMsg(FetcherMessages.RoutersNotInitialised)

        fetcher ! FetcherMessages.InitialiseRouters(List(dataTreeDocument))
        expectMsg(FetcherMessages.RoutersInitialised)

        val mapper = TestActorRef(
          MappingOneToOneWorker.props(
            agentRunIdentifier = agentRunIdentifier,
            fetcher = fetcher,
            sequenceRow = None,
            sourceDataTrees = List(
              SourceDataTreeListEntry(dfasdlId = dfasdl.id,
                                      document = Option(doc),
                                      actorRef = dataTreeDocument)
            ),
            targetDfasdl = dfasdl,
            targetTree = doc,
            targetTreeWalker = walker,
            writer = self,
            maxLoops = 0L
          )
        )

        val mt = MappingTransformation(
          sources =
            List(ElementReference(dfasdl.id, "MY-DATA"), ElementReference(dfasdl.id, "DATA-2")),
          targets =
            List(ElementReference(dfasdl.id, "DATA-2"), ElementReference(dfasdl.id, "MY-DATA")),
          transformations = transformations,
          atomicTransformations = atomicTransformations
        )

        // A mapping processed message must be send from the mapping worker!
        EventFilter.warning(start = "unhandled message",
                            pattern = "MappingProcessed\\(2\\)",
                            occurrences = 1) intercept {
          mapper ! MapperMessages.ProcessMapping(
            mapping = mt,
            lastWriterMessageNumber = 0L,
            recipeMode = Recipe.MapOneToOne,
            maxLoops = 0L,
            sequenceRow = None
          )

          val m1 = expectMsgType[WriteData]
          m1.number should be(1L)
          m1.metaData.get.id should be("DATA-2")
          m1.data should be(1449576653000L)
          val m2 = expectMsgType[WriteData]
          m2.number should be(2L)
          m2.metaData.get.id should be("MY-DATA")
          m2.data should be(1450190338000L)
        }
      }
    }

    describe("for a simple mapping of multiple fields including the same field multiple times") {
      it("should map all fields correctly") {
        val xml =
          """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><num id="MY-DATA"/><num id="DATA-2"/><str id="DATA-3"/></dfasdl>"""
        val dfasdl = DFASDL("ID", xml)
        val data1 = ParserDataContainer(
          1449576653946L,
          "MY-DATA",
          Option("ID"),
          -1L,
          Option(calculateDataElementStorageHash("MY-DATA", List.empty[(String, Long)]))
        )
        val data2 = ParserDataContainer(
          1450190338L,
          "DATA-2",
          Option("ID"),
          -1L,
          Option(calculateDataElementStorageHash("DATA-2", List.empty[(String, Long)]))
        )
        val data3 = ParserDataContainer(
          "Some test string...",
          "DATA-3",
          Option("ID"),
          -1L,
          Option(calculateDataElementStorageHash("DATA-3", List.empty[(String, Long)]))
        )

        val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))
        dataTreeDocument ! SaveData(data1,
                                    calculateDataElementStorageHash(data1.elementId,
                                                                    List.empty[(String, Long)]))
        dataTreeDocument ! SaveData(data2,
                                    calculateDataElementStorageHash(data2.elementId,
                                                                    List.empty[(String, Long)]))
        dataTreeDocument ! SaveData(data3,
                                    calculateDataElementStorageHash(data3.elementId,
                                                                    List.empty[(String, Long)]))

        val atomicTransformations = List(
          AtomicTransformationDescription(
            element = ElementReference(dfasdl.id, "MY-DATA"),
            transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
            options =
              TransformerOptions(classOf[String], classOf[String], List(("perform", "reduce")))
          )
        )
        val transformations = List(
          TransformationDescription(
            transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
            options = TransformerOptions(classOf[String], classOf[String], List(("perform", "add")))
          )
        )

        val doc = createTestDocumentBuilder().parse(
          new ByteArrayInputStream(xml.getBytes(Charset.defaultCharset()))
        )
        val tr = doc.asInstanceOf[DocumentTraversal]
        val walker = tr.createTreeWalker(doc.getDocumentElement,
                                         NodeFilter.SHOW_ELEMENT,
                                         new DataElementFilter(),
                                         true)
        val fetcher = TestActorRef(Fetcher.props(agentRunIdentifier))

        fetcher ! FetcherMessages.AreRoutersInitialised
        expectMsg(FetcherMessages.RoutersNotInitialised)

        fetcher ! FetcherMessages.InitialiseRouters(List(dataTreeDocument))
        expectMsg(FetcherMessages.RoutersInitialised)

        val mapper = TestActorRef(
          MappingOneToOneWorker.props(
            agentRunIdentifier = agentRunIdentifier,
            fetcher = fetcher,
            sequenceRow = None,
            sourceDataTrees = List(
              SourceDataTreeListEntry(dfasdlId = dfasdl.id,
                                      document = Option(doc),
                                      actorRef = dataTreeDocument)
            ),
            targetDfasdl = dfasdl,
            targetTree = doc,
            targetTreeWalker = walker,
            writer = self,
            maxLoops = 0L
          )
        )

        val mt = MappingTransformation(
          sources = List(ElementReference(dfasdl.id, "MY-DATA"),
                         ElementReference(dfasdl.id, "DATA-2"),
                         ElementReference(dfasdl.id, "DATA-2")),
          targets = List(ElementReference(dfasdl.id, "DATA-2"),
                         ElementReference(dfasdl.id, "MY-DATA"),
                         ElementReference(dfasdl.id, "DATA-3")),
          transformations = transformations,
          atomicTransformations = atomicTransformations
        )

        // A mapping processed message must be send from the mapping worker!
        EventFilter.warning(start = "unhandled message",
                            pattern = "MappingProcessed\\(3\\)",
                            occurrences = 1) intercept {
          mapper ! MapperMessages.ProcessMapping(
            mapping = mt,
            lastWriterMessageNumber = 0L,
            recipeMode = Recipe.MapOneToOne,
            maxLoops = 0L,
            sequenceRow = None
          )

          val m1 = expectMsgType[WriteData]
          m1.number should be(1L)
          m1.metaData.get.id should be("DATA-2")
          m1.data should be(1449576653000L)
          val m2 = expectMsgType[WriteData]
          m2.number should be(2L)
          m2.metaData.get.id should be("MY-DATA")
          m2.data should be(1450190338000L)
          val m3 = expectMsgType[WriteData]
          m3.number should be(3L)
          m3.metaData.get.id should be("DATA-3")
          m3.data should be(ByteString("1450190338000"))
        }
      }
    }
  }
}
