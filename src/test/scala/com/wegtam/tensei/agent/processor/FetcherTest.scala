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

import akka.testkit.TestActorRef
import com.wegtam.tensei.adt.{
  AtomicTransformationDescription,
  DFASDL,
  ElementReference,
  TransformerOptions
}
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages.SaveData
import com.wegtam.tensei.agent.helpers.GenericHelpers
import com.wegtam.tensei.agent.processor.Fetcher.FetcherMessages
import com.wegtam.tensei.agent.{ ActorSpec, DataTreeDocument, XmlTestHelpers }
import com.wegtam.tensei.agent.adt.ParserDataContainer

class FetcherTest extends ActorSpec with XmlTestHelpers with GenericHelpers {
  describe("Fetcher") {
    val agentRunIdentifier = Option("Fetcher-TEST")

    describe("for a simple data document") {
      it("should fetch data and apply transformations") {
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

        val transformations = List(
          AtomicTransformationDescription(
            element = ElementReference(dfasdl.id, "MY-DATA"),
            transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
            options =
              TransformerOptions(classOf[String], classOf[String], List(("perform", "reduce")))
          ),
          AtomicTransformationDescription(
            element = ElementReference(dfasdl.id, "MY-DATA"),
            transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.TimestampAdjuster",
            options = TransformerOptions(classOf[String], classOf[String], List(("perform", "add")))
          ),
          AtomicTransformationDescription(
            element = ElementReference(dfasdl.id, "MY-DATA"),
            transformerClassName = "com.wegtam.tensei.agent.transformers.atomic.BoxDataIntoList",
            options = TransformerOptions(classOf[String], classOf[String])
          )
        )

        val doc = createTestDocumentBuilder().parse(
          new ByteArrayInputStream(xml.getBytes(Charset.defaultCharset()))
        )
        val fetcher = TestActorRef(Fetcher.props(agentRunIdentifier))

        fetcher ! FetcherMessages.AreRoutersInitialised
        expectMsg(FetcherMessages.RoutersNotInitialised)

        fetcher ! FetcherMessages.InitialiseRouters(List(dataTreeDocument))
        expectMsg(FetcherMessages.RoutersInitialised)

        fetcher ! FetcherMessages.FetchData(
          element = doc.getElementById("MY-DATA"),
          sourceRef = dataTreeDocument,
          transformations = transformations,
          locator = FetchDataLocator(None, None, None)
        )

        val expectedResponse = ParserDataContainer(
          data = List(1449576653000L),
          elementId = "MY-DATA",
          dfasdlId = Option("ID"),
          sequenceRowCounter = -1L,
          dataElementHash =
            Option(calculateDataElementStorageHash("MY-DATA", List.empty[(String, Long)]))
        )
        val response = expectMsgType[ParserDataContainer]

        response should be(expectedResponse)
      }
    }
  }
}
