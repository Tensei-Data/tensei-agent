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

import akka.actor.ActorRef
import akka.testkit.{ EventFilter, TestActorRef }
import com.wegtam.tensei.adt.{ DFASDL, ElementReference }
import com.wegtam.tensei.agent.XmlActorSpec
import com.wegtam.tensei.agent.processor.UniqueValueBuffer
import com.wegtam.tensei.agent.processor.UniqueValueBuffer.UniqueValueBufferMessages
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages
import org.scalatest.BeforeAndAfterEach

import scala.collection.SortedSet

class BaseWriterFilterWorkerTest extends XmlActorSpec with BeforeAndAfterEach {
  val agentRunIdentifier                  = Option("BaseWriterFilterWorker-TEST")
  var uniqueValueBuffer: Option[ActorRef] = None

  /**
    * Stop the unique value buffer actor after each test.
    */
  override protected def afterEach(): Unit =
    uniqueValueBuffer.foreach(
      u =>
        EventFilter
          .debug(message = "stopped", occurrences = 1, source = u.path.toString) intercept system
          .stop(u)
    )

  /**
    * Initialise the unique value buffer actor before each test.
    */
  override protected def beforeEach(): Unit = {
    val u = TestActorRef(UniqueValueBuffer.props(agentRunIdentifier), "UniqueValueBuffer") // Create unique value buffer on event channel.
    uniqueValueBuffer = Option(u)
  }

  describe("BaseWriterFilterWorker") {
    it("should connect to unique value buffer at startup") {
      val dfasdl = DFASDL(id = "TEST", content = "")
      uniqueValueBuffer.fold(fail("No unique value buffer found!"))(
        u =>
          EventFilter.debug(message = s"Received handshake from unique value buffer at ${u.path}.",
                            occurrences = 1) intercept TestActorRef(
            BaseWriterFilterWorker.props(agentRunIdentifier, dfasdl)
        )
      )
    }

    describe("if not unique elements are defined") {
      it("should return the same data") {
        val dfasdl =
          DFASDL("SIMPLE-01",
                 scala.io.Source
                   .fromInputStream(
                     getClass.getResourceAsStream("/com/wegtam/tensei/agent/writers/simple-01.xml")
                   )
                   .mkString)
        val a = TestActorRef(BaseWriterFilterWorker.props(agentRunIdentifier, dfasdl))

        val d = List(
          BaseWriterMessages.WriteData(
            number = 1L,
            data = 1L,
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
          ),
          BaseWriterMessages.WriteData(
            number = 2L,
            data = "Albert Einstein",
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
          ),
          BaseWriterMessages.WriteData(
            number = 3L,
            data = None,
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
          ),
          BaseWriterMessages.WriteData(
            number = 7L,
            data = java.sql.Date.valueOf("1879-03-14"),
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
          ),
          BaseWriterMessages.WriteData(
            number = 8L,
            data = new java.math.BigDecimal("3.14"),
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
          ),
          BaseWriterMessages.WriteData(
            number = 9L,
            data = 2L,
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
          ),
          BaseWriterMessages.WriteData(
            number = 10L,
            data = "Bernhard Riemann",
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
          ),
          BaseWriterMessages.WriteData(
            number = 11L,
            data = None,
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
          ),
          BaseWriterMessages.WriteData(
            number = 12L,
            data = java.sql.Date.valueOf("1826-09-17"),
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
          ),
          BaseWriterMessages.WriteData(
            number = 13L,
            data = new java.math.BigDecimal("0.00"),
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
          ),
          BaseWriterMessages.WriteData(
            number = 14L,
            data = 3L,
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
          ),
          BaseWriterMessages.WriteData(
            number = 15L,
            data = "Johann Carl Friedrich Gauß",
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
          ),
          BaseWriterMessages.WriteData(
            number = 16L,
            data = None,
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
          ),
          BaseWriterMessages.WriteData(
            number = 17L,
            data = java.sql.Date.valueOf("1777-04-30"),
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
          ),
          BaseWriterMessages.WriteData(
            number = 18L,
            data = new java.math.BigDecimal("2.71"),
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
          ),
          BaseWriterMessages.WriteData(
            number = 19L,
            data = 4L,
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
          ),
          BaseWriterMessages.WriteData(
            number = 20L,
            data = "Johann Benedict Listing",
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
          ),
          BaseWriterMessages.WriteData(
            number = 21L,
            data = None,
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
          ),
          BaseWriterMessages.WriteData(
            number = 22L,
            data = java.sql.Date.valueOf("1808-07-25"),
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
          ),
          BaseWriterMessages.WriteData(
            number = 23L,
            data = new java.math.BigDecimal("4.2"),
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
          ),
          BaseWriterMessages.WriteData(
            number = 24L,
            data = 5L,
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
          ),
          BaseWriterMessages.WriteData(
            number = 25L,
            data = "Gottfried Wilhelm Leibnitz",
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
          ),
          BaseWriterMessages.WriteData(
            number = 26L,
            data = None,
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
          ),
          BaseWriterMessages.WriteData(
            number = 27L,
            data = java.sql.Date.valueOf("1646-07-01"),
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
          ),
          BaseWriterMessages.WriteData(
            number = 28L,
            data = new java.math.BigDecimal("1.99"),
            metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
          )
        )
        val s = SortedSet.empty[BaseWriterMessages.WriteData] ++ d

        val seqElement = createNormalizedDocument(dfasdl.content).getElementById("accounts")
        a ! BaseWriterFilterWorker.FilterSequenceRows(s, seqElement, None)
        expectMsg(BaseWriterMessages.WriteBatchData(s.toList))
        system stop a
      }
    }

    describe("if unique elements are defined") {
      describe("without saved unique values and included duplicates") {
        it("should filter data correctly") {
          val dfasdl = DFASDL("SIMPLE-01",
                              scala.io.Source
                                .fromInputStream(
                                  getClass.getResourceAsStream(
                                    "/com/wegtam/tensei/agent/writers/simple-01-unique.xml"
                                  )
                                )
                                .mkString)
          val a = TestActorRef(BaseWriterFilterWorker.props(agentRunIdentifier, dfasdl))

          val d = List(
            BaseWriterMessages.WriteData(
              number = 1L,
              data = 1L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 2L,
              data = "Albert Einstein",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 3L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 7L,
              data = java.sql.Date.valueOf("1879-03-14"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 8L,
              data = new java.math.BigDecimal("3.14"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 9L,
              data = 2L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 10L,
              data = "Bernhard Riemann",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 11L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 12L,
              data = java.sql.Date.valueOf("1826-09-17"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 13L,
              data = new java.math.BigDecimal("0.00"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 14L,
              data = 3L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 15L,
              data = "Johann Carl Friedrich Gauß",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 16L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 17L,
              data = java.sql.Date.valueOf("1777-04-30"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 18L,
              data = new java.math.BigDecimal("2.71"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 19L,
              data = 4L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 20L,
              data = "Johann Benedict Listing",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 21L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 22L,
              data = java.sql.Date.valueOf("1808-07-25"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 23L,
              data = new java.math.BigDecimal("4.2"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 24L,
              data = 5L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 25L,
              data = "Gottfried Wilhelm Leibnitz",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 26L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 27L,
              data = java.sql.Date.valueOf("1646-07-01"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 28L,
              data = new java.math.BigDecimal("1.99"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 29L,
              data = 6L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 30L,
              data = "Albert Einstein",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 31L,
              data = "Relativitätstheorie...",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 32L,
              data = java.sql.Date.valueOf("1879-03-14"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 33L,
              data = new java.math.BigDecimal("6.28"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            )
          )
          val s = SortedSet.empty[BaseWriterMessages.WriteData] ++ d
          val e = List(
            BaseWriterMessages.WriteData(
              number = 1L,
              data = 1L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 2L,
              data = "Albert Einstein",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 3L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 7L,
              data = java.sql.Date.valueOf("1879-03-14"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 8L,
              data = new java.math.BigDecimal("3.14"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 9L,
              data = 2L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 10L,
              data = "Bernhard Riemann",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 11L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 12L,
              data = java.sql.Date.valueOf("1826-09-17"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 13L,
              data = new java.math.BigDecimal("0.00"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 14L,
              data = 3L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 15L,
              data = "Johann Carl Friedrich Gauß",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 16L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 17L,
              data = java.sql.Date.valueOf("1777-04-30"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 18L,
              data = new java.math.BigDecimal("2.71"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 19L,
              data = 4L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 20L,
              data = "Johann Benedict Listing",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 21L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 22L,
              data = java.sql.Date.valueOf("1808-07-25"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 23L,
              data = new java.math.BigDecimal("4.2"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 24L,
              data = 5L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 25L,
              data = "Gottfried Wilhelm Leibnitz",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 26L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 27L,
              data = java.sql.Date.valueOf("1646-07-01"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 28L,
              data = new java.math.BigDecimal("1.99"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            )
          )
          val expected = SortedSet.empty[BaseWriterMessages.WriteData] ++ e

          val seqElement = createNormalizedDocument(dfasdl.content).getElementById("accounts")
          a ! BaseWriterFilterWorker.FilterSequenceRows(s, seqElement, None)
          expectMsg(BaseWriterMessages.WriteBatchData(expected.toList))
          system stop a
        }
      }

      describe("with saved unique values and included duplicates") {
        it("should filter data correctly") {
          val dfasdl = DFASDL("SIMPLE-01",
                              scala.io.Source
                                .fromInputStream(
                                  getClass.getResourceAsStream(
                                    "/com/wegtam/tensei/agent/writers/simple-01-unique.xml"
                                  )
                                )
                                .mkString)
          val a = TestActorRef(BaseWriterFilterWorker.props(agentRunIdentifier, dfasdl))

          val uniqueRef = ElementReference(dfasdlId = dfasdl.id, elementId = "accounts-row-name")
          EventFilter.debug(message = s"Stored unique value for element $uniqueRef.",
                            occurrences = 1) intercept uniqueValueBuffer.foreach(
            u => u ! UniqueValueBufferMessages.Store(uniqueRef, "Albert Einstein")
          )

          val d = List(
            BaseWriterMessages.WriteData(
              number = 9L,
              data = 2L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 10L,
              data = "Bernhard Riemann",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 11L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 12L,
              data = java.sql.Date.valueOf("1826-09-17"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 13L,
              data = new java.math.BigDecimal("0.00"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 14L,
              data = 3L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 15L,
              data = "Johann Carl Friedrich Gauß",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 16L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 17L,
              data = java.sql.Date.valueOf("1777-04-30"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 18L,
              data = new java.math.BigDecimal("2.71"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 19L,
              data = 4L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 20L,
              data = "Johann Benedict Listing",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 21L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 22L,
              data = java.sql.Date.valueOf("1808-07-25"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 23L,
              data = new java.math.BigDecimal("4.2"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 24L,
              data = 5L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 25L,
              data = "Gottfried Wilhelm Leibnitz",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 26L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 27L,
              data = java.sql.Date.valueOf("1646-07-01"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 28L,
              data = new java.math.BigDecimal("1.99"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 29L,
              data = 6L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 30L,
              data = "Albert Einstein",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 31L,
              data = "Relativitätstheorie...",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 32L,
              data = java.sql.Date.valueOf("1879-03-14"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 33L,
              data = new java.math.BigDecimal("6.28"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 34L,
              data = 7L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 35L,
              data = "Gottfried Wilhelm Leibnitz",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 36L,
              data = "Tralala",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 37L,
              data = java.sql.Date.valueOf("1646-07-01"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 38L,
              data = new java.math.BigDecimal("1.99"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            )
          )
          val s = SortedSet.empty[BaseWriterMessages.WriteData] ++ d
          val e = List(
            BaseWriterMessages.WriteData(
              number = 9L,
              data = 2L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 10L,
              data = "Bernhard Riemann",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 11L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 12L,
              data = java.sql.Date.valueOf("1826-09-17"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 13L,
              data = new java.math.BigDecimal("0.00"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 14L,
              data = 3L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 15L,
              data = "Johann Carl Friedrich Gauß",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 16L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 17L,
              data = java.sql.Date.valueOf("1777-04-30"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 18L,
              data = new java.math.BigDecimal("2.71"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 19L,
              data = 4L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 20L,
              data = "Johann Benedict Listing",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 21L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 22L,
              data = java.sql.Date.valueOf("1808-07-25"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 23L,
              data = new java.math.BigDecimal("4.2"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 24L,
              data = 5L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 25L,
              data = "Gottfried Wilhelm Leibnitz",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 26L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 27L,
              data = java.sql.Date.valueOf("1646-07-01"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 28L,
              data = new java.math.BigDecimal("1.99"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            )
          )
          val expected = SortedSet.empty[BaseWriterMessages.WriteData] ++ e

          val seqElement = createNormalizedDocument(dfasdl.content).getElementById("accounts")
          a ! BaseWriterFilterWorker.FilterSequenceRows(s, seqElement, None)
          expectMsg(BaseWriterMessages.WriteBatchData(expected.toList))
          system stop a
        }
      }

      describe("with saved unique values, included duplicates and incomplete rows") {
        it("should filter data correctly") {
          val dfasdl = DFASDL("SIMPLE-01",
                              scala.io.Source
                                .fromInputStream(
                                  getClass.getResourceAsStream(
                                    "/com/wegtam/tensei/agent/writers/simple-01-unique.xml"
                                  )
                                )
                                .mkString)
          val a = TestActorRef(BaseWriterFilterWorker.props(agentRunIdentifier, dfasdl))

          val uniqueRef = ElementReference(dfasdlId = dfasdl.id, elementId = "accounts-row-name")
          EventFilter.debug(message = s"Stored unique value for element $uniqueRef.",
                            occurrences = 1) intercept uniqueValueBuffer.foreach(
            u => u ! UniqueValueBufferMessages.Store(uniqueRef, "Albert Einstein")
          )

          val d = List(
            BaseWriterMessages.WriteData(
              number = 9L,
              data = 2L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 10L,
              data = "Bernhard Riemann",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 11L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 12L,
              data = java.sql.Date.valueOf("1826-09-17"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 13L,
              data = new java.math.BigDecimal("0.00"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 14L,
              data = 3L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 15L,
              data = "Johann Carl Friedrich Gauß",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 16L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 17L,
              data = java.sql.Date.valueOf("1777-04-30"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 18L,
              data = new java.math.BigDecimal("2.71"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 19L,
              data = 4L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 20L,
              data = "Johann Benedict Listing",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 21L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 22L,
              data = java.sql.Date.valueOf("1808-07-25"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 23L,
              data = new java.math.BigDecimal("4.2"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 24L,
              data = 5L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 25L,
              data = "Gottfried Wilhelm Leibnitz",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 26L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 27L,
              data = java.sql.Date.valueOf("1646-07-01"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 28L,
              data = new java.math.BigDecimal("1.99"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 29L,
              data = 6L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 30L,
              data = "Albert Einstein",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 31L,
              data = "Relativitätstheorie...",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 32L,
              data = java.sql.Date.valueOf("1879-03-14"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 33L,
              data = new java.math.BigDecimal("6.28"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 34L,
              data = 7L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 35L,
              data = "Gottfried Wilhelm Leibnitz",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            )
          )
          val s = SortedSet.empty[BaseWriterMessages.WriteData] ++ d
          val e = List(
            BaseWriterMessages.WriteData(
              number = 9L,
              data = 2L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 10L,
              data = "Bernhard Riemann",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 11L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 12L,
              data = java.sql.Date.valueOf("1826-09-17"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 13L,
              data = new java.math.BigDecimal("0.00"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 14L,
              data = 3L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 15L,
              data = "Johann Carl Friedrich Gauß",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 16L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 17L,
              data = java.sql.Date.valueOf("1777-04-30"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 18L,
              data = new java.math.BigDecimal("2.71"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 19L,
              data = 4L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 20L,
              data = "Johann Benedict Listing",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 21L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 22L,
              data = java.sql.Date.valueOf("1808-07-25"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 23L,
              data = new java.math.BigDecimal("4.2"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            ),
            BaseWriterMessages.WriteData(
              number = 24L,
              data = 5L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 25L,
              data = "Gottfried Wilhelm Leibnitz",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 26L,
              data = None,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 27L,
              data = java.sql.Date.valueOf("1646-07-01"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 28L,
              data = new java.math.BigDecimal("1.99"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            )
          )
          val expected = SortedSet.empty[BaseWriterMessages.WriteData] ++ e
          val incompleteRow = SortedSet.empty[BaseWriterMessages.WriteData] ++ List(
            BaseWriterMessages.WriteData(
              number = 34L,
              data = 7L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 35L,
              data = "Gottfried Wilhelm Leibnitz",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            )
          )

          val seqElement = createNormalizedDocument(dfasdl.content).getElementById("accounts")
          a ! BaseWriterFilterWorker.FilterSequenceRows(s, seqElement, None)
          expectMsg(BaseWriterMessages.WriteBatchData(expected.toList))
          a.underlyingActor.asInstanceOf[BaseWriterFilterWorker].unfilteredDataBuffer should be(
            incompleteRow
          )

          val s2 = SortedSet.empty[BaseWriterMessages.WriteData] ++ List(
            BaseWriterMessages.WriteData(
              number = 36L,
              data = "Tralala",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 37L,
              data = java.sql.Date.valueOf("1646-07-01"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 38L,
              data = new java.math.BigDecimal("1.99"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            )
          )
          a ! BaseWriterFilterWorker.FilterSequenceRows(s2, seqElement, None)
          val expected2 = SortedSet.empty[BaseWriterMessages.WriteData] ++ List(
            BaseWriterMessages.WriteData(
              number = 34L,
              data = 7L,
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-id"))
            ),
            BaseWriterMessages.WriteData(
              number = 35L,
              data = "Gottfried Wilhelm Leibnitz",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-name"))
            ),
            BaseWriterMessages.WriteData(
              number = 36L,
              data = "Tralala",
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-description"))
            ),
            BaseWriterMessages.WriteData(
              number = 37L,
              data = java.sql.Date.valueOf("1646-07-01"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-birthday"))
            ),
            BaseWriterMessages.WriteData(
              number = 38L,
              data = new java.math.BigDecimal("1.99"),
              metaData = Option(BaseWriter.WriterMessageMetaData(id = "accounts-row-salary"))
            )
          )
          expectMsg(BaseWriterMessages.WriteBatchData(expected2.toList))
          a.underlyingActor.asInstanceOf[BaseWriterFilterWorker].unfilteredDataBuffer should be(
            SortedSet.empty[BaseWriterMessages.WriteData]
          )
          system stop a
        }
      }
    }
  }
}
