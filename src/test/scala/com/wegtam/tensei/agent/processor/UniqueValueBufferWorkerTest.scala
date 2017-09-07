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

import akka.testkit.{ EventFilter, TestActorRef }
import com.wegtam.tensei.adt.ElementReference
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.processor.UniqueValueBuffer.UniqueValueBufferMessages

class UniqueValueBufferWorkerTest extends ActorSpec {
  describe("UniqueValueBufferWorker") {
    val agentRunIdentifier = Option("TEST")

    describe("checking values") {
      describe("for existing values") {
        it("should work") {
          val r = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
          val a = TestActorRef(UniqueValueBufferWorker.props(agentRunIdentifier, r))
          a ! UniqueValueBufferMessages.Store(r, "FOO")
          expectMsg(UniqueValueBufferMessages.StoreAck(r))
          a ! UniqueValueBufferMessages.CheckIfValueExists(r, "FOO")
          expectMsg(UniqueValueBufferMessages.ValueExists(r, "FOO"))
        }
      }

      describe("for missing values") {
        it("should work") {
          val r = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
          val a = TestActorRef(UniqueValueBufferWorker.props(agentRunIdentifier, r))
          a ! UniqueValueBufferMessages.Store(r, "BAR")
          expectMsg(UniqueValueBufferMessages.StoreAck(r))
          a ! UniqueValueBufferMessages.CheckIfValueExists(r, "FOO")
          expectMsg(UniqueValueBufferMessages.ValueDoesNotExist(r, "FOO"))
          val wrongRef = ElementReference(dfasdlId = "SOME-DFASDL", elementId = "SOME-ELEMENT")
          a ! UniqueValueBufferMessages.CheckIfValueExists(wrongRef, "BAR")
          expectMsg(UniqueValueBufferMessages.ValueDoesNotExist(wrongRef, "BAR"))
        }
      }
    }

    describe("storing values") {
      describe("for unknown references") {
        it("should log an error message") {
          val r = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
          val a = TestActorRef(UniqueValueBufferWorker.props(agentRunIdentifier, r))

          val wrongRef = ElementReference(dfasdlId = "SOME-DFASDL", elementId = "SOME-ELEMENT")

          EventFilter.error(
            occurrences = 1,
            start = "Received store unique element value message for wrong element"
          ) intercept (a ! UniqueValueBufferMessages.Store(wrongRef, "FOO"))
        }
      }

      describe("for known references") {
        describe("with new values") {
          it("should work") {
            val r = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
            val a = TestActorRef(UniqueValueBufferWorker.props(agentRunIdentifier, r))
            val vs = Vector(
              UniqueValueBufferMessages.Store(r, "FOO"),
              UniqueValueBufferMessages.Store(r, "BAR"),
              UniqueValueBufferMessages.Store(r, 1L),
              UniqueValueBufferMessages.Store(r, None)
            )
            vs.foreach { v =>
              a ! v
              expectMsg(UniqueValueBufferMessages.StoreAck(v.ref))
            }
            val vss: Set[Any] = Set("FOOBAR", 3L)
            a ! UniqueValueBufferMessages.StoreS(r, vss)
            expectMsg(UniqueValueBufferMessages.StoreSeqAck(r))

            vs.foreach(
              v =>
                a.underlyingActor.asInstanceOf[UniqueValueBufferWorker].buffer should contain(
                  v.value
              )
            )
            vss.foreach(
              v => a.underlyingActor.asInstanceOf[UniqueValueBufferWorker].buffer should contain(v)
            )
          }
        }

        describe("with already stored values") {
          it("should log a warning message") {
            val r = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
            val a = TestActorRef(UniqueValueBufferWorker.props(agentRunIdentifier, r))
            a ! UniqueValueBufferMessages.Store(r, "FOO")

            EventFilter.warning(
              message = s"Given unique element value for $r already stored!",
              occurrences = 1
            ) intercept (a ! UniqueValueBufferMessages.Store(r, "FOO"))
          }
        }
      }
    }
  }

}
