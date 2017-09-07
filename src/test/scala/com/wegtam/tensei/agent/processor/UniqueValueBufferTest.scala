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

import akka.testkit.TestActorRef
import com.wegtam.tensei.adt.ElementReference
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.processor.UniqueValueBuffer.UniqueValueBufferMessages

class UniqueValueBufferTest extends ActorSpec {
  describe("UniqueValueBuffer") {
    val agentRunIdentifier = Option("TEST")

    describe("checking values") {
      describe("for unknown references") {
        it("should work") {
          val a = TestActorRef(UniqueValueBuffer.props(agentRunIdentifier))
          val r = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
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

          val wrongRef = ElementReference(dfasdlId = "SOME-DFASDL", elementId = "SOME-ELEMENT")
          a ! UniqueValueBufferMessages.CheckIfValueExists(wrongRef, "FOO")
          expectMsg(UniqueValueBufferMessages.ValueDoesNotExist(wrongRef, "FOO"))
        }
      }

      describe("for known references") {
        it("should work") {
          val a  = TestActorRef(UniqueValueBuffer.props(agentRunIdentifier))
          val r1 = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
          val vs1 = Vector(
            UniqueValueBufferMessages.Store(r1, "FOO"),
            UniqueValueBufferMessages.Store(r1, "BAR"),
            UniqueValueBufferMessages.Store(r1, 1L),
            UniqueValueBufferMessages.Store(r1, None)
          )
          vs1.foreach { v =>
            a ! v
            expectMsg(UniqueValueBufferMessages.StoreAck(v.ref))
          }
          val r2 = ElementReference(dfasdlId = "MY-DFASDL", elementId = "ANOTHER-ELEMENT")
          val vs2 = Vector(
            UniqueValueBufferMessages.Store(r2, "FOO"),
            UniqueValueBufferMessages.Store(r2, "BAR"),
            UniqueValueBufferMessages.Store(r2, 1L),
            UniqueValueBufferMessages.Store(r2, None)
          )
          vs2.foreach { v =>
            a ! v
            expectMsg(UniqueValueBufferMessages.StoreAck(v.ref))
          }
          val r3            = ElementReference(dfasdlId = "ANOTHER-DFASDL", elementId = "ANOTHER-ELEMENT")
          val vs3: Set[Any] = Set("FOO", "BAR", 1L, None)
          a ! UniqueValueBufferMessages.StoreS(r3, vs3)
          expectMsg(UniqueValueBufferMessages.StoreSeqAck(r3))

          vs1.foreach { v =>
            a ! UniqueValueBufferMessages.CheckIfValueExists(v.ref, v.value)
            expectMsg(UniqueValueBufferMessages.ValueExists(v.ref, v.value))
          }
          vs2.foreach { v =>
            a ! UniqueValueBufferMessages.CheckIfValueExists(v.ref, v.value)
            expectMsg(UniqueValueBufferMessages.ValueExists(v.ref, v.value))
          }
          vs3.foreach { v =>
            a ! UniqueValueBufferMessages.CheckIfValueExists(r3, v)
            expectMsg(UniqueValueBufferMessages.ValueExists(r3, v))
          }
        }
      }
    }

    describe("storing values") {
      it("should work") {
        val a  = TestActorRef(UniqueValueBuffer.props(agentRunIdentifier))
        val r1 = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
        val vs1 = Vector(
          UniqueValueBufferMessages.Store(r1, "FOO"),
          UniqueValueBufferMessages.Store(r1, "BAR"),
          UniqueValueBufferMessages.Store(r1, 1L),
          UniqueValueBufferMessages.Store(r1, None)
        )
        vs1.foreach { v =>
          a ! v
          expectMsg(UniqueValueBufferMessages.StoreAck(v.ref))
        }
        val r2 = ElementReference(dfasdlId = "MY-DFASDL", elementId = "ANOTHER-ELEMENT")
        val vs2 = Vector(
          UniqueValueBufferMessages.Store(r2, "FOO"),
          UniqueValueBufferMessages.Store(r2, "BAR"),
          UniqueValueBufferMessages.Store(r2, 1L),
          UniqueValueBufferMessages.Store(r2, None)
        )
        vs2.foreach { v =>
          a ! v
          expectMsg(UniqueValueBufferMessages.StoreAck(v.ref))
        }
        val r3 = ElementReference(dfasdlId = "ANOTHER-DFASDL", elementId = "ANOTHER-ELEMENT")
        val vs3 = Vector(
          UniqueValueBufferMessages.Store(r3, "FOO"),
          UniqueValueBufferMessages.Store(r3, "BAR"),
          UniqueValueBufferMessages.Store(r3, 1L),
          UniqueValueBufferMessages.Store(r3, None)
        )
        vs3.foreach { v =>
          a ! v
          expectMsg(UniqueValueBufferMessages.StoreAck(v.ref))
        }

        a.underlyingActor.asInstanceOf[UniqueValueBuffer].buffer.size should be(3)

        vs1.foreach { v =>
          a ! UniqueValueBufferMessages.CheckIfValueExists(v.ref, v.value)
          expectMsg(UniqueValueBufferMessages.ValueExists(v.ref, v.value))
        }
        vs2.foreach { v =>
          a ! UniqueValueBufferMessages.CheckIfValueExists(v.ref, v.value)
          expectMsg(UniqueValueBufferMessages.ValueExists(v.ref, v.value))
        }
        vs3.foreach { v =>
          a ! UniqueValueBufferMessages.CheckIfValueExists(v.ref, v.value)
          expectMsg(UniqueValueBufferMessages.ValueExists(v.ref, v.value))
        }
      }
    }
  }

}
