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
import com.wegtam.tensei.agent.adt.TenseiForeignKeyValueType
import com.wegtam.tensei.agent.processor.AutoIncrementValueBuffer.{
  AutoIncrementValueBufferMessages,
  AutoIncrementValuePair
}

class AutoIncrementValueBufferTest extends ActorSpec {
  describe("AutoIncrementValueBuffer") {
    val agentRunIdentifier = Option("TEST")

    describe("returning values") {
      describe("for unknown references") {
        it("should return a not found message") {
          val a = TestActorRef(AutoIncrementValueBuffer.props(agentRunIdentifier))
          val r = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
          val vs = Vector(
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(1L)),
                                   TenseiForeignKeyValueType.FkLong(Option(5L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(2L)),
                                   TenseiForeignKeyValueType.FkLong(Option(6L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(3L)),
                                   TenseiForeignKeyValueType.FkLong(Option(7L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(4L)),
                                   TenseiForeignKeyValueType.FkLong(Option(8L)))
          )

          a ! AutoIncrementValueBufferMessages.Store(r, vs)

          val wrongRef = ElementReference(dfasdlId = "SOME-DFASDL", elementId = "SOME-ELEMENT")

          a ! AutoIncrementValueBufferMessages.Return(wrongRef,
                                                      TenseiForeignKeyValueType.FkLong(Option(1L)))
          expectMsg(
            AutoIncrementValueBufferMessages
              .ValueNotFound(wrongRef, TenseiForeignKeyValueType.FkLong(Option(1L)))
          )
        }
      }

      describe("for known references") {
        it("should forward the message to the correct actor") {
          val a = TestActorRef(AutoIncrementValueBuffer.props(agentRunIdentifier))
          val r = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
          val vs = Vector(
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(1L)),
                                   TenseiForeignKeyValueType.FkLong(Option(5L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(2L)),
                                   TenseiForeignKeyValueType.FkLong(Option(6L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(3L)),
                                   TenseiForeignKeyValueType.FkLong(Option(7L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(4L)),
                                   TenseiForeignKeyValueType.FkLong(Option(8L)))
          )

          a ! AutoIncrementValueBufferMessages.Store(r, vs)

          vs.foreach { p =>
            a ! AutoIncrementValueBufferMessages.Return(r, p.oldValue)
            expectMsg(AutoIncrementValueBufferMessages.ChangedValue(r, p.oldValue, p.newValue))
          }
        }
      }
    }

    describe("storing values") {
      it("should work") {
        val a = TestActorRef(AutoIncrementValueBuffer.props(agentRunIdentifier))
        val r = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
        val vs = Vector(
          AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(1L)),
                                 TenseiForeignKeyValueType.FkLong(Option(5L))),
          AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(2L)),
                                 TenseiForeignKeyValueType.FkLong(Option(6L))),
          AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(3L)),
                                 TenseiForeignKeyValueType.FkLong(Option(7L))),
          AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(4L)),
                                 TenseiForeignKeyValueType.FkLong(Option(8L)))
        )

        a ! AutoIncrementValueBufferMessages.Store(r, vs)

        vs.foreach { p =>
          a ! AutoIncrementValueBufferMessages.Return(r, p.oldValue)
          expectMsg(AutoIncrementValueBufferMessages.ChangedValue(r, p.oldValue, p.newValue))
        }

        val r2 = ElementReference(dfasdlId = "MY-DFASDL-2", elementId = "MY-ELEMENT-2")
        val vs2 = Vector(
          AutoIncrementValuePair(TenseiForeignKeyValueType.FkString(Option("FOO")),
                                 TenseiForeignKeyValueType.FkLong(Option(5L))),
          AutoIncrementValuePair(TenseiForeignKeyValueType.FkString(Option("BAR")),
                                 TenseiForeignKeyValueType.FkLong(Option(6L))),
          AutoIncrementValuePair(TenseiForeignKeyValueType.FkString(Option("FOOBAR")),
                                 TenseiForeignKeyValueType.FkLong(Option(7L))),
          AutoIncrementValuePair(TenseiForeignKeyValueType.FkString(None),
                                 TenseiForeignKeyValueType.FkLong(Option(8L)))
        )

        a ! AutoIncrementValueBufferMessages.Store(r2, vs2)

        vs2.foreach { p =>
          a ! AutoIncrementValueBufferMessages.Return(r2, p.oldValue)
          expectMsg(AutoIncrementValueBufferMessages.ChangedValue(r2, p.oldValue, p.newValue))
        }

        a.underlyingActor.asInstanceOf[AutoIncrementValueBuffer].buffer.size should be(2)
      }
    }
  }
}
