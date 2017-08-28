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
import com.wegtam.tensei.agent.adt.TenseiForeignKeyValueType
import com.wegtam.tensei.agent.processor.AutoIncrementValueBuffer.{
  AutoIncrementValueBufferMessages,
  AutoIncrementValuePair
}

class AutoIncrementValueBufferWorkerTest extends ActorSpec {
  describe("AutoIncrementValueBufferWorker") {
    val agentRunIdentifier = Option("TEST")

    describe("returning values") {
      describe("for unknown values") {
        it("should return a not found message") {
          val r = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
          val a = TestActorRef(AutoIncrementValueBufferWorker.props(agentRunIdentifier, r))
          val vs = Vector(
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(1L)),
                                   TenseiForeignKeyValueType.FkLong(Option(5L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkString(Option("FOO")),
                                   TenseiForeignKeyValueType.FkLong(Option(6L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(3L)),
                                   TenseiForeignKeyValueType.FkLong(Option(7L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkString(Option("BAR")),
                                   TenseiForeignKeyValueType.FkLong(Option(8L)))
          )

          a ! AutoIncrementValueBufferMessages.Store(r, vs)

          val unknownValue = TenseiForeignKeyValueType.FkLong(Option(-1L))
          a ! AutoIncrementValueBufferMessages.Return(r, unknownValue)
          expectMsg(AutoIncrementValueBufferMessages.ValueNotFound(r, unknownValue))
        }
      }

      describe("for known values") {
        it("should return the values") {
          val r = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
          val a = TestActorRef(AutoIncrementValueBufferWorker.props(agentRunIdentifier, r))
          val vs = Vector(
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(1L)),
                                   TenseiForeignKeyValueType.FkLong(Option(5L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkString(Option("FOO")),
                                   TenseiForeignKeyValueType.FkLong(Option(6L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(3L)),
                                   TenseiForeignKeyValueType.FkLong(Option(7L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkString(Option("BAR")),
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
      describe("for unknown element references") {
        it("should fail") {
          val r = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
          val a = TestActorRef(AutoIncrementValueBufferWorker.props(agentRunIdentifier, r))

          val wrongRef = ElementReference(dfasdlId = "SOME-DFASDL", elementId = "SOME-ELEMENT")

          EventFilter.error(
            occurrences = 1,
            start = "Received store foreign key value message for wrong element"
          ) intercept (a ! AutoIncrementValueBufferMessages.Store(
            wrongRef,
            Vector.empty[AutoIncrementValuePair]
          ))
        }
      }

      describe("for the correct element reference") {
        it("should work") {
          val r = ElementReference(dfasdlId = "MY-DFASDL", elementId = "MY-ELEMENT")
          val a = TestActorRef(AutoIncrementValueBufferWorker.props(agentRunIdentifier, r))
          val vs = Vector(
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(1L)),
                                   TenseiForeignKeyValueType.FkLong(Option(5L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkString(Option("FOO")),
                                   TenseiForeignKeyValueType.FkLong(Option(6L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkLong(Option(3L)),
                                   TenseiForeignKeyValueType.FkLong(Option(7L))),
            AutoIncrementValuePair(TenseiForeignKeyValueType.FkString(Option("BAR")),
                                   TenseiForeignKeyValueType.FkLong(Option(8L)))
          )

          a ! AutoIncrementValueBufferMessages.Store(r, vs)

          vs.foreach(
            p =>
              a.underlyingActor
                .asInstanceOf[AutoIncrementValueBufferWorker]
                .buffer(p.oldValue) should be(p.newValue)
          )

        }
      }
    }
  }
}
