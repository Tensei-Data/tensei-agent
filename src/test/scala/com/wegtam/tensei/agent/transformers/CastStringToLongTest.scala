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

package com.wegtam.tensei.agent.transformers

import akka.testkit.TestActorRef
import akka.util.ByteString
import com.wegtam.tensei.adt.TransformerOptions
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform,
  StartTransformation,
  TransformerResponse
}

class CastStringToLongTest extends ActorSpec {
  describe("CastStringToLong") {
    describe("with empty string") {
      it("should return None") {
        val actor = TestActorRef(CastStringToLong.props)

        actor ! PrepareForTransformation

        expectMsg(ReadyToTransform)

        actor ! StartTransformation(List(ByteString("")),
                                    new TransformerOptions(classOf[String], classOf[String]))

        val response = TransformerResponse(List(None), None.getClass)

        expectMsg(response)
      }
    }
    describe("with invalid number string") {
      it("should return None") {
        val actor = TestActorRef(CastStringToLong.props)

        actor ! PrepareForTransformation

        expectMsg(ReadyToTransform)

        actor ! StartTransformation(List(ByteString("foo")),
                                    new TransformerOptions(classOf[String], classOf[String]))

        val response = TransformerResponse(List(None), None.getClass)

        expectMsg(response)
      }
    }
    describe("with valid number string") {
      it("should return the number as Long value") {
        val actor = TestActorRef(CastStringToLong.props)

        actor ! PrepareForTransformation

        expectMsg(ReadyToTransform)

        actor ! StartTransformation(List(ByteString("123")),
                                    new TransformerOptions(classOf[String], classOf[String]))

        val response = TransformerResponse(List(123L), Long.getClass)

        expectMsg(response)
      }

      it("should return the numbers as Long values") {
        val actor = TestActorRef(CastStringToLong.props)

        actor ! PrepareForTransformation

        expectMsg(ReadyToTransform)

        actor ! StartTransformation(List(ByteString("123"),
                                         ByteString("456"),
                                         ByteString("789"),
                                         ByteString("-1")),
                                    new TransformerOptions(classOf[String], classOf[String]))

        val response = TransformerResponse(List(123L, 456L, 789L, -1L), Long.getClass)

        expectMsg(response)
      }
    }
  }
}
