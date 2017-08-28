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

class EmptyStringTest extends ActorSpec {
  describe("Transformers") {
    describe("EmptyString") {
      describe("when given an empty input") {
        it("should return a list with an empty string") {
          val actor = TestActorRef(EmptyString.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)

          actor ! StartTransformation(List(),
                                      new TransformerOptions(classOf[String], classOf[String]))
          val response = TransformerResponse(List(ByteString("")), classOf[String])
          expectMsg(response)
        }
      }

      describe("when given a list of data") {
        it("should return a list with an empty string") {
          val actor = TestActorRef(EmptyString.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)

          actor ! StartTransformation(List("a", "b", "c"),
                                      new TransformerOptions(classOf[String], classOf[String]))
          val response = TransformerResponse(List(ByteString("")), classOf[String])
          expectMsg(response)
        }
      }
    }
  }
}
