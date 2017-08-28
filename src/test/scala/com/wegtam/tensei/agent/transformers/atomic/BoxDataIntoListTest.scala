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

package com.wegtam.tensei.agent.transformers.atomic

import akka.testkit.TestActorRef
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform,
  StartTransformation,
  TransformerResponse
}
import com.wegtam.tensei.adt.TransformerOptions

class BoxDataIntoListTest extends ActorSpec {
  describe("Transformers") {
    describe("Atomic") {
      describe("BoxDataIntoList") {
        describe("when given data") {
          it("should return List(data)") {
            val actor = TestActorRef(BoxDataIntoList.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val data = "I am a string!"

            actor ! new StartTransformation(List(data),
                                            new TransformerOptions(classOf[String],
                                                                   classOf[String]))

            val expectedResponse = new TransformerResponse(List(List(data)), classOf[String])

            val response = expectMsg(expectedResponse)

            response.data.head should be(List(data))
          }
        }

        describe("when given List(data)") {
          it("should return List(List(data))") {
            val actor = TestActorRef(BoxDataIntoList.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val data = List("I am a string!")

            actor ! new StartTransformation(List(data),
                                            new TransformerOptions(classOf[String],
                                                                   classOf[String]))

            val expectedResponse = new TransformerResponse(List(List(data)), classOf[String])

            val response = expectMsg(expectedResponse)

            response.data.head should be(List(data))
          }
        }
      }
    }
  }
}
