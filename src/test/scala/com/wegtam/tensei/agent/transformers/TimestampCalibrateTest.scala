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
import com.wegtam.tensei.adt.TransformerOptions
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform,
  StartTransformation,
  TransformerResponse
}

class TimestampCalibrateTest extends ActorSpec {
  describe("TimestampCalibrate") {
    describe("when given no data") {
      it("should return an empty list") {
        val actor = TestActorRef(TimestampCalibrate.props)

        actor ! PrepareForTransformation

        expectMsg(ReadyToTransform)

        actor ! new StartTransformation(List(),
                                        new TransformerOptions(classOf[String], classOf[String]))

        val expectedResponse = new TransformerResponse(List(), classOf[String])

        val response = expectMsg(expectedResponse)

        response.data.size should be(0)
      }
    }

    describe("add") {
      describe("when given one timestamp value") {
        it("should return a list with the transformed value") {
          val actor = TestActorRef(TimestampCalibrate.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          val data = List(1441196805)

          val params = List(("perform", "add"))

          actor ! new StartTransformation(data,
                                          new TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))

          val expectedResponse = new TransformerResponse(List(1441196805000L), classOf[String])

          val response = expectMsg(expectedResponse)

          response.data.size should be(1)

          response.data.head should be(1441196805000L)
        }
      }

      describe("when given multiple timestamp values") {
        it("should return a list with the transformed values") {
          val actor = TestActorRef(TimestampCalibrate.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          val data = List(1441196805, 1441197462, 1441197489)

          val params = List(("perform", "add"))

          actor ! new StartTransformation(data,
                                          new TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))

          val expectedResponse =
            new TransformerResponse(List(1441196805000L, 1441197462000L, 1441197489000L),
                                    classOf[String])

          val response = expectMsg(expectedResponse)

          response.data.size should be(3)

          response.data.head should be(1441196805000L)

          response.data.drop(1).head should be(1441197462000L)

          response.data.drop(2).head should be(1441197489000L)
        }
      }
    }

    describe("reduce") {
      describe("when given one timestamp value") {
        it("should return a list with the transformed value") {
          val actor = TestActorRef(TimestampCalibrate.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          val data = List(1441196805000L)

          val params = List(("perform", "reduce"))

          actor ! new StartTransformation(data,
                                          new TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))

          val expectedResponse = new TransformerResponse(List(1441196805L), classOf[String])

          val response = expectMsg(expectedResponse)

          response.data.size should be(1)

          response.data.head should be(1441196805L)
        }
      }

      describe("when given multiple timestamp values") {
        it("should return a list with the transformed values") {
          val actor = TestActorRef(TimestampCalibrate.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          val data = List(1441196805000L, 1441197462000L, 1441197489000L)

          val params = List(("perform", "reduce"))

          actor ! new StartTransformation(data,
                                          new TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))

          val expectedResponse =
            new TransformerResponse(List(1441196805L, 1441197462L, 1441197489L), classOf[String])

          val response = expectMsg(expectedResponse)

          response.data.size should be(3)

          response.data.head should be(1441196805L)

          response.data.drop(1).head should be(1441197462L)

          response.data.drop(2).head should be(1441197489L)
        }
      }
    }
  }
}
