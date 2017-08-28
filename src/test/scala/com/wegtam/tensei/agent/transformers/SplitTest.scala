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

class SplitTest extends ActorSpec {
  describe("Transfomers") {
    describe("Split") {
      describe("when given an empty list") {
        it("should return an empty string") {
          val actor = TestActorRef(Split.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          actor ! StartTransformation(List(),
                                      new TransformerOptions(classOf[String], classOf[String]))

          val response = TransformerResponse(List(ByteString("")), classOf[String])

          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }

        describe("with an empty pattern") {
          it("should return an empty string") {
            val actor = TestActorRef(Split.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("pattern", ""))

            actor ! StartTransformation(List(),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))

            val response = TransformerResponse(List(ByteString("")), classOf[String])

            expectMsg(response)
          }
        }

        describe("with a limit") {
          it("should return an empty string") {
            val actor = TestActorRef(Split.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("limit", "2"))

            actor ! StartTransformation(List(),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))

            val response = TransformerResponse(List(ByteString("")), classOf[String])

            expectMsg(response)
          }
        }

        describe("with selected entries") {
          it("should return an empty string") {
            val actor = TestActorRef(Split.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("selected", "1,2"))

            actor ! StartTransformation(List(),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))

            val response = TransformerResponse(List(ByteString("")), classOf[String])

            expectMsg(response)
          }
        }

        describe("with selected entries and a limit") {
          it("should return an empty string") {
            val actor = TestActorRef(Split.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("selected", "1,2"), ("limit", "2"))

            actor ! StartTransformation(List(),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))

            val response = TransformerResponse(List(ByteString("")), classOf[String])

            expectMsg(response)
          }
        }
      }

      describe("when given a string") {
        it("should return the unsplitted string") {
          val actor = TestActorRef(Split.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          actor ! StartTransformation(
            List(ByteString("alex, mustermann, 25.11.1980, 0381-123456789")),
            new TransformerOptions(classOf[String], classOf[String])
          )

          val response =
            TransformerResponse(List(ByteString("alex, mustermann, 25.11.1980, 0381-123456789")),
                                classOf[String])

          expectMsg(response)
        }

        describe("with a pattern that is not in the string") {
          it("should return the string") {
            val actor = TestActorRef(Split.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("pattern", ";"))

            actor ! StartTransformation(
              List(ByteString("alex, mustermann, 25.11.1980, 0381-123456789")),
              new TransformerOptions(classOf[String], classOf[String], params)
            )

            val response =
              TransformerResponse(List(ByteString("alex, mustermann, 25.11.1980, 0381-123456789")),
                                  classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with a pattern") {
          it("should return the splitted parts of the string") {
            val actor = TestActorRef(Split.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("pattern", ","))

            actor ! StartTransformation(
              List(ByteString("alex, mustermann, 25.11.1980, 0381-123456789")),
              new TransformerOptions(classOf[String], classOf[String], params)
            )

            val response = TransformerResponse(List(ByteString("alex"),
                                                    ByteString("mustermann"),
                                                    ByteString("25.11.1980"),
                                                    ByteString("0381-123456789")),
                                               classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with a pattern and a limit") {
          it("should return the limited first splitted parts of the string") {
            val actor = TestActorRef(Split.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("pattern", ","), ("limit", "2"))

            actor ! StartTransformation(
              List(ByteString("alex, mustermann, 25.11.1980, 0381-123456789")),
              new TransformerOptions(classOf[String], classOf[String], params)
            )

            val response = TransformerResponse(List(ByteString("alex"), ByteString("mustermann")),
                                               classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with a pattern and a selected amount of splitted parts") {
          it("should return the selected splitted parts of the string") {
            val actor = TestActorRef(Split.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("pattern", ","), ("selected", "0, 2, 3"))

            actor ! StartTransformation(
              List(ByteString("alex, mustermann, 25.11.1980, 0381-123456789")),
              new TransformerOptions(classOf[String], classOf[String], params)
            )

            val response = TransformerResponse(List(ByteString("alex"),
                                                    ByteString("25.11.1980"),
                                                    ByteString("0381-123456789")),
                                               classOf[String])

            expectMsg(response)
          }
        }

        describe(
          "with a pattern and a selected amount of splitted parts where the selected parts are incorrect"
        ) {
          it("should return the selected splitted parts of the string that are within the list") {
            val actor = TestActorRef(Split.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("pattern", ","), ("selected", "0, 2, 5"))

            actor ! StartTransformation(
              List(ByteString("alex, mustermann, 25.11.1980, 0381-123456789")),
              new TransformerOptions(classOf[String], classOf[String], params)
            )

            val response = TransformerResponse(List(ByteString("alex"),
                                                    ByteString("25.11.1980"),
                                                    ByteString("")),
                                               classOf[String])

            expectMsg(response)
          }
        }
      }
    }
  }
}
