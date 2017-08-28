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

import java.lang.Double

import akka.util.ByteString
import argonaut._, Argonaut._
import akka.testkit.TestActorRef
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform,
  StartTransformation,
  TransformerResponse
}
import com.wegtam.tensei.adt.TransformerOptions

class ToJsonTest extends ActorSpec {
  describe("Transformers") {
    describe("ToJson") {
      describe("when given an empty list") {
        it("should return an empty string") {
          val actor = TestActorRef(ToJson.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          actor ! new StartTransformation(List(),
                                          new TransformerOptions(classOf[String], classOf[String]))

          val response = new TransformerResponse(List(ByteString("")), classOf[String])

          expectMsg(response)
        }
      }

      describe("when given a single value") {
        describe("without label") {
          describe("using a string") {
            it("should return the proper json string") {
              val actor = TestActorRef(ToJson.props)

              actor ! PrepareForTransformation

              expectMsg(ReadyToTransform)

              actor ! new StartTransformation(List(ByteString("I am a lonely string.")),
                                              new TransformerOptions(classOf[String],
                                                                     classOf[String]))

              val response =
                new TransformerResponse(List(ByteString("I am a lonely string.".asJson.nospaces)),
                                        classOf[String])

              expectMsg(response)
            }
          }

          describe("using a double") {
            it("should return the proper json string") {
              val actor = TestActorRef(ToJson.props)

              actor ! PrepareForTransformation

              expectMsg(ReadyToTransform)

              actor ! new StartTransformation(List(new Double(3.14)),
                                              new TransformerOptions(classOf[Double],
                                                                     classOf[String]))

              val response =
                new TransformerResponse(List(ByteString(new Double(3.14).asJson.nospaces)),
                                        classOf[String])

              expectMsg(response)
            }
          }
        }

        describe("with label") {
          describe("using a string") {
            it("should return the proper json string") {
              val actor = TestActorRef(ToJson.props)

              actor ! PrepareForTransformation

              expectMsg(ReadyToTransform)

              val options = List(("label", "simpleLabel"))
              actor ! new StartTransformation(List(ByteString("I am a lonely string.")),
                                              new TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     options))

              val expectedJson = Json("simpleLabel" := "I am a lonely string.")
              val response =
                new TransformerResponse(List(ByteString(expectedJson.nospaces)), classOf[String])

              expectMsg(response)
            }
          }

          describe("using a double") {
            it("should return the proper json string") {
              val actor = TestActorRef(ToJson.props)

              actor ! PrepareForTransformation

              expectMsg(ReadyToTransform)

              val options = List(("label", "simpleLabel"))
              actor ! new StartTransformation(List(new Double(2.71)),
                                              new TransformerOptions(classOf[String],
                                                                     classOf[String],
                                                                     options))

              val expectedJson = Json("simpleLabel" := new Double(2.71))
              val response =
                new TransformerResponse(List(ByteString(expectedJson.nospaces)), classOf[String])

              expectMsg(response)
            }
          }
        }
      }

      describe("when given a list of values") {
        describe("without label") {
          it("should return json string containing an array") {
            val actor = TestActorRef(ToJson.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            actor ! new StartTransformation(List(new Double(2.71),
                                                 ByteString("I am a not so lonely string. :-)")),
                                            new TransformerOptions(classOf[String],
                                                                   classOf[String]))

            val expectedJson =
              Json.array(new Double(2.71).asJson, "I am a not so lonely string. :-)".asJson)
            val response =
              new TransformerResponse(List(ByteString(expectedJson.nospaces)), classOf[String])

            expectMsg(response)
          }
        }

        describe("with label") {
          it("should return a json string containing an object with a labeled array") {
            val actor = TestActorRef(ToJson.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val options = List(("label", "simpleLabel"))
            actor ! new StartTransformation(List(new Double(2.71),
                                                 ByteString("I am a not so lonely string. :-)")),
                                            new TransformerOptions(classOf[String],
                                                                   classOf[String],
                                                                   options))

            val jsonArray =
              Json.array(new Double(2.71).asJson, "I am a not so lonely string. :-)".asJson)
            val expectedJson = Json("simpleLabel" := jsonArray)
            val response =
              new TransformerResponse(List(ByteString(expectedJson.nospaces)), classOf[String])

            expectMsg(response)
          }
        }
      }
    }
  }
}
