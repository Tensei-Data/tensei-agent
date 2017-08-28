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

class ExtractBiggestValueTest extends ActorSpec {
  describe("Transformers") {
    describe("IfBigger") {
      describe("when given no values") {
        it("should return an empty String") {
          val actor = TestActorRef(ExtractBiggestValue.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          actor ! StartTransformation(List(),
                                      new TransformerOptions(classOf[String], classOf[String]))

          val response = TransformerResponse(List(ByteString("")), classOf[String])

          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given empty values") {
        it("should return an empty String") {
          val actor = TestActorRef(ExtractBiggestValue.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          actor ! StartTransformation(List(ByteString(""), ByteString("")),
                                      new TransformerOptions(classOf[String], classOf[String]))

          val response = TransformerResponse(List(ByteString("")), classOf[String])

          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given only one value") {
        it("should return the value") {
          val actor = TestActorRef(ExtractBiggestValue.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          actor ! StartTransformation(List(ByteString("one value")),
                                      new TransformerOptions(classOf[String], classOf[String]))

          val response = TransformerResponse(List(ByteString("one value")), classOf[String])

          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("for numerical values") {
        describe("the first is bigger than the second") {
          it("should return the first value") {
            val actor = TestActorRef(ExtractBiggestValue.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            actor ! StartTransformation(List(232136.77, 2323),
                                        new TransformerOptions(classOf[String], classOf[String]))

            val response = TransformerResponse(List(ByteString("232136.77")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("the second is bigger than the first") {
          it("should return the second value") {
            val actor = TestActorRef(ExtractBiggestValue.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            actor ! StartTransformation(List(4, 2323),
                                        new TransformerOptions(classOf[String], classOf[String]))

            val response = TransformerResponse(List(ByteString("2323")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("more than two values") {
          describe("the first is the biggest") {
            it("should return the first value") {
              val actor = TestActorRef(ExtractBiggestValue.props)

              actor ! PrepareForTransformation

              expectMsg(ReadyToTransform)

              actor ! StartTransformation(List(4000, 2, 111, 2323, 7),
                                          new TransformerOptions(classOf[String], classOf[String]))

              val response = TransformerResponse(List(ByteString("4000")), classOf[String])

              val d = expectMsgType[TransformerResponse]
              d.data shouldEqual response.data
            }
          }

          describe("the last is the biggest") {
            it("should return the last value") {
              val actor = TestActorRef(ExtractBiggestValue.props)

              actor ! PrepareForTransformation

              expectMsg(ReadyToTransform)

              actor ! StartTransformation(List(4000, 2, 111, 2323, 783651423),
                                          new TransformerOptions(classOf[String], classOf[String]))

              val response = TransformerResponse(List(ByteString("783651423")), classOf[String])

              val d = expectMsgType[TransformerResponse]
              d.data shouldEqual response.data
            }
          }

          describe("one of the middle is the biggest") {
            it("should return the correct value") {
              val actor = TestActorRef(ExtractBiggestValue.props)

              actor ! PrepareForTransformation

              expectMsg(ReadyToTransform)

              actor ! StartTransformation(List(4000, 2, 9999, 2323, 2312),
                                          new TransformerOptions(classOf[String], classOf[String]))

              val response = TransformerResponse(List(ByteString("9999")), classOf[String])

              val d = expectMsgType[TransformerResponse]
              d.data shouldEqual response.data
            }
          }
        }
      }

      describe("for string values") {
        describe("one is a numerical value that is longer than the string value") {
          it("should return the numerical value") {
            val actor = TestActorRef(ExtractBiggestValue.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            actor ! StartTransformation(List(ByteString("cat"), 2323),
                                        new TransformerOptions(classOf[String], classOf[String]))

            val response = TransformerResponse(List(ByteString("2323")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("one is a numerical value that is smaller than the string value") {
          it("should return the string value") {
            val actor = TestActorRef(ExtractBiggestValue.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            actor ! StartTransformation(List(ByteString("Hausmeister"), 2323),
                                        new TransformerOptions(classOf[String], classOf[String]))

            val response = TransformerResponse(List(ByteString("Hausmeister")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("the first string is longer than the second") {
          it("should return the first value") {
            val actor = TestActorRef(ExtractBiggestValue.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            actor ! StartTransformation(List(ByteString("sieben"), ByteString("eins")),
                                        new TransformerOptions(classOf[String], classOf[String]))

            val response = TransformerResponse(List(ByteString("sieben")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("the first string is smaller than the second") {
          it("should return the second value") {
            val actor = TestActorRef(ExtractBiggestValue.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            actor ! StartTransformation(List(ByteString("null!"),
                                             ByteString("Dies ist mal ein ganzer Satz!")),
                                        new TransformerOptions(classOf[String], classOf[String]))

            val response = TransformerResponse(List(ByteString("Dies ist mal ein ganzer Satz!")),
                                               classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("more than two values") {
          describe("the first is the longest") {
            it("should return the first string") {
              val actor = TestActorRef(ExtractBiggestValue.props)

              actor ! PrepareForTransformation

              expectMsg(ReadyToTransform)

              actor ! StartTransformation(
                List(ByteString("Dies ist mal ein ganzer Satz!"),
                     ByteString("null!"),
                     ByteString("FooBar"),
                     ByteString("Tensei-Data")),
                new TransformerOptions(classOf[String], classOf[String])
              )

              val response = TransformerResponse(List(ByteString("Dies ist mal ein ganzer Satz!")),
                                                 classOf[String])

              val d = expectMsgType[TransformerResponse]
              d.data shouldEqual response.data
            }
          }

          describe("the last is the longest") {
            it("should return the last string") {
              val actor = TestActorRef(ExtractBiggestValue.props)

              actor ! PrepareForTransformation

              expectMsg(ReadyToTransform)

              actor ! StartTransformation(
                List(
                  ByteString("Dies ist mal ein ganzer Satz!"),
                  ByteString("null!"),
                  ByteString("FooBar"),
                  ByteString("Tensei-Data"),
                  ByteString("Das ist das Haus vom Nikolaus und nebenan wohnt der Weihnachtsmann!")
                ),
                new TransformerOptions(classOf[String], classOf[String])
              )

              val response = TransformerResponse(
                List(
                  ByteString("Das ist das Haus vom Nikolaus und nebenan wohnt der Weihnachtsmann!")
                ),
                classOf[String]
              )

              val d = expectMsgType[TransformerResponse]
              d.data shouldEqual response.data
            }
          }

          describe("one of the middle values is the longest") {
            it("should return the correct string") {
              val actor = TestActorRef(ExtractBiggestValue.props)

              actor ! PrepareForTransformation

              expectMsg(ReadyToTransform)

              actor ! StartTransformation(
                List(
                  ByteString("Dies ist mal ein ganzer Satz!"),
                  ByteString("null!"),
                  ByteString("FooBar ist ein wirklich komischer Begriff, aber was solls!"),
                  ByteString("Tensei-Data"),
                  ByteString("Das ist das Haus vom Nikolaus!")
                ),
                new TransformerOptions(classOf[String], classOf[String])
              )

              val response = TransformerResponse(
                List(ByteString("FooBar ist ein wirklich komischer Begriff, aber was solls!")),
                classOf[String]
              )

              val d = expectMsgType[TransformerResponse]
              d.data shouldEqual response.data
            }
          }
        }
      }
    }
  }
}
