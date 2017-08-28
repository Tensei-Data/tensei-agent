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

class OverwriteTest extends ActorSpec {
  describe("Transformers") {
    describe("Overwrite") {
      describe("when given an empty input") {
        it("should return a list with an empty string") {
          val actor = TestActorRef(Overwrite.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)

          actor ! StartTransformation(List(),
                                      new TransformerOptions(classOf[String], classOf[String]))
          val response = TransformerResponse(List(ByteString("")), classOf[String])
          expectMsg(response)
        }
      }

      describe("classOf[String]") {
        describe("with empty `value`") {
          it("should return a list with an empty string") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", ""), ("type", "String"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse = TransformerResponse(List(ByteString("")), classOf[String])
            expectMsg(expectedResponse)
          }
        }

        describe("with a `value`") {
          it("should return a list with a string") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", "foo"), ("type", "String"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse = TransformerResponse(List(ByteString("foo")), classOf[String])
            expectMsg(expectedResponse)
          }
        }

        describe("with a `value` and a wrong `type`") {
          it("should throw an exception") {
            intercept[AssertionError] {
              val actor = TestActorRef(Overwrite.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("value", "abc"), ("type", "Long"))
              actor ! StartTransformation(List(ByteString("a")),
                                          new TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))
              expectMsgType[TransformerResponse]
            }
          }
        }
      }

      describe("classOf[long]") {
        describe("with empty `value`") {
          it("should return a list with the value 0 as classOf[Long]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", ""), ("type", "Long"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse = TransformerResponse(List(0L), classOf[java.lang.Long])
            expectMsg(expectedResponse)
          }
        }

        describe("with a `value`") {
          it("should return a list with the provided value as classOf[Long]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", "55"), ("type", "Long"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse = TransformerResponse(List(55L), classOf[java.lang.Long])
            expectMsg(expectedResponse)
          }
        }

        describe("with a `value` and a wrong `type`") {
          it("should throw an exception") {
            intercept[AssertionError] {
              val actor = TestActorRef(Overwrite.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("value", "abc"), ("type", "Long"))
              actor ! StartTransformation(List(ByteString("a")),
                                          new TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))
              expectMsgType[TransformerResponse]
            }
          }
        }
      }

      describe("classOf[BigDecimal]") {
        describe("with empty `value`") {
          it("should return a list with the value 0 as classOf[BigDecimal]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", ""), ("type", "bigdecimal"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse =
              TransformerResponse(List(new java.math.BigDecimal(0)), classOf[java.math.BigDecimal])
            expectMsg(expectedResponse)
          }
        }

        describe("with a bigger long `value`") {
          it("should return a list with the provided value as classOf[Long]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", "55"), ("type", "bigdecimal"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse = TransformerResponse(List(new java.math.BigDecimal("55")),
                                                       classOf[java.math.BigDecimal])
            expectMsg(expectedResponse)
          }
        }

        describe("with a precision `value`") {
          it("should return a list with the provided value as classOf[Long]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", "55.21"), ("type", "bigdecimal"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse = TransformerResponse(List(new java.math.BigDecimal("55.21")),
                                                       classOf[java.math.BigDecimal])
            expectMsg(expectedResponse)
          }
        }

        describe("with a bigger precision `value`") {
          it("should return a list with the provided value as classOf[Long]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", "55.2145621923"), ("type", "bigdecimal"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse =
              TransformerResponse(List(new java.math.BigDecimal("55.2145621923")),
                                  classOf[java.math.BigDecimal])
            expectMsg(expectedResponse)
          }
        }

        describe("with a `value` and a wrong `type`") {
          it("should throw an exception") {
            intercept[AssertionError] {
              val actor = TestActorRef(Overwrite.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("value", "abc"), ("type", "bigdecimal"))
              actor ! StartTransformation(List(ByteString("a")),
                                          new TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))
              expectMsgType[TransformerResponse]
            }
          }
        }
      }

      describe("classOf[date]") {
        describe("with empty `value`") {
          it("should return a list with the value 0 as classOf[java.sql.Date]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", ""), ("type", "date"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse = TransformerResponse(List(java.sql.Date.valueOf("1970-01-01")),
                                                       classOf[java.sql.Date])
            expectMsg(expectedResponse)
          }
        }

        describe("with a `value`") {
          it("should return a list with the provided value as classOf[java.sql.Date]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", "2015-03-13"), ("type", "date"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse = TransformerResponse(List(java.sql.Date.valueOf("2015-03-13")),
                                                       classOf[java.sql.Date])
            expectMsg(expectedResponse)
          }
        }

        describe("with a `value` and a wrong `type`") {
          it("should throw an exception") {
            intercept[AssertionError] {
              val actor = TestActorRef(Overwrite.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("value", "00:00:00"), ("type", "date"))
              actor ! StartTransformation(List(ByteString("a")),
                                          new TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))
              expectMsgType[TransformerResponse]
            }
          }
        }
      }

      describe("classOf[time]") {
        describe("with empty `value`") {
          it("should return a list with the value 0 as classOf[java.sql.Time]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", ""), ("type", "time"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse =
              TransformerResponse(List(java.sql.Time.valueOf("00:00:00")), classOf[java.sql.Time])
            expectMsg(expectedResponse)
          }
        }

        describe("with a `value`") {
          it("should return a list with the provided value as classOf[java.sql.Time]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", "13:00:25"), ("type", "time"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse =
              TransformerResponse(List(java.sql.Time.valueOf("13:00:25")), classOf[java.sql.Time])
            expectMsg(expectedResponse)
          }
        }

        describe("with a `value` and a wrong `type`") {
          it("should throw an exception") {
            intercept[AssertionError] {
              val actor = TestActorRef(Overwrite.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("value", "1979-01-01"), ("type", "time"))
              actor ! StartTransformation(List(ByteString("a")),
                                          new TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))
              expectMsgType[TransformerResponse]
            }
          }
        }
      }

      describe("classOf[datetime]") {
        describe("with empty `value`") {
          it("should return a list with the value 0 as classOf[java.sql.Timestamp]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", ""), ("type", "datetime"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse =
              TransformerResponse(List(java.sql.Timestamp.valueOf("1970-01-01 00:00:00")),
                                  classOf[java.sql.Timestamp])
            expectMsg(expectedResponse)
          }
        }

        describe("with a `value`") {
          it("should return a list with the provided value as classOf[java.sql.Timestamp]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", "2015-09-24 13:00:25"), ("type", "datetime"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse =
              TransformerResponse(List(java.sql.Timestamp.valueOf("2015-09-24 13:00:25")),
                                  classOf[java.sql.Timestamp])
            expectMsg(expectedResponse)
          }
        }

        describe("with a `value` and a wrong `type`") {
          it("should throw an exception") {
            intercept[AssertionError] {
              val actor = TestActorRef(Overwrite.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("value", "00:00:00"), ("type", "datetime"))
              actor ! StartTransformation(List(ByteString("a")),
                                          new TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))
              expectMsgType[TransformerResponse]
            }
          }
        }
      }

      describe("classOf[byte]") {
        describe("with empty `value`") {
          it("should return a list with the an empty Array[Byte]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", ""), ("type", "byte"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse =
              TransformerResponse(List(Array.empty[Byte]), classOf[Array[Byte]])
            val r = expectMsgType[TransformerResponse]
            r.data.head shouldEqual expectedResponse.data.head
          }
        }

        describe("with a `value`") {
          it("should return a list with the provided value as classOf[Array[Byte]]") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", "ff f0 90 a0"), ("type", "byte"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse =
              TransformerResponse(List(Array(255.toByte, 240.toByte, 144.toByte, 160.toByte)),
                                  classOf[Array[Byte]])
            val r = expectMsgType[TransformerResponse]
            r.data.head shouldEqual expectedResponse.data.head
          }
        }
      }

      describe("classOf[None]") {
        describe("with empty `value`") {
          it("should return a list with the value None as None.getClass") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", ""), ("type", "none"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse = TransformerResponse(List(None), None.getClass)
            expectMsg(expectedResponse)
          }
        }

        describe("with a `value`") {
          it("should return a list with None as None.getClass") {
            val actor = TestActorRef(Overwrite.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)

            val params = List(("value", "None"), ("type", "none"))

            actor ! StartTransformation(List(ByteString("a")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
            val expectedResponse = TransformerResponse(List(None), None.getClass)
            expectMsg(expectedResponse)
          }
        }
      }
    }
  }
}
