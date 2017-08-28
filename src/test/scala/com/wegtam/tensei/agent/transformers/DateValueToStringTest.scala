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

import akka.testkit.{ EventFilter, TestActorRef }
import akka.util.ByteString
import com.wegtam.tensei.adt.TransformerOptions
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform,
  StartTransformation,
  TransformerResponse
}

class DateValueToStringTest extends ActorSpec {
  describe("DateValueToString") {
    describe("using an empty list") {
      it("should return an empty list") {
        val actor = TestActorRef(DateValueToString.props)
        actor ! PrepareForTransformation
        expectMsg(ReadyToTransform)
        actor ! StartTransformation(List(), TransformerOptions(classOf[String], classOf[String]))
        val response = TransformerResponse(List(), classOf[String])
        val d        = expectMsgType[TransformerResponse]
        d.data should be(response.data)
      }
    }

    describe("using an empty `format` parameter") {
      it("should return the input value as String") {
        val actor = TestActorRef(DateValueToString.props)
        actor ! PrepareForTransformation
        expectMsg(ReadyToTransform)
        val params = List(("format", ""))
        actor ! StartTransformation(List(java.sql.Date.valueOf("2016-04-27")),
                                    TransformerOptions(classOf[String], classOf[String], params))
        val response = TransformerResponse(List(ByteString("2016-04-27")), classOf[String])
        val d        = expectMsgType[TransformerResponse]
        d.data should be(response.data)
      }
    }

    describe("using a correct format") {
      describe("when giving one input value") {
        describe("using a Date") {
          describe("with new format") {
            it("should return the converted input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", "dd.MM.yyyy"))
              actor ! StartTransformation(List(java.sql.Date.valueOf("2016-04-27")),
                                          TransformerOptions(classOf[String],
                                                             classOf[String],
                                                             params))
              val response = TransformerResponse(List(ByteString("27.04.2016")), classOf[String])
              val d        = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }

          describe("with empty format") {
            it("should return the input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", ""))
              actor ! StartTransformation(List(java.sql.Date.valueOf("2016-04-27")),
                                          TransformerOptions(classOf[String],
                                                             classOf[String],
                                                             params))
              val response = TransformerResponse(List(ByteString("2016-04-27")), classOf[String])
              val d        = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }
        }

        describe("using Time") {
          describe("with new format") {
            it("should return the converted input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", "HH:mm"))
              actor ! StartTransformation(List(java.sql.Time.valueOf("13:22:22")),
                                          TransformerOptions(classOf[String],
                                                             classOf[String],
                                                             params))
              val response = TransformerResponse(List(ByteString("13:22")), classOf[String])
              val d        = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }

          describe("with empty format") {
            it("should return the converted input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", ""))
              actor ! StartTransformation(List(java.sql.Time.valueOf("13:22:22")),
                                          TransformerOptions(classOf[String],
                                                             classOf[String],
                                                             params))
              val response = TransformerResponse(List(ByteString("13:22:22")), classOf[String])
              val d        = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }
        }

        describe("using Timestamp") {
          describe("with new format") {
            it("should return the converted input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", "dd.MM.yyyy h:mm a"))
              actor ! StartTransformation(List(java.sql.Timestamp.valueOf("2016-04-27 13:22:22")),
                                          TransformerOptions(classOf[String],
                                                             classOf[String],
                                                             params))
              val response =
                TransformerResponse(List(ByteString("27.04.2016 1:22 PM")), classOf[String])
              val d = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }

          describe("with different new format") {
            it("should return the converted input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", "dd.MM.yyyy HH:mm:ss"))
              actor ! StartTransformation(List(java.sql.Timestamp.valueOf("2016-04-27 13:22:22")),
                                          TransformerOptions(classOf[String],
                                                             classOf[String],
                                                             params))
              val response =
                TransformerResponse(List(ByteString("27.04.2016 13:22:22")), classOf[String])
              val d = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }

          describe("with empty format") {
            it("should return the converted input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", ""))
              actor ! StartTransformation(List(java.sql.Timestamp.valueOf("2016-04-27 13:22:22")),
                                          TransformerOptions(classOf[String],
                                                             classOf[String],
                                                             params))
              val response =
                TransformerResponse(List(ByteString("2016-04-27 13:22:22.0")), classOf[String])
              val d = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }
        }

      }

      describe("when giving multiple input values") {
        describe("using a Date") {
          describe("with new format") {
            it("should return the converted input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", "dd.MM.yyyy"))
              actor ! StartTransformation(
                List(java.sql.Date.valueOf("2016-04-27"),
                     java.sql.Date.valueOf("2016-04-26"),
                     java.sql.Date.valueOf("2016-04-25")),
                TransformerOptions(classOf[String], classOf[String], params)
              )
              val response = TransformerResponse(List(ByteString("27.04.2016"),
                                                      ByteString("26.04.2016"),
                                                      ByteString("25.04.2016")),
                                                 classOf[String])
              val d = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }

          describe("with empty format") {
            it("should return the input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", ""))
              actor ! StartTransformation(
                List(java.sql.Date.valueOf("2016-04-27"),
                     java.sql.Date.valueOf("2016-04-26"),
                     java.sql.Date.valueOf("2016-04-25")),
                TransformerOptions(classOf[String], classOf[String], params)
              )
              val response = TransformerResponse(List(ByteString("2016-04-27"),
                                                      ByteString("2016-04-26"),
                                                      ByteString("2016-04-25")),
                                                 classOf[String])
              val d = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }
        }

        describe("using Time") {
          describe("with new format") {
            it("should return the converted input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", "HH:mm"))
              actor ! StartTransformation(
                List(java.sql.Time.valueOf("13:22:22"),
                     java.sql.Time.valueOf("12:22:22"),
                     java.sql.Time.valueOf("11:22:22")),
                TransformerOptions(classOf[String], classOf[String], params)
              )
              val response = TransformerResponse(List(ByteString("13:22"),
                                                      ByteString("12:22"),
                                                      ByteString("11:22")),
                                                 classOf[String])
              val d = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }

          describe("with empty format") {
            it("should return the converted input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", ""))
              actor ! StartTransformation(
                List(java.sql.Time.valueOf("13:22:22"),
                     java.sql.Time.valueOf("12:22:22"),
                     java.sql.Time.valueOf("11:22:22")),
                TransformerOptions(classOf[String], classOf[String], params)
              )
              val response = TransformerResponse(List(ByteString("13:22:22"),
                                                      ByteString("12:22:22"),
                                                      ByteString("11:22:22")),
                                                 classOf[String])
              val d = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }
        }

        describe("using Timestamp") {
          describe("with new format") {
            it("should return the converted input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", "dd.MM.yyyy h:mm a"))
              actor ! StartTransformation(
                List(java.sql.Timestamp.valueOf("2016-04-27 13:22:22"),
                     java.sql.Timestamp.valueOf("2016-04-26 12:22:22"),
                     java.sql.Timestamp.valueOf("2016-04-25 11:22:22")),
                TransformerOptions(classOf[String], classOf[String], params)
              )
              val response = TransformerResponse(List(ByteString("27.04.2016 1:22 PM"),
                                                      ByteString("26.04.2016 12:22 PM"),
                                                      ByteString("25.04.2016 11:22 AM")),
                                                 classOf[String])
              val d = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }

          describe("with different new format") {
            it("should return the converted input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", "dd.MM.yyyy HH:mm:ss"))
              actor ! StartTransformation(
                List(java.sql.Timestamp.valueOf("2016-04-27 13:22:22"),
                     java.sql.Timestamp.valueOf("2016-04-26 12:22:22"),
                     java.sql.Timestamp.valueOf("2016-04-25 11:22:22")),
                TransformerOptions(classOf[String], classOf[String], params)
              )
              val response = TransformerResponse(List(ByteString("27.04.2016 13:22:22"),
                                                      ByteString("26.04.2016 12:22:22"),
                                                      ByteString("25.04.2016 11:22:22")),
                                                 classOf[String])
              val d = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }

          describe("with empty format") {
            it("should return the converted input value as String") {
              val actor = TestActorRef(DateValueToString.props)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
              val params = List(("format", ""))
              actor ! StartTransformation(
                List(java.sql.Timestamp.valueOf("2016-04-27 13:22:22"),
                     java.sql.Timestamp.valueOf("2016-04-26 12:22:22"),
                     java.sql.Timestamp.valueOf("2016-04-25 11:22:22")),
                TransformerOptions(classOf[String], classOf[String], params)
              )
              val response = TransformerResponse(List(ByteString("2016-04-27 13:22:22.0"),
                                                      ByteString("2016-04-26 12:22:22.0"),
                                                      ByteString("2016-04-25 11:22:22.0")),
                                                 classOf[String])
              val d = expectMsgType[TransformerResponse]
              d.data should be(response.data)
            }
          }
        }
      }
    }

    describe("using an incorrect format") {
      describe("using a Date") {
        describe("with incorrect format") {
          it("should throw an exception") {
            val actor = TestActorRef(DateValueToString.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)
            val params = List(("format", "ll.rr.oooo"))
            EventFilter[IllegalArgumentException](occurrences = 1) intercept {
              actor ! StartTransformation(List(java.sql.Date.valueOf("2016-04-27")),
                                          TransformerOptions(classOf[String],
                                                             classOf[String],
                                                             params))
              TransformerResponse(List(ByteString("27.04.2016")), classOf[String])
            }
          }
        }
      }
    }
  }

}
