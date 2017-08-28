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

import java.util.Locale

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

class LowerOrUpperTest extends ActorSpec {
  describe("Transformer") {
    describe("LowerOrUpper") {
      describe("without src string") {
        it("should return an empty string") {
          val actor = TestActorRef(LowerOrUpper.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          actor ! StartTransformation(List(), TransformerOptions(classOf[String], classOf[String]))
          val response = TransformerResponse(List(ByteString("")), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("without `perform` parameter") {
        it("should return the string") {
          val actor = TestActorRef(LowerOrUpper.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          actor ! StartTransformation(List(ByteString("Foo")),
                                      TransformerOptions(classOf[String], classOf[String]))
          val response = TransformerResponse(List(ByteString("Foo")), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("with parameter `perform`") {
        describe("with parameter `locale`") {
          val locales = Locale.getAvailableLocales

          locales.foreach { locale =>
            describe(s"using locale ${locale.toLanguageTag}") {
              describe("and `perform` = `lower`") {
                it("should return the correct value") {
                  val actor = TestActorRef(LowerOrUpper.props)
                  actor ! PrepareForTransformation
                  expectMsg(ReadyToTransform)
                  val params = List(("perform", "lower"), ("locale", locale.toLanguageTag))
                  actor ! StartTransformation(List(ByteString("Foo BAR")),
                                              TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))
                  val response = TransformerResponse(List(ByteString("foo bar")), classOf[String])
                  val d        = expectMsgType[TransformerResponse]
                  d.data shouldEqual response.data
                  system.stop(actor)
                }
              }

              describe("and `perform` = `upper`") {
                it("should return the correct value") {
                  val actor = TestActorRef(LowerOrUpper.props)
                  actor ! PrepareForTransformation
                  expectMsg(ReadyToTransform)
                  val params = List(("perform", "upper"), ("locale", locale.toLanguageTag))
                  actor ! StartTransformation(List(ByteString("Foo BAR")),
                                              TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))
                  val response = TransformerResponse(List(ByteString("FOO BAR")), classOf[String])
                  val d        = expectMsgType[TransformerResponse]
                  d.data shouldEqual response.data
                  system.stop(actor)
                }
              }

              describe("and `perform` = `firstupper`") {
                it("should return the correct value") {
                  val actor = TestActorRef(LowerOrUpper.props)
                  actor ! PrepareForTransformation
                  expectMsg(ReadyToTransform)
                  val params = List(("perform", "firstupper"), ("locale", locale.toLanguageTag))
                  actor ! StartTransformation(List(ByteString("Foo BAR")),
                                              TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))
                  val response = TransformerResponse(List(ByteString("Foo BAR")), classOf[String])
                  val d        = expectMsgType[TransformerResponse]
                  d.data shouldEqual response.data
                  system.stop(actor)
                }
              }

              describe("and `perform` = `firstlower`") {
                it("should return the correct value") {
                  val actor = TestActorRef(LowerOrUpper.props)
                  actor ! PrepareForTransformation
                  expectMsg(ReadyToTransform)
                  val params = List(("perform", "firstlower"), ("locale", locale.toLanguageTag))
                  actor ! StartTransformation(List(ByteString("Foo BAR")),
                                              TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))
                  val response = TransformerResponse(List(ByteString("foo BAR")), classOf[String])
                  val d        = expectMsgType[TransformerResponse]
                  d.data shouldEqual response.data
                  system.stop(actor)
                }
              }
            }
          }
        }

        describe("without parameter `locale`") {
          describe("`lower` value") {
            describe("without src string") {
              it("should return the unchanged string") {
                val actor = TestActorRef(LowerOrUpper.props)
                actor ! PrepareForTransformation
                expectMsg(ReadyToTransform)
                val params = List(("perform", "lower"))
                actor ! StartTransformation(List(),
                                            TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
                val response = TransformerResponse(List(ByteString("")), classOf[String])
                val d        = expectMsgType[TransformerResponse]
                d.data shouldEqual response.data
              }
            }

            describe("with valid src string that already has lower characters") {
              it("should return the unchanged string") {
                val actor = TestActorRef(LowerOrUpper.props)
                actor ! PrepareForTransformation
                expectMsg(ReadyToTransform)
                val params = List(("perform", "lower"))
                actor ! StartTransformation(List(ByteString("foo")),
                                            TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
                val response = TransformerResponse(List(ByteString("foo")), classOf[String])
                val d        = expectMsgType[TransformerResponse]
                d.data shouldEqual response.data
              }
            }

            describe("with valid src string that has upper characters") {
              it("should return the changed string") {
                val actor = TestActorRef(LowerOrUpper.props)
                actor ! PrepareForTransformation
                expectMsg(ReadyToTransform)
                val params = List(("perform", "lower"))
                actor ! StartTransformation(List(ByteString("Foo BAR")),
                                            TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
                val response = TransformerResponse(List(ByteString("foo bar")), classOf[String])
                val d        = expectMsgType[TransformerResponse]
                d.data shouldEqual response.data
              }
            }
          }

          describe("`upper` value") {
            describe("without src string") {
              it("should return the unchanged string") {
                val actor = TestActorRef(LowerOrUpper.props)
                actor ! PrepareForTransformation
                expectMsg(ReadyToTransform)
                val params = List(("perform", "upper"))
                actor ! StartTransformation(List(),
                                            TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
                val response = TransformerResponse(List(ByteString("")), classOf[String])
                val d        = expectMsgType[TransformerResponse]
                d.data shouldEqual response.data
              }
            }

            describe("with valid src string that already has upper characters") {
              it("should return the unchanged string") {
                val actor = TestActorRef(LowerOrUpper.props)
                actor ! PrepareForTransformation
                expectMsg(ReadyToTransform)
                val params = List(("perform", "upper"))
                actor ! StartTransformation(List(ByteString("FOO")),
                                            TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
                val response = TransformerResponse(List(ByteString("FOO")), classOf[String])
                val d        = expectMsgType[TransformerResponse]
                d.data shouldEqual response.data
              }
            }

            describe("with valid src string that has lower characters") {
              it("should return the changed string") {
                val actor = TestActorRef(LowerOrUpper.props)
                actor ! PrepareForTransformation
                expectMsg(ReadyToTransform)
                val params = List(("perform", "upper"))
                actor ! StartTransformation(List(ByteString("Foo bar")),
                                            TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
                val response = TransformerResponse(List(ByteString("FOO BAR")), classOf[String])
                val d        = expectMsgType[TransformerResponse]
                d.data shouldEqual response.data
              }
            }
          }

          describe("`firstlower` value") {
            describe("without src string") {
              it("should return the unchanged string") {
                val actor = TestActorRef(LowerOrUpper.props)
                actor ! PrepareForTransformation
                expectMsg(ReadyToTransform)
                val params = List(("perform", "firstlower"))
                actor ! StartTransformation(List(),
                                            TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
                val response = TransformerResponse(List(ByteString("")), classOf[String])
                val d        = expectMsgType[TransformerResponse]
                d.data shouldEqual response.data
              }
            }

            describe("with valid src string that already has lower first character") {
              it("should return the unchanged string") {
                val actor = TestActorRef(LowerOrUpper.props)
                actor ! PrepareForTransformation
                expectMsg(ReadyToTransform)
                val params = List(("perform", "firstlower"))
                actor ! StartTransformation(List(ByteString("fOO")),
                                            TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
                val response = TransformerResponse(List(ByteString("fOO")), classOf[String])
                val d        = expectMsgType[TransformerResponse]
                d.data shouldEqual response.data
              }
            }

            describe("with valid src string that has upper first character") {
              it("should return the changed string") {
                val actor = TestActorRef(LowerOrUpper.props)
                actor ! PrepareForTransformation
                expectMsg(ReadyToTransform)
                val params = List(("perform", "firstlower"))
                actor ! StartTransformation(List(ByteString("Foo Bar")),
                                            TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
                val response = TransformerResponse(List(ByteString("foo Bar")), classOf[String])
                val d        = expectMsgType[TransformerResponse]
                d.data shouldEqual response.data
              }
            }
          }

          describe("`firstupper` value") {
            describe("without src string") {
              it("should return the unchanged string") {
                val actor = TestActorRef(LowerOrUpper.props)
                actor ! PrepareForTransformation
                expectMsg(ReadyToTransform)
                val params = List(("perform", "firstupper"))
                actor ! StartTransformation(List(),
                                            TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
                val response = TransformerResponse(List(ByteString("")), classOf[String])
                val d        = expectMsgType[TransformerResponse]
                d.data shouldEqual response.data
              }
            }

            describe("with valid src string that already has upper first character") {
              it("should return the unchanged string") {
                val actor = TestActorRef(LowerOrUpper.props)
                actor ! PrepareForTransformation
                expectMsg(ReadyToTransform)
                val params = List(("perform", "firstupper"))
                actor ! StartTransformation(List(ByteString("FOO")),
                                            TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
                val response = TransformerResponse(List(ByteString("FOO")), classOf[String])
                val d        = expectMsgType[TransformerResponse]
                d.data shouldEqual response.data
              }
            }

            describe("with valid src string that has lower first character") {
              it("should return the changed string") {
                val actor = TestActorRef(LowerOrUpper.props)
                actor ! PrepareForTransformation
                expectMsg(ReadyToTransform)
                val params = List(("perform", "firstupper"))
                actor ! StartTransformation(List(ByteString("foo Bar")),
                                            TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))
                val response = TransformerResponse(List(ByteString("Foo Bar")), classOf[String])
                val d        = expectMsgType[TransformerResponse]
                d.data shouldEqual response.data
              }
            }
          }
        }
      }
    }
  }
}
