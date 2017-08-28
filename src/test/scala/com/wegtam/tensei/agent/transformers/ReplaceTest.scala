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

class ReplaceTest extends ActorSpec {
  describe("Transformers") {
    describe("Replace") {
      describe("replaceSome") {
        describe("when given a count < 1") {
          it("should return the original string") {
            val regex   = "[aeiou]"
            val replace = "u"
            val count   = 0
            val search  = "We don't need no education!"
            Replace.replaceSome(regex, replace, count, search) shouldEqual search
          }
        }

        describe("when given a non matching regular expression") {
          it("should return the original string") {
            val regex   = "\\d+"
            val replace = "u"
            val count   = 0
            val search  = "We don't need no education!"
            Replace.replaceSome(regex, replace, count, search) shouldEqual search
          }
        }

        describe("when given proper parameters") {
          it("should replace 1 correctly") {
            val regex    = "[aeiou]"
            val replace  = "u"
            val count    = 1
            val search   = "We don't need no education!"
            val expected = "Wu don't need no education!"
            Replace.replaceSome(regex, replace, count, search) shouldEqual expected
          }

          it("should replace 2 correctly") {
            val regex    = "[aeiou]"
            val replace  = "u"
            val count    = 2
            val search   = "We don't need no education!"
            val expected = "Wu dun't need no education!"
            Replace.replaceSome(regex, replace, count, search) shouldEqual expected
          }

          it("should replace 3 correctly") {
            val regex    = "[aeiou]"
            val replace  = "u"
            val count    = 3
            val search   = "We don't need no education!"
            val expected = "Wu dun't nued no education!"
            Replace.replaceSome(regex, replace, count, search) shouldEqual expected
          }
        }
      }

      describe("when given an empty list") {
        describe("with empty parameters") {
          it("should return an empty string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            actor ! StartTransformation(List(),
                                        TransformerOptions(classOf[String], classOf[String]))

            val response = TransformerResponse(List(ByteString("")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with an empty search parameter") {
          it("should return an empty string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("replace", "bar"), ("count", "1"))

            actor ! StartTransformation(List(),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))

            val response = TransformerResponse(List(ByteString("")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with an empty replace parameter") {
          it("should return an empty string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("search", "'foo'"), ("count", "1"))

            actor ! StartTransformation(List(),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))

            val response = TransformerResponse(List(ByteString("")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with an empty count value") {
          it("should return an empty string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("search", "'foo'"), ("replace", "bar"))

            actor ! StartTransformation(List(),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))

            val response = TransformerResponse(List(ByteString("")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with specific parameters") {
          it("should return an empty string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("search", "'foo'"), ("replace", "bar"), ("count", "1"))

            actor ! StartTransformation(List(),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))

            val response = TransformerResponse(List(ByteString("")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }
      }

      describe("when given a string") {
        describe("without search parameter") {
          it("should return the source string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("replace", "bar"), ("count", "1"))

            actor ! StartTransformation(List(ByteString("This is the original source string!")),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))

            val response =
              TransformerResponse(List(ByteString("This is the original source string!")),
                                  classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("without replace parameter") {
          it("should return the source string without the search string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("search", "'original'"), ("count", "1"))

            actor ! StartTransformation(List(ByteString("This is the original source string!")),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))

            val response =
              TransformerResponse(List(ByteString("This is the  source string!")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("without count parameter") {
          it("should return the replaced string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("search", "'original'"), ("replace", "actual"))

            actor ! StartTransformation(List(ByteString("This is the original source string!")),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))

            val response =
              TransformerResponse(List(ByteString("This is the actual source string!")),
                                  classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with multiple search strings") {
          it("should return the replaced string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params =
              List(("search", "'original','actual'"), ("replace", "bar"), ("count", "1"))

            actor ! StartTransformation(
              List(ByteString("This is the original actual source string!")),
              TransformerOptions(classOf[String], classOf[String], params)
            )

            val response =
              TransformerResponse(List(ByteString("This is the bar bar source string!")),
                                  classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with multiple search strings that are separated with spaces") {
          it("should return the replaced string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params =
              List(("search", "'original', 'actual'"), ("replace", "bar"), ("count", "1"))

            actor ! StartTransformation(
              List(ByteString("This is the original actual source string!")),
              TransformerOptions(classOf[String], classOf[String], params)
            )

            val response =
              TransformerResponse(List(ByteString("This is the bar bar source string!")),
                                  classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with a search string that contains spaces") {
          it("should return the replaced string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("search", "' original '"), ("replace", "bar"), ("count", "1"))

            actor ! StartTransformation(
              List(ByteString("This is the original actual source string!")),
              TransformerOptions(classOf[String], classOf[String], params)
            )

            val response =
              TransformerResponse(List(ByteString("This is thebaractual source string!")),
                                  classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("without count parameter and multiple search occurences") {
          it("should return the replaced string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("search", "'original'"), ("replace", "bar"))

            actor ! StartTransformation(
              List(ByteString("This is the original original original original source string!")),
              TransformerOptions(classOf[String], classOf[String], params)
            )

            val response =
              TransformerResponse(List(ByteString("This is the bar bar bar bar source string!")),
                                  classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with count parameter and multiple occurences") {
          describe("with count set to 1") {
            it("should return the replaced string") {
              val actor = TestActorRef(Replace.props)

              actor ! PrepareForTransformation

              expectMsg(ReadyToTransform)

              val params = List(("search", "'original'"), ("replace", "bar"), ("count", "1"))

              actor ! StartTransformation(
                List(ByteString("This is the original original original original source string!")),
                TransformerOptions(classOf[String], classOf[String], params)
              )

              val response = TransformerResponse(
                List(ByteString("This is the bar original original original source string!")),
                classOf[String]
              )

              val d = expectMsgType[TransformerResponse]
              d.data shouldEqual response.data
            }
          }

          describe("with count set to 3") {
            it("should return the replaced string") {
              val actor = TestActorRef(Replace.props)

              actor ! PrepareForTransformation

              expectMsg(ReadyToTransform)

              val params = List(("search", "'original'"), ("replace", "bar"), ("count", "3"))

              actor ! StartTransformation(
                List(ByteString("This is the original original original original source string!")),
                TransformerOptions(classOf[String], classOf[String], params)
              )

              val response = TransformerResponse(
                List(ByteString("This is the bar bar bar original source string!")),
                classOf[String]
              )

              val d = expectMsgType[TransformerResponse]
              d.data shouldEqual response.data
            }
          }

          describe("with count set to 5") {
            it("should return the replaced string") {
              val actor = TestActorRef(Replace.props)

              actor ! PrepareForTransformation

              expectMsg(ReadyToTransform)

              val params = List(("search", "'original'"), ("replace", "bar"), ("count", "5"))

              actor ! StartTransformation(
                List(ByteString("This is the original original original original source string!")),
                TransformerOptions(classOf[String], classOf[String], params)
              )

              val response =
                TransformerResponse(List(ByteString("This is the bar bar bar bar source string!")),
                                    classOf[String])

              val d = expectMsgType[TransformerResponse]
              d.data shouldEqual response.data
            }
          }
        }
      }

      describe("with specific search strings") {
        describe("when given one round bracket") {
          it("should return the replaced string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("search", "'\\('"))

            actor ! StartTransformation(List(ByteString("(123) 456-789")),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))

            val response = TransformerResponse(List(ByteString("123) 456-789")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("when given two round brackets") {
          it("should return the replaced string") {
            val actor = TestActorRef(Replace.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("search", "'\\(','\\)'"))

            actor ! StartTransformation(List(ByteString("(123) 456-789")),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))

            val response = TransformerResponse(List(ByteString("123 456-789")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("when given a regex as search string") {
          it("should return the replaced string") {
            val actor = TestActorRef(Replace.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)
            val params = List(("search", "'\\w+'"), ("replace", "1"))
            actor ! StartTransformation(List(ByteString("test")),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))
            val response = TransformerResponse(List(ByteString("1")), classOf[String])
            val d        = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("when given a regex as search string II") {
          it("should return the replaced string") {
            val actor = TestActorRef(Replace.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)
            val params = List(("search", "'\\w+'"), ("replace", "22"))
            actor ! StartTransformation(List(ByteString("test test")),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))
            val response = TransformerResponse(List(ByteString("22 22")), classOf[String])
            val d        = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("when given a regex as search string and a count") {
          it("should replace only the specified number of occurences") {
            val actor = TestActorRef(Replace.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)
            val params = List(("search", "'[aeiou]'"), ("replace", "u"), ("count", "3"))
            actor ! StartTransformation(List(ByteString("We don't need no education!")),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))
            val response =
              TransformerResponse(List(ByteString("Wu dun't nued no education!")), classOf[String])
            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe(
          "when given a search string which is a substring of the replacement and a count value of 2"
        ) {
          it("should return the replaced string") {
            val actor = TestActorRef(Replace.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)
            val params = List(("search", "'e'"), ("replace", "ee"), ("count", "2"))
            actor ! StartTransformation(List(ByteString("test test test")),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))
            val response =
              TransformerResponse(List(ByteString("teest teest test")), classOf[String])
            val d = expectMsgType[TransformerResponse]
            d.data.head.asInstanceOf[ByteString].utf8String shouldEqual response.data.head
              .asInstanceOf[ByteString]
              .utf8String
          }
        }

        describe(
          "when given a search string which is a substring of the replacement and a count value of 4"
        ) {
          it("should return the replaced string") {
            val actor = TestActorRef(Replace.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)
            val params = List(("search", "'e'"), ("replace", "ee"), ("count", "4"))
            actor ! StartTransformation(
              List(ByteString("test fahrrad test cars test test test auto test")),
              TransformerOptions(classOf[String], classOf[String], params)
            )
            val response = TransformerResponse(
              List(ByteString("teest fahrrad teest cars teest teest test auto test")),
              classOf[String]
            )
            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("when the search string contains a comma") {
          it("should return the replaced string") {
            val actor = TestActorRef(Replace.props)
            actor ! PrepareForTransformation
            expectMsg(ReadyToTransform)
            val params = List(("search", "'a\\{1\\,2\\}'"), ("replace", "foobar"))
            actor ! StartTransformation(List(ByteString("this is a a{1,2} string")),
                                        TransformerOptions(classOf[String],
                                                           classOf[String],
                                                           params))
            val response =
              TransformerResponse(List(ByteString("this is a foobar string")), classOf[String])
            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }
      }
    }
  }
}
