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

class MergeAndExtractByRegExTest extends ActorSpec {
  describe("Transfomers") {
    describe("MergeAndExtractByRegEx") {
      describe("when given an empty list") {
        it("should return an empty string") {
          val actor = TestActorRef(MergeAndExtractByRegEx.props)

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
            val actor = TestActorRef(MergeAndExtractByRegEx.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("regexp", ""))

            actor ! StartTransformation(List(),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))

            val response = TransformerResponse(List(ByteString("")), classOf[String])

            expectMsg(response)
          }
        }

        describe("with a group selection") {
          it("should return an empty string") {
            val actor = TestActorRef(MergeAndExtractByRegEx.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("groups", "1"))

            actor ! StartTransformation(List(),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))

            val response = TransformerResponse(List(ByteString("")), classOf[String])

            expectMsg(response)
          }
        }

        describe("with multiple groups") {
          it("should return an empty string") {
            val actor = TestActorRef(MergeAndExtractByRegEx.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("groups", "1,2"))

            actor ! StartTransformation(List(),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))

            val response = TransformerResponse(List(ByteString("")), classOf[String])

            expectMsg(response)
          }
        }

        describe("with multiple groups and a filler") {
          it("should return an empty string") {
            val actor = TestActorRef(MergeAndExtractByRegEx.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("groups", "0,1"), ("filler", "-"))

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
        it("should return the given string") {
          val actor = TestActorRef(MergeAndExtractByRegEx.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          actor ! StartTransformation(List("Das ist ein [Haus], mit :drei: Fenstern!"),
                                      new TransformerOptions(classOf[String], classOf[String]))

          val response =
            TransformerResponse(List(ByteString("Das ist ein [Haus], mit :drei: Fenstern!")),
                                classOf[String])

          expectMsg(response)
        }

        describe("with only one group in regexp") {
          it("should return the matched parts of the string") {
            val actor = TestActorRef(MergeAndExtractByRegEx.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("regexp", ".*(Haus).*"))

            actor ! StartTransformation(
              List(ByteString("Das ist ein [Haus], mit :drei: Fenstern!")),
              new TransformerOptions(classOf[String], classOf[String], params)
            )

            val response = TransformerResponse(List(ByteString("Haus")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with a regexp") {
          it("should return the matched parts of the string") {
            val actor = TestActorRef(MergeAndExtractByRegEx.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("regexp", ".*(Haus).*(Fenstern).*"))

            actor ! StartTransformation(
              List(ByteString("Das ist ein [Haus], mit :drei: Fenstern!")),
              new TransformerOptions(classOf[String], classOf[String], params)
            )

            val response =
              TransformerResponse(List(ByteString("HausFenstern")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with a filler") {
          it("should return the matched parts connected with the filler") {
            val actor = TestActorRef(MergeAndExtractByRegEx.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("regexp", ".*(Haus).*(Fenstern).*"), ("filler", "-"))

            actor ! StartTransformation(
              List(ByteString("Das ist ein [Haus], mit :drei: Fenstern!")),
              new TransformerOptions(classOf[String], classOf[String], params)
            )

            val response =
              TransformerResponse(List(ByteString("Haus-Fenstern")), classOf[String])

            val d = expectMsgType[TransformerResponse]
            d.data shouldEqual response.data
          }
        }

        describe("with a regexp and a selected amount of groups") {
          it(
            "should return the selected parts, connected with the filler and only the specified groups"
          ) {
            val actor = TestActorRef(MergeAndExtractByRegEx.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params =
              List(("regexp", ".*(Das).*(Haus).*(Fenster).*"), ("filler", "#"), ("groups", "0,2"))

            actor ! StartTransformation(
              List(ByteString("Das ist ein [Haus], mit :drei: Fenstern!")),
              new TransformerOptions(classOf[String], classOf[String], params)
            )

            val response =
              TransformerResponse(List(ByteString("Das#Fenster")), classOf[String])

            expectMsg(response)
          }
        }

        describe("with a regexp and a selected amount of groups 2") {
          it(
            "should return the selected parts, connected with the filler and only the specified groups 2"
          ) {
            val actor = TestActorRef(MergeAndExtractByRegEx.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("regexp", "-(\\d{4,})(-[\\w]+|-[\\D][\\w]+-[\\w]+|\\w{0,0})$"),
                              ("groups", "0"))

            actor ! StartTransformation(List(ByteString("Birk-128261-403137-37")),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))

            val response = TransformerResponse(List(ByteString("403137")), classOf[String])

            expectMsg(response)
          }
        }

        describe("with a regexp and a filler and specified groups that are incorrect") {
          it("should return the matched parts connected with the filler") {
            val actor = TestActorRef(MergeAndExtractByRegEx.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("regexp", ".*(Das).*(Haus).*(Fenster).*"),
                              ("filler", "#"),
                              ("groups", "0,2,5"))

            actor ! StartTransformation(
              List(ByteString("Das ist eine [Haus], mit :drei: Fenstern!")),
              new TransformerOptions(classOf[String], classOf[String], params)
            )

            val response =
              TransformerResponse(List(ByteString("Das#Fenster")), classOf[String])

            expectMsg(response)
          }
        }
      }

      describe("using street names and a simple regex and groups 0,1") {
        it("should return the correct data") {
          val actor = TestActorRef(MergeAndExtractByRegEx.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)

          val inputData = List(
            ByteString("Dominikanerbastei 22"),
            ByteString("Webergasse 1"),
            ByteString("Mohandas Karamchand Gandhi Straße 23"),
            ByteString("Arnulf-Klett-Platz 1 (Die Klett-Passage am Hauptbahnhof)")
          )
          val expectedData = List(
            ByteString("Dominikanerbastei"),
            ByteString("Webergasse"),
            ByteString("Mohandas Karamchand Gandhi Straße"),
            ByteString("Arnulf-Klett-Platz (Die Klett-Passage am Hauptbahnhof)")
          )
          val params = List(
            ("regexp", "^([\\D\\s]+)\\s[\\d-]+([\\D\\s-\\(\\)]*)"),
            ("groups", "0,1")
          )

          actor ! StartTransformation(inputData,
                                      new TransformerOptions(classOf[String],
                                                             classOf[String],
                                                             params))

          val response = expectMsgType[TransformerResponse]
          response.data should be(expectedData)
        }
      }

      describe("using some complex product sku samples") {
        describe("when extracting article IDs from the sku") {
          it("should return the exact IDs") {

            val actor = TestActorRef(MergeAndExtractByRegEx.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val data = List(
              (ByteString("Cipo-C1033-10648-L32W29"), ByteString("10648")),
              (ByteString("NOB-DAVID-SCHWARZ-50034-42"), ByteString("50034")),
              (ByteString("AIN-191103-01-300975-M"), ByteString("300975")),
              (ByteString("Stetson-Paradise-khaki-911509-L"), ByteString("911509")),
              (ByteString("RB-R-41018-NEU-40201-L32W36"), ByteString("40201")),
              (ByteString("ZWEI-Mademoiselle-M12-blue-801941"), ByteString("801941")),
              (ByteString("RB-M1005-203819-WHITE-XL"), ByteString("203819")),
              (ByteString("CIPO-C894-13487-L34-W32"), ByteString("13487")),
              (ByteString("Birk-128261-403137-37"), ByteString("403137")),
              (ByteString("AIN-100101-07-300972-L"), ByteString("300972")),
              (ByteString("Khujo-Dyani-1149JK163-450-204048-M"), ByteString("204048")),
              (ByteString("rb-6013-schwarz-300812-xl"), ByteString("300812")),
              (ByteString("RB-R41223-203916-BLUE-XL"), ByteString("203916")),
              (ByteString("RB-41451-Grau-300997-XXL"), ByteString("300997")),
              (ByteString("Khujo-Dairi-1066CO163-325-11965-L"), ByteString("11965")),
              (ByteString("rb-2111-weiss-202709-"), ByteString("202709")),
              (ByteString("DC-NET-53024-42.5"), ByteString("53024")),
              (ByteString("DC-Woodland-52861-EU44.5"), ByteString("52861")),
              (ByteString("Yakuza-GLHOB 7127-202350-weiß-S-Neu"), ByteString("202350")),
              (ByteString("Yakuza-JB 7033-300718-indigio moon-M-Neu"), ByteString("300718"))
            )

            val params = List(
              ("regexp",
               "-(\\d{5,})(-[\\w\\.]+|-[\\D][\\w]*-[\\w\\.]+|-[\\d\\.]{1,4}-[\\d\\.]{1,4}|-[-\\sa-zA-Zß]+|-[\\D][\\w]*-[\\w\\.]{1,4}-[\\D][\\w\\.]*|-|\\w{0,0})$"),
              ("groups", "0")
            )

            data.foreach { d =>
              val response = TransformerResponse(List(d._2), classOf[String])
              actor ! StartTransformation(List(d._1),
                                          new TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))
              expectMsg(response)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
            }

          }
        }

        describe("when extracting sizes from the sku") {
          it("should return the exact IDs") {

            val actor = TestActorRef(MergeAndExtractByRegEx.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val data = List(
              (ByteString("Cipo-C1033-10648-L32W29"), ByteString("L32W29")),
              (ByteString("NOB-DAVID-SCHWARZ-50034-42"), ByteString("42")),
              (ByteString("AIN-191103-01-300975-M"), ByteString("M")),
              (ByteString("Stetson-Paradise-khaki-911509-L"), ByteString("L")),
              (ByteString("RB-R-41018-NEU-40201-L32W36"), ByteString("L32W36")),
              (ByteString("ZWEI-Mademoiselle-M12-blue-801941"), ByteString("")),
              (ByteString("RB-M1005-203819-WHITE-XL"), ByteString("WHITE-XL")),
              (ByteString("CIPO-C894-13487-L34-W32"), ByteString("L34-W32")),
              (ByteString("Birk-128261-403137-37"), ByteString("37")),
              (ByteString("AIN-100101-07-300972-L"), ByteString("L")),
              (ByteString("Khujo-Dyani-1149JK163-450-204048-M"), ByteString("M")),
              (ByteString("rb-6013-schwarz-300812-xl"), ByteString("xl")),
              (ByteString("RB-R41223-203916-BLUE-XL"), ByteString("BLUE-XL")),
              (ByteString("RB-41451-Grau-300997-XXL"), ByteString("XXL")),
              (ByteString("Khujo-Dairi-1066CO163-325-11965-L"), ByteString("L")),
              (ByteString("rb-2111-weiss-202709-"), ByteString("")),
              (ByteString("DC-NET-53024-42.5"), ByteString("42.5")),
              (ByteString("DC-Woodland-52861-EU44.5"), ByteString("EU44.5")),
              (ByteString("Yakuza-GLHOB 7127-202350-weiß-S-Neu"), ByteString("weiß-S-Neu")),
              (ByteString("Yakuza-JB 7033-300718-indigio moon-M-Neu"),
               ByteString("indigio moon-M-Neu"))
            )

            val params = List(
              ("regexp",
               "-\\d{5,}-([\\w\\.]+|[\\D][\\w]*-[\\w\\.]+|[\\d\\.]{1,4}-[\\d\\.]{1,4}|[-\\sa-zA-Zß]+|[\\D][\\w]*-[\\w\\.]{1,4}-[\\D][\\w\\.]*|\\w{0,0})$"),
              ("groups", "0")
            )

            data.foreach { d =>
              val response = TransformerResponse(List(d._2), classOf[String])
              actor ! StartTransformation(List(d._1),
                                          new TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 params))
              expectMsg(response)
              actor ! PrepareForTransformation
              expectMsg(ReadyToTransform)
            }

          }
        }
      }
    }
  }
}
