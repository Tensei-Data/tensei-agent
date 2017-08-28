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

package com.wegtam.tensei.agent.analyzer

import akka.testkit.TestActorRef
import com.wegtam.tensei.adt.StatsResult.{ BasicStatisticsResult, StatsResultString }
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.Stats.StatsMessages.GetStatisticResult
import com.wegtam.tensei.agent.adt.ParserDataContainer
import com.wegtam.tensei.agent.analyzer.GenericAnalyzer.NumericAnalyzerMessages.AnalyzeData
import com.wegtam.tensei.agent.helpers.{ GenericHelpers, XmlHelpers }
import org.dfasdl.utils.{ AttributeNames, ElementNames }

class StringAnalyzerTest extends ActorSpec with XmlHelpers with GenericHelpers {
  describe("StringAnalyzer") {
    describe("AnalyzeData") {
      describe("with one data message") {
        describe("that is empty") {
          it("should work") {
            val doc     = createNewDocument()
            val element = doc.createElement(ElementNames.STRING)
            element.setAttribute("id", "MyElement")

            val analyzer = TestActorRef(StringAnalyzer.props("MyElement", element))
            val data = ParserDataContainer(
              "",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )

            analyzer ! AnalyzeData(data)

            analyzer ! GetStatisticResult

            val response = new StatsResultString(
              "MyElement",
              BasicStatisticsResult(1, Option(1), Option(0.0), Option(0.0), Option(0.0), None)
            )

            expectMsg(response)
          }
        }

        describe("that is string number") {
          it("should work") {
            val doc     = createNewDocument()
            val element = doc.createElement(ElementNames.STRING)
            element.setAttribute("id", "MyElement")

            val analyzer = TestActorRef(StringAnalyzer.props("MyElement", element))
            val data = ParserDataContainer(
              "5",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )

            analyzer ! AnalyzeData(data)

            analyzer ! GetStatisticResult

            val response = new StatsResultString("MyElement",
                                                 BasicStatisticsResult(1,
                                                                       Option(1),
                                                                       Option(1.0),
                                                                       Option(1.0),
                                                                       Option(1.0)))

            expectMsg(response)
          }
        }

        describe("that is string text") {
          it("should work") {
            val doc     = createNewDocument()
            val element = doc.createElement(ElementNames.STRING)
            element.setAttribute("id", "MyElement")

            val analyzer = TestActorRef(StringAnalyzer.props("MyElement", element))
            val data = ParserDataContainer(
              "This is a text!",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )

            analyzer ! AnalyzeData(data)

            analyzer ! GetStatisticResult

            val response = new StatsResultString("MyElement",
                                                 BasicStatisticsResult(1,
                                                                       Option(1),
                                                                       Option(15.0),
                                                                       Option(15.0),
                                                                       Option(15.0)))

            expectMsg(response)
          }
        }
      }

      describe("with multiple data messages") {
        describe("example of 3 names") {
          it("should work") {
            val doc     = createNewDocument()
            val element = doc.createElement(ElementNames.STRING)
            element.setAttribute("id", "MyElement")

            val analyzer = TestActorRef(StringAnalyzer.props("MyElement", element))
            val data = ParserDataContainer(
              "Smith",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data2 = ParserDataContainer(
              "von Hohenstein",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data3 = ParserDataContainer(
              "Xi",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )

            analyzer ! AnalyzeData(data)
            analyzer ! AnalyzeData(data2)
            analyzer ! AnalyzeData(data3)

            analyzer ! GetStatisticResult

            val response = new StatsResultString("MyElement",
                                                 BasicStatisticsResult(3,
                                                                       Option(3),
                                                                       Option(2.0),
                                                                       Option(14.0),
                                                                       Option(7.0)))

            expectMsg(response)
          }
        }
      }

      describe("with a maximum length at the element") {
        it("should work") {
          val doc     = createNewDocument()
          val element = doc.createElement(ElementNames.STRING)
          element.setAttribute("id", "MyElement")
          element.setAttribute(AttributeNames.MAX_LENGTH, "7")

          val analyzer = TestActorRef(StringAnalyzer.props("MyElement", element))
          val data = ParserDataContainer(
            "Smith",
            "MyElement",
            Option("ID"),
            -1L,
            Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
          )
          val data2 = ParserDataContainer(
            "von Hohenstein",
            "MyElement",
            Option("ID"),
            -1L,
            Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
          )
          val data3 = ParserDataContainer(
            "Xi",
            "MyElement",
            Option("ID"),
            -1L,
            Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
          )

          analyzer ! AnalyzeData(data)
          analyzer ! AnalyzeData(data2)
          analyzer ! AnalyzeData(data3)

          analyzer ! GetStatisticResult

          val response = new StatsResultString("MyElement",
                                               BasicStatisticsResult(3,
                                                                     Option(3),
                                                                     Option(2.0),
                                                                     Option(7.0),
                                                                     Option(4.666666666666667)))

          expectMsg(response)
        }
      }

      describe("with a trim at the element") {
        it("should work") {
          val doc     = createNewDocument()
          val element = doc.createElement(ElementNames.STRING)
          element.setAttribute("id", "MyElement")
          element.setAttribute(AttributeNames.TRIM, "both")

          val analyzer = TestActorRef(StringAnalyzer.props("MyElement", element))
          val data = ParserDataContainer(
            " Smith ",
            "MyElement",
            Option("ID"),
            -1L,
            Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
          )
          val data2 = ParserDataContainer(
            "  von Hohenstein      ",
            "MyElement",
            Option("ID"),
            -1L,
            Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
          )
          val data3 = ParserDataContainer(
            " Xi",
            "MyElement",
            Option("ID"),
            -1L,
            Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
          )

          analyzer ! AnalyzeData(data)
          analyzer ! AnalyzeData(data2)
          analyzer ! AnalyzeData(data3)

          analyzer ! GetStatisticResult

          val response = new StatsResultString(
            "MyElement",
            BasicStatisticsResult(3, Option(3), Option(2.0), Option(14.0), Option(7.0))
          )

          expectMsg(response)
        }
      }
    }
  }

}
