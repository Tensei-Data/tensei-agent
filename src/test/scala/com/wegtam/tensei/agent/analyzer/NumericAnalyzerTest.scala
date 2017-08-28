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
import com.wegtam.tensei.adt.StatsResult.{
  BasicStatisticsResult,
  StatisticErrors,
  StatsResultNumeric
}
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.Stats.StatsMessages.GetStatisticResult
import com.wegtam.tensei.agent.adt.ParserDataContainer
import com.wegtam.tensei.agent.analyzer.GenericAnalyzer.NumericAnalyzerMessages.AnalyzeData
import com.wegtam.tensei.agent.helpers.{ GenericHelpers, XmlHelpers }
import org.dfasdl.utils.{ AttributeNames, ElementNames }

class NumericAnalyzerTest extends ActorSpec with XmlHelpers with GenericHelpers {
  describe("NumericAnalyzer") {
    describe("AnalyzeData") {
      describe("with one data message") {
        describe("that is empty") {
          it("should work") {
            val doc     = createNewDocument()
            val element = doc.createElement(ElementNames.NUMBER)
            element.setAttribute("id", "MyElement")

            val analyzer = TestActorRef(NumericAnalyzer.props("MyElement", element))
            val data = ParserDataContainer(
              "",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )

            analyzer ! AnalyzeData(data)

            analyzer ! GetStatisticResult

            val response =
              new StatsResultNumeric("MyElement",
                                     BasicStatisticsResult(1,
                                                           Option(0),
                                                           None,
                                                           None,
                                                           None,
                                                           Option(StatisticErrors(1, 0, 0))))

            expectMsg(response)
          }
        }

        describe("that is incorrect") {
          it("should work") {
            val doc     = createNewDocument()
            val element = doc.createElement(ElementNames.NUMBER)
            element.setAttribute("id", "MyElement")

            val analyzer = TestActorRef(NumericAnalyzer.props("MyElement", element))
            val data = ParserDataContainer(
              "Some Data...",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )

            analyzer ! AnalyzeData(data)

            analyzer ! GetStatisticResult

            val response =
              new StatsResultNumeric("MyElement",
                                     BasicStatisticsResult(1,
                                                           Option(0),
                                                           None,
                                                           None,
                                                           None,
                                                           Option(StatisticErrors(1, 0, 0))))

            expectMsg(response)
          }
        }

        describe("that is correct") {
          it("should work") {
            val doc     = createNewDocument()
            val element = doc.createElement(ElementNames.NUMBER)
            element.setAttribute("id", "MyElement")

            val analyzer = TestActorRef(NumericAnalyzer.props("MyElement", element))
            val data = ParserDataContainer(
              "5",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )

            analyzer ! AnalyzeData(data)

            analyzer ! GetStatisticResult

            val response = new StatsResultNumeric("MyElement",
                                                  BasicStatisticsResult(1,
                                                                        Option(1),
                                                                        Option(5.0),
                                                                        Option(5.0),
                                                                        Option(5.0)))

            expectMsg(response)
          }
        }
      }

      describe("with multiple data messages") {
        describe("with three correct data elements") {
          it("should work") {
            val doc     = createNewDocument()
            val element = doc.createElement(ElementNames.NUMBER)
            element.setAttribute("id", "MyElement")

            val analyzer = TestActorRef(NumericAnalyzer.props("MyElement", element))
            val data = ParserDataContainer(
              "1",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data2 = ParserDataContainer(
              "5",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data3 = ParserDataContainer(
              "9",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )

            analyzer ! AnalyzeData(data)
            analyzer ! AnalyzeData(data2)
            analyzer ! AnalyzeData(data3)

            analyzer ! GetStatisticResult

            val response = new StatsResultNumeric("MyElement",
                                                  BasicStatisticsResult(3,
                                                                        Option(3),
                                                                        Option(1.0),
                                                                        Option(9.0),
                                                                        Option(5.0)))

            expectMsg(response)
          }
        }

        describe("with some simple correct and incorrect data elements") {
          it("should work") {
            val doc     = createNewDocument()
            val element = doc.createElement(ElementNames.NUMBER)
            element.setAttribute("id", "MyElement")

            val analyzer = TestActorRef(NumericAnalyzer.props("MyElement", element))
            val data = ParserDataContainer(
              "1",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data2 = ParserDataContainer(
              "-",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data3 = ParserDataContainer(
              "5",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data4 = ParserDataContainer(
              "",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data5 = ParserDataContainer(
              "9",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data6 = ParserDataContainer(
              "haus.2",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )

            analyzer ! AnalyzeData(data)
            analyzer ! AnalyzeData(data2)
            analyzer ! AnalyzeData(data3)
            analyzer ! AnalyzeData(data4)
            analyzer ! AnalyzeData(data5)
            analyzer ! AnalyzeData(data6)

            analyzer ! GetStatisticResult

            val response =
              new StatsResultNumeric("MyElement",
                                     BasicStatisticsResult(6,
                                                           Option(3),
                                                           Option(1.0),
                                                           Option(9.0),
                                                           Option(5.0),
                                                           Option(StatisticErrors(3, 0, 0))))

            expectMsg(response)
          }
        }

        describe("with some bigger correct and incorrect data elements") {
          it("should work") {
            val doc     = createNewDocument()
            val element = doc.createElement(ElementNames.NUMBER)
            element.setAttribute("id", "MyElement")

            val analyzer = TestActorRef(NumericAnalyzer.props("MyElement", element))
            val data = ParserDataContainer(
              "23",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data2 = ParserDataContainer(
              "-",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data3 = ParserDataContainer(
              "58",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data4 = ParserDataContainer(
              "",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data5 = ParserDataContainer(
              "9",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data6 = ParserDataContainer(
              "haus.2",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data7 = ParserDataContainer(
              "2",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )

            analyzer ! AnalyzeData(data)
            analyzer ! AnalyzeData(data2)
            analyzer ! AnalyzeData(data3)
            analyzer ! AnalyzeData(data4)
            analyzer ! AnalyzeData(data5)
            analyzer ! AnalyzeData(data6)
            analyzer ! AnalyzeData(data7)

            analyzer ! GetStatisticResult

            val response =
              new StatsResultNumeric("MyElement",
                                     BasicStatisticsResult(7,
                                                           Option(4),
                                                           Option(2),
                                                           Option(58.0),
                                                           Option(23.0),
                                                           Option(StatisticErrors(3, 0, 0))))

            expectMsg(response)
          }
        }

        describe("with some bigger complex correct and incorrect data elements") {
          it("should work") {
            val doc     = createNewDocument()
            val element = doc.createElement(ElementNames.NUMBER)
            element.setAttribute("id", "MyElement")
            element.setAttribute(AttributeNames.PRECISION, "2")

            val analyzer = TestActorRef(NumericAnalyzer.props("MyElement", element))
            val data = ParserDataContainer(
              "2320",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data2 = ParserDataContainer(
              "-",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data3 = ParserDataContainer(
              "5850",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data4 = ParserDataContainer(
              "",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data5 = ParserDataContainer(
              "988",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data6 = ParserDataContainer(
              "haus.2",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data7 = ParserDataContainer(
              "233",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )

            analyzer ! AnalyzeData(data)
            analyzer ! AnalyzeData(data2)
            analyzer ! AnalyzeData(data3)
            analyzer ! AnalyzeData(data4)
            analyzer ! AnalyzeData(data5)
            analyzer ! AnalyzeData(data6)
            analyzer ! AnalyzeData(data7)

            analyzer ! GetStatisticResult

            val response =
              new StatsResultNumeric("MyElement",
                                     BasicStatisticsResult(7,
                                                           Option(4),
                                                           Option(2.33),
                                                           Option(58.5),
                                                           Option(23.4775),
                                                           Option(StatisticErrors(3, 0, 0))))

            expectMsg(response)
          }
        }

        describe(
          "with some bigger complex correct and incorrect data elements and different separator"
        ) {
          it("should work") {
            val doc     = createNewDocument()
            val element = doc.createElement(ElementNames.NUMBER)
            element.setAttribute("id", "MyElement")
            element.setAttribute(AttributeNames.PRECISION, "4")
            element.setAttribute(AttributeNames.DECIMAL_SEPARATOR, ".")

            val analyzer = TestActorRef(NumericAnalyzer.props("MyElement", element))
            val data = ParserDataContainer(
              "2320000",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data2 = ParserDataContainer(
              "-",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data3 = ParserDataContainer(
              "58500000",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data4 = ParserDataContainer(
              "",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data5 = ParserDataContainer(
              "98800",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data6 = ParserDataContainer(
              "haus.2",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data7 = ParserDataContainer(
              "23300",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data8 = ParserDataContainer(
              "3",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data9 = ParserDataContainer(
              "-43",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )
            val data10 = ParserDataContainer(
              "-330000",
              "MyElement",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("MyElement", List.empty[(String, Long)]))
            )

            analyzer ! AnalyzeData(data)
            analyzer ! AnalyzeData(data2)
            analyzer ! AnalyzeData(data3)
            analyzer ! AnalyzeData(data4)
            analyzer ! AnalyzeData(data5)
            analyzer ! AnalyzeData(data6)
            analyzer ! AnalyzeData(data7)
            analyzer ! AnalyzeData(data8)
            analyzer ! AnalyzeData(data9)
            analyzer ! AnalyzeData(data10)

            analyzer ! GetStatisticResult

            val response =
              new StatsResultNumeric("MyElement",
                                     BasicStatisticsResult(10,
                                                           Option(7),
                                                           Option(-33.0),
                                                           Option(5850.0),
                                                           Option(865.8865714285713),
                                                           Option(StatisticErrors(3, 0, 0))))

            expectMsg(response)
          }
        }
      }
    }
  }
}
