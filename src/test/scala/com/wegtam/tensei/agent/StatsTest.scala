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

package com.wegtam.tensei.agent

import akka.testkit.TestActorRef
import com.wegtam.tensei.adt.StatsMessages.CalculateStatisticsResult
import com.wegtam.tensei.adt.StatsResult.{
  BasicStatisticsResult,
  StatisticErrors,
  StatsResultNumeric,
  StatsResultString
}
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.Stats.StatsMessages.FinishAnalysis
import com.wegtam.tensei.agent.adt.ParserDataContainer
import com.wegtam.tensei.agent.helpers.{ GenericHelpers, XmlHelpers }

import scalaz._
import Scalaz._

class StatsTest extends ActorSpec with XmlHelpers with GenericHelpers {
  describe("Stats") {
    describe("DataTreeDocumentMessages.SaveData") {
      describe("with one data message") {
        val sourceData =
          getClass.getResource("/com/wegtam/tensei/agent/stats/simple-data.csv").toURI
        val dfasdl = DFASDL(
          "SIMPLE-DFASDL",
          scala.io.Source
            .fromInputStream(
              getClass.getResourceAsStream("/com/wegtam/tensei/agent/stats/simple-dfasdl.xml")
            )
            .mkString
        )
        val sourceElements = List(
          ElementReference(dfasdl.id, "alter"),
          ElementReference(dfasdl.id, "name")
        )
        val targetElements = List(
          ElementReference(dfasdl.id, "alter"),
          ElementReference(dfasdl.id, "name")
        )
        val mapping  = MappingTransformation(sourceElements, targetElements)
        val recipe   = new Recipe("COPY-COLUMNS", Recipe.MapOneToOne, List(mapping))
        val cookbook = Cookbook("COOKBOOK", List(dfasdl), Option(dfasdl), List(recipe))
        val source =
          ConnectionInformation(sourceData, Option(DFASDLReference(cookbook.id, dfasdl.id)))

        describe("for string data") {
          describe("that is empty") {
            it("should work") {
              val stats = TestActorRef(Stats.props(source, cookbook, List("alter", "name")))
              val data = ParserDataContainer(
                "",
                "name",
                Option("ID"),
                -1L,
                Option(calculateDataElementStorageHash("name", List.empty[(String, Long)]))
              )

              stats ! DataTreeDocumentMessages.SaveData(data, data.dataElementHash.get)

              stats ! FinishAnalysis

              val sResult = new StatsResultString("name",
                                                  BasicStatisticsResult(1,
                                                                        Option(1),
                                                                        Option(0.0),
                                                                        Option(0.0),
                                                                        Option(0.0)))
              val cresult = new CalculateStatisticsResult(List(sResult).right[String],
                                                          source,
                                                          cookbook,
                                                          List("alter", "name"))
              val response = cresult

              expectMsg(response)
            }
          }

          describe("that is not empty") {
            it("should work") {
              val stats = TestActorRef(Stats.props(source, cookbook, List("alter", "name")))
              val data = ParserDataContainer(
                "Augustus",
                "name",
                Option("ID"),
                -1L,
                Option(calculateDataElementStorageHash("name", List.empty[(String, Long)]))
              )

              stats ! DataTreeDocumentMessages.SaveData(data, data.dataElementHash.get)

              stats ! FinishAnalysis

              val sResult = new StatsResultString("name",
                                                  BasicStatisticsResult(1,
                                                                        Option(1),
                                                                        Option(8.0),
                                                                        Option(8.0),
                                                                        Option(8.0)))
              val cresult = new CalculateStatisticsResult(List(sResult).right[String],
                                                          source,
                                                          cookbook,
                                                          List("alter", "name"))
              val response = cresult

              expectMsg(response)
            }
          }
        }
        describe("for numerical data") {
          describe("that is empty") {
            it("should work") {
              val stats = TestActorRef(Stats.props(source, cookbook, List("alter", "name")))
              val data = ParserDataContainer(
                "",
                "alter",
                Option("ID"),
                -1L,
                Option(calculateDataElementStorageHash("alter", List.empty[(String, Long)]))
              )

              stats ! DataTreeDocumentMessages.SaveData(data, data.dataElementHash.get)

              stats ! FinishAnalysis

              val sResult =
                new StatsResultNumeric("alter",
                                       BasicStatisticsResult(1,
                                                             Option(0),
                                                             None,
                                                             None,
                                                             None,
                                                             Option(StatisticErrors(1, 0, 0))))
              val cresult = new CalculateStatisticsResult(List(sResult).right[String],
                                                          source,
                                                          cookbook,
                                                          List("alter", "name"))
              val response = cresult

              expectMsg(response)
            }
          }
        }
        describe("that is incorrect") {
          it("should work") {
            val stats = TestActorRef(Stats.props(source, cookbook, List("alter", "name")))
            val data = ParserDataContainer(
              "haus",
              "alter",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("alter", List.empty[(String, Long)]))
            )

            stats ! DataTreeDocumentMessages.SaveData(data, data.dataElementHash.get)

            stats ! FinishAnalysis

            val sResult =
              new StatsResultNumeric("alter",
                                     BasicStatisticsResult(1,
                                                           Option(0),
                                                           None,
                                                           None,
                                                           None,
                                                           Option(StatisticErrors(1, 0, 0))))
            val cresult = new CalculateStatisticsResult(List(sResult).right[String],
                                                        source,
                                                        cookbook,
                                                        List("alter", "name"))
            val response = cresult

            expectMsg(response)
          }
        }
        describe("that is correct") {
          it("should work") {
            val stats = TestActorRef(Stats.props(source, cookbook, List("alter", "name")))
            val data = ParserDataContainer(
              "27",
              "alter",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("alter", List.empty[(String, Long)]))
            )

            stats ! DataTreeDocumentMessages.SaveData(data, data.dataElementHash.get)

            stats ! FinishAnalysis

            val sResult = new StatsResultNumeric("alter",
                                                 BasicStatisticsResult(1,
                                                                       Option(1),
                                                                       Option(27.0),
                                                                       Option(27.0),
                                                                       Option(27.0)))
            val cresult = new CalculateStatisticsResult(List(sResult).right[String],
                                                        source,
                                                        cookbook,
                                                        List("alter", "name"))
            val response = cresult

            expectMsg(response)
          }
        }
      }

      describe("with multiple data messages") {
        val sourceData =
          getClass.getResource("/com/wegtam/tensei/agent/stats/simple-data.csv").toURI
        val dfasdl = DFASDL(
          "SIMPLE-DFASDL",
          scala.io.Source
            .fromInputStream(
              getClass.getResourceAsStream("/com/wegtam/tensei/agent/stats/simple-dfasdl.xml")
            )
            .mkString
        )
        val sourceElements = List(
          ElementReference(dfasdl.id, "alter"),
          ElementReference(dfasdl.id, "name")
        )
        val targetElements = List(
          ElementReference(dfasdl.id, "alter"),
          ElementReference(dfasdl.id, "name")
        )
        val mapping  = MappingTransformation(sourceElements, targetElements)
        val recipe   = new Recipe("COPY-COLUMNS", Recipe.MapOneToOne, List(mapping))
        val cookbook = Cookbook("COOKBOOK", List(dfasdl), Option(dfasdl), List(recipe))
        val source =
          ConnectionInformation(sourceData, Option(DFASDLReference(cookbook.id, dfasdl.id)))

        describe("only string data") {
          it("should work") {
            val stats = TestActorRef(Stats.props(source, cookbook, List("alter", "name")))
            val data = ParserDataContainer(
              "Mark",
              "name",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("name", List.empty[(String, Long)]))
            )
            val data2 = ParserDataContainer(
              "Karin",
              "name",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("name", List.empty[(String, Long)]))
            )
            val data3 = ParserDataContainer(
              "Augustus",
              "name",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("name", List.empty[(String, Long)]))
            )

            stats ! DataTreeDocumentMessages.SaveData(data, data.dataElementHash.get)
            stats ! DataTreeDocumentMessages.SaveData(data2, data2.dataElementHash.get)
            stats ! DataTreeDocumentMessages.SaveData(data3, data3.dataElementHash.get)

            stats ! FinishAnalysis

            val sResult = new StatsResultString("name",
                                                BasicStatisticsResult(3,
                                                                      Option(3),
                                                                      Option(4.0),
                                                                      Option(8.0),
                                                                      Option(5.666666666666667)))
            val cresult = new CalculateStatisticsResult(List(sResult).right[String],
                                                        source,
                                                        cookbook,
                                                        List("alter", "name"))
            val response = cresult

            expectMsg(response)
          }
        }
        describe("only numeric data") {
          it("should work") {
            val stats = TestActorRef(Stats.props(source, cookbook, List("alter", "name")))
            val data = ParserDataContainer(
              "27",
              "alter",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("alter", List.empty[(String, Long)]))
            )
            val data2 = ParserDataContainer(
              "26",
              "alter",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("alter", List.empty[(String, Long)]))
            )
            val data3 = ParserDataContainer(
              "3",
              "alter",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("alter", List.empty[(String, Long)]))
            )

            stats ! DataTreeDocumentMessages.SaveData(data, data.dataElementHash.get)
            stats ! DataTreeDocumentMessages.SaveData(data2, data2.dataElementHash.get)
            stats ! DataTreeDocumentMessages.SaveData(data3, data3.dataElementHash.get)

            stats ! FinishAnalysis

            val sResult = new StatsResultNumeric("alter",
                                                 BasicStatisticsResult(3,
                                                                       Option(3),
                                                                       Option(3.0),
                                                                       Option(27.0),
                                                                       Option(18.666666666666668)))
            val cresult = new CalculateStatisticsResult(List(sResult).right[String],
                                                        source,
                                                        cookbook,
                                                        List("alter", "name"))
            val response = cresult

            expectMsg(response)
          }
        }
        describe("mixed data") {
          it("should work") {
            val stats = TestActorRef(Stats.props(source, cookbook, List("alter", "name")))
            val stringData = ParserDataContainer(
              "Mark",
              "name",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("name", List.empty[(String, Long)]))
            )
            val stringData2 = ParserDataContainer(
              "Karin",
              "name",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("name", List.empty[(String, Long)]))
            )
            val stringData3 = ParserDataContainer(
              "Augustus",
              "name",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("name", List.empty[(String, Long)]))
            )

            val numericData = ParserDataContainer(
              "27",
              "alter",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("alter", List.empty[(String, Long)]))
            )
            val numericData2 = ParserDataContainer(
              "26",
              "alter",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("alter", List.empty[(String, Long)]))
            )
            val numericData3 = ParserDataContainer(
              "3",
              "alter",
              Option("ID"),
              -1L,
              Option(calculateDataElementStorageHash("alter", List.empty[(String, Long)]))
            )

            stats ! DataTreeDocumentMessages.SaveData(stringData, stringData.dataElementHash.get)
            stats ! DataTreeDocumentMessages.SaveData(numericData, numericData.dataElementHash.get)
            stats ! DataTreeDocumentMessages.SaveData(stringData2, stringData2.dataElementHash.get)
            stats ! DataTreeDocumentMessages.SaveData(numericData2,
                                                      numericData2.dataElementHash.get)
            stats ! DataTreeDocumentMessages.SaveData(stringData3, stringData3.dataElementHash.get)
            stats ! DataTreeDocumentMessages.SaveData(numericData3,
                                                      numericData3.dataElementHash.get)

            stats ! FinishAnalysis

            val response = expectMsgType[CalculateStatisticsResult]
            response.source should be(source)
            response.cookbook should be(cookbook)
            response.sourceIds should be(List("alter", "name"))
            response.results match {
              case -\/(failure) => fail(failure)
              case \/-(success) =>
                withClue("The number of results should be correct!") {
                  success.size should be(2)
                }
                withClue("The result list should contain the correct results!") {
                  success should contain(
                    new StatsResultNumeric("alter",
                                           BasicStatisticsResult(3,
                                                                 Option(3),
                                                                 Option(3.0),
                                                                 Option(27.0),
                                                                 Option(18.666666666666668)))
                  )
                  success should contain(
                    new StatsResultString("name",
                                          BasicStatisticsResult(3,
                                                                Option(3),
                                                                Option(4.0),
                                                                Option(8.0),
                                                                Option(5.666666666666667)))
                  )
                }
            }
          }
        }
      }
    }
  }
}
