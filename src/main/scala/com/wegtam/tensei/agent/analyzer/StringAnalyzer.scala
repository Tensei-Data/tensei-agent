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

import akka.actor.Props
import com.wegtam.tensei.adt.StatsResult.{
  BasicStatisticsResult,
  StatisticErrors,
  StatsResultString
}
import com.wegtam.tensei.agent.Stats.StatsMessages.GetStatisticResult
import com.wegtam.tensei.agent.adt.ParserDataContainer
import com.wegtam.tensei.agent.analyzer.GenericAnalyzer.NumericAnalyzerMessages.AnalyzeData
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import org.w3c.dom.Element

object StringAnalyzer {
  def props(elementId: String, element: Element, percent: Int = 100): Props =
    Props(new StringAnalyzer(elementId, element, percent))
}

/**
  * A string analyzer performs the statistics for a string field.
  */
class StringAnalyzer(elementId: String, element: Element, percent: Int = 100)
    extends GenericAnalyzer(elementId, element, percent) {
  val summary                           = new SummaryStatistics()
  var formatExceptionCounter: Long      = 0
  var nullPointerExceptionCounter: Long = 0
  var unexpectedExceptionCounter: Long  = 0

  override def receive: Receive = {
    case AnalyzeData(data) =>
      log.debug("Received data for a string analysis")

      val rawString = cleanString(data)

      if (rawString.isDefined) {
        val length = rawString.get.length
        summary.addValue(length.toDouble)
      } else {
        unexpectedExceptionCounter += 1
      }
    case GetStatisticResult =>
      log.info("Received request to finish the string anylysis")
      val result = createStatisticsResult()
      sender() ! result
  }

  def createStatisticsResult(): StatsResultString = {
    val quantity =
      if (summary.getN.isValidLong)
        Option(summary.getN)
      else
        None
    val min =
      if (!summary.getMin.isNaN)
        Option(summary.getMin)
      else
        None
    val max =
      if (!summary.getMax.isNaN)
        Option(summary.getMax)
      else
        None
    val mean =
      if (!summary.getMean.isNaN)
        Option(summary.getMean)
      else
        None
    val totalDataAnalyzed =
      if (quantity.isDefined)
        quantity.get + formatExceptionCounter + nullPointerExceptionCounter + unexpectedExceptionCounter
      else
        formatExceptionCounter + nullPointerExceptionCounter + unexpectedExceptionCounter
    val error: Option[StatisticErrors] =
      if (formatExceptionCounter > 0 | nullPointerExceptionCounter > 0 | unexpectedExceptionCounter > 0)
        Option(
          StatisticErrors(formatExceptionCounter,
                          nullPointerExceptionCounter,
                          unexpectedExceptionCounter)
        )
      else
        None
    val basic = BasicStatisticsResult(totalDataAnalyzed, quantity, min, max, mean, error)
    StatsResultString(elementId, basic)
  }

  def cleanString(data: ParserDataContainer): Option[String] = {
    val value =
      try {
        Option(processStringData(data.data.toString, element))
      } catch {
        case _: Throwable =>
          None
      }
    value
  }
}
