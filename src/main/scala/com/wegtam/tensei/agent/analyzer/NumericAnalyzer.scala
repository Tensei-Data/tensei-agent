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
  StatsResultNumeric
}
import com.wegtam.tensei.agent.Stats.StatsMessages.GetStatisticResult
import com.wegtam.tensei.agent.adt.ParserDataContainer
import com.wegtam.tensei.agent.analyzer.GenericAnalyzer.NumericAnalyzerMessages.AnalyzeData
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import org.dfasdl.utils.AttributeNames
import org.w3c.dom.Element

import scalaz._, Scalaz._

object NumericAnalyzer {
  def props(elementId: String, element: Element, percent: Int = 100): Props =
    Props(classOf[NumericAnalyzer], elementId, element, percent)
}

/**
  * A numeric analyzer performs the statistics for a numeric field.
  */
class NumericAnalyzer(elementId: String, element: Element, percent: Int = 100)
    extends GenericAnalyzer(elementId, element, percent) {
  val summary                           = new SummaryStatistics()
  var formatExceptionCounter: Long      = 0
  var nullPointerExceptionCounter: Long = 0
  var unexpectedExceptionCounter: Long  = 0

  override def receive: Receive = {
    case AnalyzeData(data) =>
      log.debug("Received data for a numerical analysis")

      val rawString = cleanString(data)
      parseDouble(rawString, data) match {
        case -\/(error) =>
          log.debug(error)
        case \/-(success) =>
          summary.addValue(success)
      }
    case GetStatisticResult =>
      log.info("Received request to finish the numerical anylysis")
      val result = createStatisticsResult()
      sender() ! result
  }

  def createStatisticsResult(): StatsResultNumeric = {
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
    val basic = new BasicStatisticsResult(totalDataAnalyzed, quantity, min, max, mean, error)
    new StatsResultNumeric(elementId, basic)
  }

  def cleanString(data: ParserDataContainer): String = {
    val value =
      try {
        Option(processNumberData(data.data.toString, element))
      } catch {
        case e: Throwable =>
          None
      }
    val rawString =
      if (value.isDefined && !value.get.isEmpty) {
        val precision =
          if (element.hasAttribute(AttributeNames.PRECISION))
            element.getAttribute(AttributeNames.PRECISION).toLong
          else
            0

        if (precision > 0) {
          // without '-'
          val digits =
            if (value.get.startsWith("-"))
              value.get.substring(1)
            else
              value.get
          val numberWithSeparator =
            if (digits.length > 0) {
              // fill the number with additional 0 at the front or take the given number and add the separator
              if (digits.length <= precision) {
                val extraZeros =
                  for (i <- 0 to (precision - digits.length + 1).toInt) yield "0"
                val filledNumber = s"${extraZeros.mkString}$digits"
                s"${filledNumber.substring(0, (filledNumber.length - precision).toInt)}.${filledNumber
                  .substring((filledNumber.length - precision).toInt)}"
              } else {
                s"${digits.substring(0, (digits.length - precision).toInt)}.${digits.substring((digits.length - precision).toInt)}"
              }
            } else
              ""

          // add the deleted '-'
          if (value.get.startsWith("-"))
            s"-$numberWithSeparator"
          else
            numberWithSeparator
        } else
          value.get
      } else
        ""
    rawString
  }

  def parseDouble(value: String, data: ParserDataContainer): String \/ Double =
    try {
      value.toDouble.right
    } catch {
      case empty: NullPointerException =>
        nullPointerExceptionCounter += 1
        s"ParseDouble: Found empty data container for element '${data.elementId}'!".left
      case number: NumberFormatException =>
        formatExceptionCounter += 1
        s"ParseDouble: Format of data '${data.data}' cannot be parsed to a double value.".left
      case _: Throwable =>
        unexpectedExceptionCounter += 1
        s"ParseDouble: An unexpected error occurred during parsing '${data.data}' to a double value.".left
    }
}
