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

import java.sql.{ Date, Time, Timestamp }

import akka.actor.Props
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}

object DateTypeConverter {
  def props: Props = Props(classOf[DateTypeConverter])
}

/**
  * Converts a given `Date`, `Time` or `DateTime` type into the defined
  * target type.
  *
  * The transformer accepts the following paramaters.
  * - `target` :  The defined target type for the incoming data. Accepted
  *               values are `date`, `time` and `datetime`. Default is `date`.
  */
class DateTypeConverter extends BaseTransformer with DateTypeConverterFunctions {

  val DEFAULT_TARGET_TYPE = "date"

  override def transform: Receive = {
    case msg: StartTransformation =>
      log.debug("Start DateTypeConverter")
      val params = msg.options.params
      val targetType =
        if (params.exists(p => p._1 == "target")) {
          val t = params.find(p => p._1 == "target").get._2.trim
          if (t.nonEmpty)
            t
          else
            DEFAULT_TARGET_TYPE
        } else
          DEFAULT_TARGET_TYPE

      val result =
        msg.src.map {
          case dateData: Date =>
            transformDate(targetType, dateData)
          case timeDate: Time =>
            transformTime(targetType, timeDate)
          case dateTimeDate: Timestamp =>
            transformDateTime(targetType, dateTimeDate)
          case anyType =>
            anyType
        }

      log.debug("DateTypeConverter transformed '{}' into '{}'", msg.src, result)
      context become receive
      sender() ! new TransformerResponse(result, classOf[String])
  }
}

trait DateTypeConverterFunctions {

  def transformDate(targetType: String, data: Date): Any =
    targetType match {
      case "date" =>
        data
      case "time" =>
        new Time(data.getTime)
      case "datetime" =>
        new Timestamp(data.getTime)
      case _ =>
        data
    }

  def transformTime(targetType: String, data: Time): Any =
    targetType match {
      case "date" =>
        new Date(0)
      case "time" =>
        data
      case "datetime" =>
        new Timestamp(data.getTime)
      case _ =>
        data
    }

  def transformDateTime(targetType: String, data: Timestamp): Any =
    targetType match {
      case "date" =>
        new Date(data.getTime)
      case "time" =>
        new Time(data.getTime)
      case "datetime" =>
        data
      case _ =>
        data
    }
}
