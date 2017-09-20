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

import java.time.{ LocalDate, LocalDateTime, LocalTime }
import java.util.Locale

import akka.actor.Props
import akka.util.ByteString
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}

import scala.language.existentials

object Overwrite {
  def props: Props = Props(classOf[Overwrite])
}

/**
  * This transformer returns the given `value` as the specified `type`. That can
  * be used to overwrite a value in a target element without considering the
  * value of the source element.
  *
  * The transformer accepts the following parameters:
  * - `value` : The value that should be returned. The default value is: ""
  * - `type` : The type of the value. The default type is: String
  *    Allowed `types` and their default values when `value` is empty:
  *    byte: "" as Array[Byte]
  *    string: ""
  *    long: 0
  *    bigdecimal: 0
  *    date: 1970-01-01
  *    time: 00:00:00
  *    datetime: 1970-01-01 00:00:00
  *    none: None
  */
class Overwrite extends BaseTransformer {
  val DEFAULT_DATETIME = "1970-01-01 00:00:00"

  val DEFAULT_DATE = "1970-01-01"

  val DEFAULT_TIME = "00:00:00"

  override def transform: Receive = {
    case msg: StartTransformation =>
      log.debug("Starting the overwrite process")

      val params = msg.options.params

      val response: Option[TransformerResponse] =
        if (msg.src.nonEmpty) {
          val replaceWith: Option[(Any, Class[_])] =
            paramValueO("type")(params).fold(None: Option[(Any, Class[_])]) { t =>
              val value = paramValue("value")(params)
              t.toLowerCase(Locale.ROOT) match {
                case "bigdecimal" =>
                  val v = value match {
                    case "" => new java.math.BigDecimal(0)
                    case _  => new java.math.BigDecimal(value)
                  }
                  Option((v, classOf[java.math.BigDecimal]))
                case "byte" =>
                  val v = value match {
                    case "" => Array.empty[Byte]
                    case _  => value.split(" ").map(hex => Integer.parseInt(hex, 16).toByte)
                  }
                  Option((v, classOf[Array[Byte]]))
                case "date" =>
                  val v = value.toLowerCase(Locale.ROOT) match {
                    case ""    => java.sql.Date.valueOf(DEFAULT_DATE)
                    case "now" => java.sql.Date.valueOf(LocalDate.now())
                    case _     => java.sql.Date.valueOf(value)
                  }
                  Option((v, classOf[java.sql.Date]))
                case "datetime" =>
                  val v = value.toLowerCase(Locale.ROOT) match {
                    case ""    => java.sql.Timestamp.valueOf(DEFAULT_DATETIME)
                    case "now" => java.sql.Timestamp.valueOf(LocalDateTime.now())
                    case _     => java.sql.Timestamp.valueOf(value)
                  }
                  Option((v, classOf[java.sql.Timestamp]))
                case "long" =>
                  val v = value match {
                    case "" => 0L
                    case _  => value.toLong
                  }
                  Option((v, classOf[java.lang.Long]))
                case "none" =>
                  Option((None, None.getClass))
                case "string" =>
                  Option((ByteString(value), classOf[String]))
                case "time" =>
                  val v = value.toLowerCase(Locale.ROOT) match {
                    case ""    => java.sql.Time.valueOf(DEFAULT_TIME)
                    case "now" => java.sql.Time.valueOf(LocalTime.now())
                    case _     => java.sql.Time.valueOf(value)
                  }
                  Option((v, classOf[java.sql.Time]))
                case unsupportedType =>
                  throw new IllegalArgumentException(
                    s"Illegal type for Overwrite transformer: $unsupportedType!"
                  )
              }
            }

          replaceWith.fold {
            throw new IllegalArgumentException(
              s"Not enough parameters for overwrite operation: ${msg.options.params} !"
            )
          } { r =>
            val (value, typeDesc) = r
            val newValues         = msg.src.map(v => value)
            log.debug("Finished overwrite process.")
            Option(TransformerResponse(newValues, typeDesc))
          }
        } else
          Option(TransformerResponse(List(ByteString("")), classOf[String]))

      response.foreach(r => sender() ! r)
      context.become(receive)
  }
}
