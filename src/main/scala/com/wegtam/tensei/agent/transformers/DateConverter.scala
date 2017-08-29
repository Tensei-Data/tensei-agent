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

import java.time.format.DateTimeFormatter
import java.time._

import akka.actor.Props
import akka.util.ByteString
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}

import scala.util.Try

object DateConverter {
  def props: Props = Props(new DateConverter())

  def convertDate(df: DateTimeFormatter)(tz: ZoneId)(data: Any): Any =
    data match {
      case b: ByteString =>
        if (b.utf8String.matches("^\\d+$"))
          java.sql.Timestamp.valueOf(
            ZonedDateTime
              .ofInstant(Instant.ofEpochMilli(b.utf8String.toLong), tz)
              .toLocalDateTime
          )
        else
          ZonedDateTime
            .of(LocalDateTime.parse(b.utf8String, df), tz)
            .toInstant
            .toEpochMilli
      case d: LocalDate =>
        ZonedDateTime.of(d, LocalTime.MIN, tz)
      case d: java.sql.Date =>
        ZonedDateTime
          .of(d.toLocalDate.getYear,
              d.toLocalDate.getMonthValue,
              d.toLocalDate.getDayOfMonth,
              0,
              0,
              0,
              0,
              tz)
          .toInstant
          .toEpochMilli
      case n: Long =>
        java.sql.Timestamp
          .valueOf(ZonedDateTime.ofInstant(Instant.ofEpochMilli(n), tz).toLocalDateTime)
      case o: OffsetDateTime =>
        o.toInstant.toEpochMilli
      case s: String =>
        if (s.matches("^\\d+$"))
          java.sql.Timestamp.valueOf(
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(s.toLong), tz).toLocalDateTime
          )
        else
          ZonedDateTime
            .of(LocalDateTime.parse(s, df), tz)
            .toInstant
            .toEpochMilli
      case t: java.sql.Timestamp =>
        ZonedDateTime.ofInstant(t.toInstant, tz).toInstant.toEpochMilli
      case otherType => otherType // Unsupported types are returned as is.
    }
}

/**
  * Converts a numeric timestamp to a real timestamp value and vice versa.
  *
  * The transformer accepts the following paramaters.
  * - `timezone` - The timezone of your date or datetime. Default value is `UTC`.
  * - `format`   - The format of your date or datetime. Default is `yyyy-MM-dd HH:mm:ss`.
  *   To define a format use the symbols of [[java.time.format.DateTimeFormatter]].
  *   This parameter can only be used if you specify string as data type in your DFASDL.
  *
  * ATTENTION: A numeric timestamp value must be in Milliseconds. If not, you must
  * use the TimestampAdjuster Transformer before using the DateConverter.
  */
class DateConverter extends BaseTransformer {
  override def transform: Receive = {
    case msg: StartTransformation =>
      log.debug("Starting DateConverter.")

      val params = msg.options.params

      val timezone = Try(params.find(p => p._1 == "timezone") match {
        case Some((_, t)) => ZoneId.of(t)
        case None         => ZoneId.of("UTC")
      }) match {
        case scala.util.Failure(f) =>
          log.error(f, "Could not parse given timezone!")
          ZoneId.of("UTC")
        case scala.util.Success(z) => z
      }
      val format        = params.find(p => p._1 == "format").fold("yyyy-MM-dd HH:mm:ss")(_._2.trim)
      val dateFormatter = DateTimeFormatter.ofPattern(format)
      val result        = msg.src.map(DateConverter.convertDate(dateFormatter)(timezone))

      log.debug("DateConverter finished.")
      log.debug("DateConverter transformed {} into {}.", msg.src, result)
      context become receive
      sender() ! TransformerResponse(result, classOf[String])
  }
}
