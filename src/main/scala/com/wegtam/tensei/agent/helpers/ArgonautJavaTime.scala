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

package com.wegtam.tensei.agent.helpers

import java.time.{ LocalDate, LocalDateTime, LocalTime, OffsetDateTime }

import argonaut._
import Argonaut._

/**
  * Contains codecs for argonaut to handle several classes from `java.time`.
  */
object ArgonautJavaTime {

  /**
    * A codec for decoding and encoding [[java.time.LocalDate]] instances.
    *
    * @return An argonaut json codec.
    */
  implicit def LocalDateCodec: CodecJson[LocalDate] = CodecJson(
    (t: LocalDate) => jString(t.toString),
    c =>
      for {
        t <- c.as[String]
      } yield LocalDate.parse(t)
  )

  /**
    * A codec for decoding and encoding [[java.time.OffsetDateTime]] instances.
    *
    * @return An argonaut json codec.
    */
  implicit def OffsetDateTimeCodec: CodecJson[OffsetDateTime] = CodecJson(
    (t: OffsetDateTime) => jString(t.toString),
    c =>
      for {
        t <- c.as[String]
      } yield OffsetDateTime.parse(t)
  )

  /**
    * A codec for decoding and encoding [[java.time.LocalDateTime]] instances.
    *
    * @return An argonaut json codec.
    */
  implicit def LocalDateTimeCodec: CodecJson[LocalDateTime] = CodecJson(
    (t: LocalDateTime) => jString(t.toString),
    c =>
      for {
        t <- c.as[String]
      } yield LocalDateTime.parse(t)
  )

  /**
    * A codec for decoding and encoding [[java.time.LocalTime]] instances.
    *
    * @return An argonaut json codec.
    */
  implicit def LocalTimeCodec: CodecJson[LocalTime] = CodecJson(
    (t: LocalTime) => jString(t.toString),
    c =>
      for {
        t <- c.as[String]
      } yield LocalTime.parse(t)
  )

}
