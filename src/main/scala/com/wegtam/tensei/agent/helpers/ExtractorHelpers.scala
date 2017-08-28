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

import java.time.format.{ DateTimeFormatter, DateTimeParseException }
import java.time.{ LocalDate, LocalDateTime, LocalTime, ZonedDateTime }
import java.util.Locale

import scala.collection.mutable.ListBuffer
import scalaz.{ -\/, \/, \/- }

trait ExtractorHelpers {

  final val DEFAULT_ENCODING = "UTF-8"

  final val DECIMAL_SEPARATOR_POINT = "."

  final val DECIMAL_SEPARATOR_COMMA = ","

  final val DECIMAL_SEPARATOR_UNICODE = "⎖"

  /**
    * Clean the given element id from invalid characters.
    * If the cleaned id does not start with a character then a character is prepended.
    *
    * @param id The extracted id from the file.
    * @return A cleaned id.
    */
  def cleanElementId(id: String): String = {
    val cleaned = id
      .replaceAll("ß", "ss")
      .replaceAll("ä", "ae")
      .replaceAll("ü", "ue")
      .replaceAll("ö", "oe")
      .replaceAll("[\\W]", "")
    if (cleaned.substring(0, 1).matches("[a-zA-z]"))
      cleaned
    else {
      s"e-$cleaned"
    }
  }

  /**
    * Create the regular expression for a `formatnum`.
    *
    * @param length    The length of the entry.
    * @param precision The precision of the entry.
    * @return The regular expression as String.
    */
  def createFormatnumRegex(length: Long, precision: Long, separator: String = "."): String = {
    val separatorSign =
      if (separator.equals("."))
        "\\."
      else
        ","

    if (length > 0)
      // length and precision
      if (precision > 0)
        s"(-?\\d{0,${length - precision}}$separatorSign\\d{0,$precision})"
      // only length
      else
        s"(-?\\d{1,$length})"
    // only precision
    else if (precision > 0)
      s"(-?\\d*?$separatorSign\\d{0,$precision})"
    // no length and no precision
    else
      createGeneralFormatnumRegex
  }

  def createGeneralFormatnumRegex: String =
    s"(-?[\\d\\$DECIMAL_SEPARATOR_POINT$DECIMAL_SEPARATOR_COMMA$DECIMAL_SEPARATOR_UNICODE]+)"

  /**
    * Determine the length of the decimal part of the numbers
    *
    * @param entries A list of numbers
    * @param decimalSeparator The decimal separator
    * @return The length of the decimal part or None if it could not be determined
    */
  def determinePrecisionLength(entries: List[String], decimalSeparator: String): Option[Int] =
    if (entries.nonEmpty) {
      val length =
        entries
          .map(entry => {
            val index       = entry.lastIndexOf(decimalSeparator)
            val decimalPart = entry.substring(index + 1)
            if (decimalPart.matches("\\d*")) // check that the extracted part contains only numbers
              decimalPart.length
            else
              0
          })
          .max
      if (length > 0)
        Option(length)
      else
        None
    } else
      None

  /**
    * Determine the separator of the entries. The uncertainty is that we can not without
    * risk determine the decimal separator. A thousand separator could also be a decimal
    * separator. There are some specific cases where the method can extremely good determine
    * the separator but the uncertainty is still given.
    *
    * @param entries A list of entries that contains numbers.
    * @return The determined decimal separator or None if the method isn't certain
    */
  def determineSeparator(entries: List[String]): Option[String] = {
    // . - separator
    val dotSeparatorPattern1 =
      s"-?[\\d$DECIMAL_SEPARATOR_COMMA]+(\\$DECIMAL_SEPARATOR_POINT)[\\d{1,2}|\\d{4,]$$"
    val dotSeparatorPattern2 =
      s"-?[\\d$DECIMAL_SEPARATOR_COMMA]+(\\$DECIMAL_SEPARATOR_POINT)\\d+$$"
    // , - separator
    val commaSeparatorPattern1 =
      s"-?[\\d\\$DECIMAL_SEPARATOR_POINT]+($DECIMAL_SEPARATOR_COMMA)[\\d{1,2}|\\d{4,]$$"
    val commaSeparatorPattern2 =
      s"-?[\\d\\$DECIMAL_SEPARATOR_POINT]+($DECIMAL_SEPARATOR_COMMA)\\d+$$"
    // ⎖ - separator
    val unicodeSeparatorPattern1 =
      s"-?[\\d\\$DECIMAL_SEPARATOR_POINT$DECIMAL_SEPARATOR_COMMA]+($DECIMAL_SEPARATOR_UNICODE)[\\d{1,2}|\\d{4,]$$"
    val unicodeSeparatorPattern2 =
      s"-?[\\d\\$DECIMAL_SEPARATOR_POINT$DECIMAL_SEPARATOR_COMMA]+($DECIMAL_SEPARATOR_UNICODE)\\d+$$"
    if (entries.nonEmpty) {
      if (entries.map(entry => entry.matches(dotSeparatorPattern1)).forall(entry => entry))
        Option(DECIMAL_SEPARATOR_POINT)
      else if (entries.map(entry => entry.matches(commaSeparatorPattern1)).forall(entry => entry))
        Option(DECIMAL_SEPARATOR_COMMA)
      else if (entries.map(entry => entry.matches(dotSeparatorPattern2)).forall(entry => entry))
        Option(DECIMAL_SEPARATOR_POINT)
      else if (entries.map(entry => entry.matches(commaSeparatorPattern2)).forall(entry => entry))
        Option(DECIMAL_SEPARATOR_COMMA)
      else if (entries
                 .map(entry => entry.matches(unicodeSeparatorPattern1))
                 .forall(entry => entry))
        Option(DECIMAL_SEPARATOR_UNICODE)
      else if (entries
                 .map(entry => entry.matches(unicodeSeparatorPattern2))
                 .forall(entry => entry))
        Option(DECIMAL_SEPARATOR_UNICODE)
      else
        None
    } else
      None
  }

  /**
    * Is the given String a Long?
    *
    * @param value May be a long value.
    * @return `true` if the given value is 'long', otherwise `false`
    */
  def parseLong(value: String): Boolean =
    try {
      value.toLong
      true
    } catch {
      case _: Throwable =>
        false
    }

  /**
    * Is the given String a Double?
    *
    * @param value May be a double value.
    * @return `true` if the given value is 'double', otherwise `false`
    */
  def parseDouble(value: String): Boolean =
    \/.fromTryCatch(
      value.toDouble
    ) match {
      case -\/(failure) =>
        \/.fromTryCatch(
          value
            .replaceAll(s"[$DECIMAL_SEPARATOR_COMMA$DECIMAL_SEPARATOR_UNICODE]",
                        s"$DECIMAL_SEPARATOR_POINT")
            .toDouble
        ) match {
          case -\/(f) =>
            false
          case \/-(s) =>
            true
        }
      case \/-(success) =>
        true
    }

  /**
    * Is the given String a `sql.Date`?
    *
    * @param value My be a `sql.Date` value.
    * @return `true` if the given value is a `sql.Date`, otherwise `false`
    */
  def parseDate(value: String): Boolean =
    try {
      java.sql.Date.valueOf(value)
      true
    } catch {
      case _: Throwable =>
        false
    }

  /**
    * Is the given String a `sql.Time`?
    *
    * @param value My be a `sql.Time` value.
    * @return `true` if the given value is a `sql.Time`, otherwise `false`
    */
  def parseTime(value: String): Boolean =
    try {
      java.sql.Time.valueOf(value)
      true
    } catch {
      case _: Throwable =>
        false
    }

  /**
    * Is the given String a `sql.Timestamp`?
    *
    * @param value My be a `sql.Timestamp` value.
    * @return `true` if the given value is a `sql.Timestamp`, otherwise `false`
    */
  def parseTimestamp(value: String): Boolean =
    try {
      java.sql.Timestamp.valueOf(value.replaceAll("T", " "))
      true
    } catch {
      case _: Throwable =>
        false
    }

  /**
    * Parse a given String with a specific pattern to determine whether the String
    * can be a `formattime` element.
    *
    * @param source The possible `formattime` value.
    * @param format The pattern for the `format` attribute of the `formattime` element.
    * @param locale An optional locale, if not provided the system locale will be used.
    * @return `true` if the `source` is a `formattime` element, `false` otherwise
    */
  def parseFormattedTime(source: String, format: String, locale: Option[Locale] = None): Boolean = {
    val l = locale.getOrElse(Locale.getDefault)
    try {
      val dateTimeFormat = DateTimeFormatter.ofPattern(format, l)
      try {
        if (!Set("x", "X", "V", "Z").exists(format.contains(_))) {
          val dateTime = LocalDateTime.parse(source, dateTimeFormat)
          java.sql.Timestamp.valueOf(dateTime)
        } else {
          val dateTime = ZonedDateTime.parse(source, dateTimeFormat)
          val local    = dateTime.toLocalDateTime
          java.sql.Timestamp.valueOf(local)
        }
      } catch {
        case e: DateTimeParseException =>
          try {
            val date = LocalDate.parse(source, dateTimeFormat)
            java.sql.Date.valueOf(date)
          } catch {
            case e: DateTimeParseException =>
              val time = LocalTime.parse(source, dateTimeFormat)
              java.sql.Time.valueOf(time)
          }
      }
      true
    } catch {
      case _: Throwable =>
        false
    }
  }

  /**
    * Helper function that parses a list of values against a list of formats
    * and returns the first format that all entries of the list of values defines.
    * If no format defines the entries of the list of values, `None` is returned.
    *
    * @param entries  List of values to parse against the formats.
    * @param formats  Specific formats that could define the given values.
    * @return If one format defines the values, it is returned, otherwise `None`
    */
  def determineSpecificFormat(entries: ListBuffer[String], formats: List[String]): Option[String] = {
    val isSpecificFormat: List[(String, List[Boolean])] =
      formats.map(
        format =>
          format ->
          entries
            .map(
              entry => parseFormattedTime(entry, format)
            )
            .toList
      )

    if (isSpecificFormat.nonEmpty) {
      val foundFormat = isSpecificFormat.map(
        e => if (e._2.contains(false)) None else Option(e._1)
      )

      if (foundFormat.nonEmpty)
        foundFormat.find(_.isDefined).getOrElse(None)
      else
        None
    } else
      None
  }
}
