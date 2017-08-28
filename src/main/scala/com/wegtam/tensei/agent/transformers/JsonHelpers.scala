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

import argonaut._
import Argonaut._
import akka.util.ByteString

/**
  * Provides several helper functions for json.
  */
trait JsonHelpers {
  // Implicit json converter for java.math.BigDecimal.
  // FIXME With one of the next releases argonaut will get support for big decimals!
  implicit def BigDecimalCodecJson: CodecJson[java.math.BigDecimal] =
    CodecJson(
      (d: java.math.BigDecimal) => jString(d.toString),
      cursor =>
        for {
          value <- cursor.as[String]
        } yield new java.math.BigDecimal(value)
    )

  /**
    * Create a json object from the given object if supported.
    *
    * @param data A data object.
    * @param label An optional label to set.
    * @return The json object.
    * @throws RuntimeException If the data type is not supported.
    */
  def createJson(data: Any, label: String): Json =
    data match {
      case d: ByteString =>
        if (label.isEmpty)
          d.utf8String.asJson
        else
          Json(label := d.utf8String)
      case d: String =>
        if (label.isEmpty)
          d.asJson
        else
          Json(label := d)
      case d: java.lang.Integer =>
        if (label.isEmpty)
          d.asJson
        else
          Json(label := d)
      case d: java.lang.Long =>
        if (label.isEmpty)
          d.asJson
        else
          Json(label := d)
      case d: java.lang.Character =>
        if (label.isEmpty)
          d.asJson
        else
          Json(label := d)
      case d: java.lang.Float =>
        if (label.isEmpty)
          d.asJson
        else
          Json(label := d)
      case d: java.lang.Double =>
        if (label.isEmpty)
          d.asJson
        else
          Json(label := d)
      case d: java.lang.Boolean =>
        if (label.isEmpty)
          d.asJson
        else
          Json(label := d)
      case d: List[Any] =>
        val array = createJsonArray(d)
        if (label.isEmpty)
          array
        else
          Json(label := array)
      case d: java.math.BigDecimal =>
        if (label.isEmpty)
          d.asJson
        else
          Json(label := d)
      case None =>
        if (label.isEmpty)
          jNull
        else
          Json(label := jNull)
      case _ =>
        throw new RuntimeException("Unsupported data type for json conversion!")
    }

  /**
    * Take a list of values and return a json array of them.
    *
    * @param data A list of values.
    * @return A json array containing the values.
    */
  def createJsonArray(data: List[Any]): Json = {
    val values: List[Json] = data.map(d => createJson(d, ""))
    jArray(values)
  }

  /**
    * Take a list of values and labels and create a json object.
    *
    * @param data A list of values.
    * @param labels A list of labels for the values.
    * @return A json object containing the values.
    */
  def createJsonObject(data: List[Any], labels: List[String]): Json = {
    val values: List[Json] = data.map(d => createJson(d, ""))
    val pairs              = labels zip values
    jObjectAssocList(pairs)
  }
}
