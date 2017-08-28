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

package com.wegtam.tensei.agent.adt

/**
  * A container class for parsed data.
  *
  * @param data                The actual data.
  * @param elementId           The ID of the DFASDL element that describes the data.
  * @param dfasdlId            An option to the ID of the DFASDL which defaults to `None`.
  * @param sequenceRowCounter  If the element is the child of a sequence the sequence row counter is stored here.
  * @param dataElementHash     An option to a possibly calculated hash that is used to pinpoint locations of stacked sequence and choice elements.
  */
final case class ParserDataContainer(
    data: Any,
    elementId: String,
    dfasdlId: Option[String] = None,
    sequenceRowCounter: Long = -1L,
    dataElementHash: Option[Long] = None
)
