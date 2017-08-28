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

package com.wegtam.tensei.agent.processor

/**
  * A helper class that holds the possible parameters for locating data elements within the data tree document.
  *
  * @param mappingKeyFieldId     An option to the ID of an element that is used as a "mapping key".
  * @param mappingKeyFieldValue  An option to the last fetched value of the "mapping key" field.
  * @param sequenceRow           An option to a sequence row number. This will be the usual case.
  */
case class FetchDataLocator(
    mappingKeyFieldId: Option[String],
    mappingKeyFieldValue: Option[Any],
    sequenceRow: Option[Long]
)
