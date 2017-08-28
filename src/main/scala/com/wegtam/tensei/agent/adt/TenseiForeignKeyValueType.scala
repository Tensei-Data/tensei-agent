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
  * A sealed trait for allowed value types of foreign key fields.
  *
  */
sealed trait TenseiForeignKeyValueType

/**
  * A companion object to keep the namespace clean.
  *
  */
object TenseiForeignKeyValueType {

  /**
    * A foreign key value that holds a simple date. This type can be used if the source
    * field holds a date which will be "normalised away" in the target database.
    *
    * @param value An option to the actual value.
    */
  final case class FkDate(value: Option[java.sql.Date]) extends TenseiForeignKeyValueType

  /**
    * A foreign key value that holds a long e.g. a numeric value.
    * This is be the "standard" type because usually established foreign key
    * relations use numeric types.
    *
    * @param value An option to the actual value.
    */
  final case class FkLong(value: Option[Long]) extends TenseiForeignKeyValueType

  /**
    * A foreign key value that holds a string. This type is used if the source data
    * has not foreign key and is transformed into a target that has one. The appropriate
    * data will be normalised and the former string value will usually be replaced by
    * a numeric value pointing to the appropriate key column in the referenced table.
    * Another possibility is the usage of generated ids (UUID values) for key values.
    *
    * @param value An option to the actual value.
    */
  final case class FkString(value: Option[String]) extends TenseiForeignKeyValueType

}
