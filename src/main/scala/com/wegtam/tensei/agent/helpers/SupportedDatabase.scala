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

/**
  * A sealed trait for all supported database types.
  */
sealed trait SupportedDatabase

/**
  * Apache Derby database.
  */
case object Derby extends SupportedDatabase

/**
  * Firebird database.
  */
case object Firebird extends SupportedDatabase

/**
  * The H2 database.
  */
case object H2 extends SupportedDatabase

/**
  * The HyperSQL (or HSQLDB) database.
  */
case object HyperSql extends SupportedDatabase

/**
  * The mariadb database.
  */
case object MariaDb extends SupportedDatabase

/**
  * The MySQL database.
  */
case object MySql extends SupportedDatabase

/**
  * The Oracle database.
  */
case object Oracle extends SupportedDatabase

/**
  * The PostreSQL database.
  */
case object PostgreSql extends SupportedDatabase

/**
  * The SQLite database.
  */
case object SQLite extends SupportedDatabase

/**
  * The microsoft sql server database.
  */
case object SqlServer extends SupportedDatabase
