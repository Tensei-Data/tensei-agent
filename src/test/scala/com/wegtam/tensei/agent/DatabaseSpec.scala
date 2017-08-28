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

package com.wegtam.tensei.agent

import org.scalatest.{ FunSpec, Matchers }

/**
  * @todo Move to integration tests!
  */
abstract class DatabaseSpec extends FunSpec with Matchers {

  /**
    * Create an in memory test database and return it's connection.
    *
    * @param name The name of the database.
    * @return A connection to the database.
    */
  def createTestDatabase(name: String = "test"): java.sql.Connection =
    java.sql.DriverManager.getConnection(s"jdbc:h2:mem:$name")
}
