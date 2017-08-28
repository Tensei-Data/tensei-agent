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

package com.wegtam.tensei.agent.writers

import com.wegtam.tensei.agent.DefaultSpec
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages.WriteData

import scala.collection.immutable.SortedSet

class BaseWriterFunctionsTest extends DefaultSpec with BaseWriterFunctions {
  describe("BaseWriterFunctions") {
    describe("messagesMissing") {
      it("should return true if messages are missing") {
        val messages = SortedSet[WriteData](
          new WriteData(1, ""),
          new WriteData(2, ""),
          new WriteData(4, ""),
          new WriteData(5, ""),
          new WriteData(6, "")
        )

        messagesMissing(messages) should be(true)
      }

      it("should return false if messages are complete") {
        val messages = SortedSet[WriteData](
          new WriteData(1, ""),
          new WriteData(2, ""),
          new WriteData(3, ""),
          new WriteData(4, ""),
          new WriteData(5, ""),
          new WriteData(6, "")
        )

        messagesMissing(messages) should be(false)
      }
    }
  }
}
