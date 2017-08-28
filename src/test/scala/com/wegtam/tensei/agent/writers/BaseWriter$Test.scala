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

class BaseWriter$Test extends DefaultSpec {
  describe("BaseWriter") {
    describe("WriterMessage") {
      describe("compare") {
        it("should order properly") {
          val a = WriteData(0, "")
          val b = WriteData(0, "")

          withClue("Same numbers should be equal.")(a compare b) should be(0)

          val expectedList = List(
            WriteData(0, ""),
            WriteData(1, ""),
            WriteData(2, ""),
            WriteData(3, ""),
            WriteData(4, ""),
            WriteData(5, "")
          )

          val unsortedList = List(
            WriteData(2, ""),
            WriteData(0, ""),
            WriteData(1, ""),
            WriteData(5, ""),
            WriteData(3, ""),
            WriteData(4, "")
          )

          withClue("A list of messages should be sorted according to it's numeration.") {
            unsortedList.sorted shouldEqual expectedList
          }
        }
      }
    }
  }
}
