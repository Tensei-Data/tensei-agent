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

import com.wegtam.tensei.adt.ElementReference
import com.wegtam.tensei.agent.DefaultSpec
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages

import scala.collection.SortedSet

class BaseWriterFilterWorkerFunctionsTest extends DefaultSpec with BaseWriterFilterWorkerFunctions {

  describe("BaseWriterFilterWorkerFunctions") {

    it("should removeRowNumbers") {
      val d = List(
        BaseWriterMessages.WriteData(
          number = 1L,
          data = "foo",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 2L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 3L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 7L,
          data = "bar",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 8L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 9L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 10L,
          data = "foobar",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 11L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 9L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 7L,
          data = "foo",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(number = 8L,
                                     data = "egal",
                                     metaData = Option(BaseWriter.WriterMessageMetaData(id = "B")))
      )
      val s = SortedSet.empty[BaseWriterMessages.WriteData] ++ d

      removeRowNumbers(s, List("A", "B", "C"), SortedSet.empty[Int]) should be(s)
      removeRowNumbers(s, List("A", "B", "C"), SortedSet.empty[Int] ++ List(0, 1, 2, 3)) should be(
        SortedSet.empty[BaseWriterMessages.WriteData]
      )

      val e = List(
        BaseWriterMessages.WriteData(
          number = 7L,
          data = "bar",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 8L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 9L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 7L,
          data = "foo",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(number = 8L,
                                     data = "egal",
                                     metaData = Option(BaseWriter.WriterMessageMetaData(id = "B")))
      )
      val expected = SortedSet.empty[BaseWriterMessages.WriteData] ++ e

      removeRowNumbers(s, List("A", "B", "C"), SortedSet.empty[Int] ++ List(0, 2)) should be(
        expected
      )
    }

    it("should removeDuplicateRows") {
      val d = List(
        BaseWriterMessages.WriteData(
          number = 1L,
          data = "foo",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 2L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 3L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 4L,
          data = "foo",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 5L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 6L,
          data = 2,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 7L,
          data = "bar",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 8L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 9L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 10L,
          data = "foo",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(number = 11L,
                                     data = "egal",
                                     metaData = Option(BaseWriter.WriterMessageMetaData(id = "B")))
      )
      val s = SortedSet.empty[BaseWriterMessages.WriteData] ++ d
      val e = List(
        BaseWriterMessages.WriteData(
          number = 1L,
          data = "foo",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 2L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 3L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 7L,
          data = "bar",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 8L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 9L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 10L,
          data = "foo",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(number = 11L,
                                     data = "egal",
                                     metaData = Option(BaseWriter.WriterMessageMetaData(id = "B")))
      )
      val expected = SortedSet.empty[BaseWriterMessages.WriteData] ++ e

      removeDuplicateRows(s, List("A", "B", "C"), List("A")) should be(expected)
    }

    it("should getSequenceRows") {
      val d = List(
        BaseWriterMessages.WriteData(
          number = 1L,
          data = "foo",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 2L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 3L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 4L,
          data = "foo",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 5L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 6L,
          data = 2,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 7L,
          data = "bar",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 8L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 9L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 10L,
          data = "foo",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(number = 11L,
                                     data = "egal",
                                     metaData = Option(BaseWriter.WriterMessageMetaData(id = "B")))
      )
      val s = SortedSet.empty[BaseWriterMessages.WriteData] ++ d
      val e = List(
        BaseWriterMessages.WriteData(
          number = 2L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 3L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 5L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 6L,
          data = 2,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 8L,
          data = "egal",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 9L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(number = 11L,
                                     data = "egal",
                                     metaData = Option(BaseWriter.WriterMessageMetaData(id = "B")))
      )
      val expected = SortedSet.empty[BaseWriterMessages.WriteData] ++ e

      getSequenceRows(s, List("B", "C")) should be(expected)
    }

    it("should findRowNumber") {
      val d = List(
        BaseWriterMessages.WriteData(
          number = 1L,
          data = "foo",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 2L,
          data = "egal1",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 3L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 7L,
          data = "bar",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(
          number = 8L,
          data = "egal2",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "B"))
        ),
        BaseWriterMessages.WriteData(
          number = 9L,
          data = 1,
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "C"))
        ),
        BaseWriterMessages.WriteData(
          number = 10L,
          data = "foo",
          metaData = Option(BaseWriter.WriterMessageMetaData(id = "A"))
        ),
        BaseWriterMessages.WriteData(number = 11L,
                                     data = "egal3",
                                     metaData = Option(BaseWriter.WriterMessageMetaData(id = "B")))
      )
      val s = SortedSet.empty[BaseWriterMessages.WriteData] ++ d

      findRowNumber(s,
                    List("A", "B", "C"),
                    ElementReference(dfasdlId = "TEST", elementId = "A"),
                    "foo") should be(Option(0))
      findRowNumber(s,
                    List("A", "B", "C"),
                    ElementReference(dfasdlId = "TEST", elementId = "A"),
                    "bar") should be(Option(1))
      findRowNumber(s,
                    List("A", "B", "C"),
                    ElementReference(dfasdlId = "TEST", elementId = "B"),
                    "egal3") should be(Option(2))
      findRowNumber(s, List("A", "B", "C"), ElementReference(dfasdlId = "TEST", elementId = "C"), 1) should be(
        Option(0)
      )
      findRowNumber(s,
                    List("A", "B", "C"),
                    ElementReference(dfasdlId = "TEST", elementId = "C"),
                    999) should be(None)
    }

  }
}
