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

import com.wegtam.tensei.agent.DefaultSpec
import net.openhft.hashing.LongHashFunction

import scala.collection.SortedSet
import scalaz._

class GenericHelpersTest extends DefaultSpec with GenericHelpers {
  describe("GenericHelpers") {
    describe("calculateDataElementStorageHash") {
      val hasher = LongHashFunction.xx()

      describe("if called without sequence data") {
        it("should return the element id") {
          val elementId = "some-element-id"
          calculateDataElementStorageHash(elementId, List.empty[(String, Long)]) should be(
            hasher.hashBytes(elementId.getBytes("UTF-8"))
          )
        }
      }

      describe("if called with sequence data") {
        it("should return a proper hash") {
          val elementId = "some-element-id"
          val sequenceData = List(("a-parent-seq", 100L),
                                  ("another-seq", 23L),
                                  ("just-another-seq", 34L),
                                  ("now-it-gets-weird", 42L))
          calculateDataElementStorageHash(elementId, sequenceData) should be(1032701306154710564L)
        }
      }

      describe("if called with some weird encoded sequence data") {
        it("should return a proper hash") {
          val elementId = "some-element-id"
          val sequenceData = List(("a-parent-seq", 100L),
                                  ("änother-seq", 23L),
                                  ("jüst-anöther-seq", 34L),
                                  ("now-it-getß-weird", 42L))
          calculateDataElementStorageHash(elementId, sequenceData) should be(-775296153933294076L)
        }
      }
    }

    describe("roundRobinFromSortedSet") {
      describe("if called with an empty set") {
        it("should throw an exception") {
          GenericHelpers.roundRobinFromSortedSet[String](SortedSet.empty[String], None) match {
            case -\/(f) => f shouldBe a[java.util.NoSuchElementException]
            case \/-(s) => fail("An exception should be returned if an empty set was given!")
          }
        }
      }

      describe("using a simple set of integers") {
        val set = SortedSet(1, 2, 3, 4, 5, 6, 7, 8, 9)

        describe("without last element") {
          it("should return the first element") {
            GenericHelpers.roundRobinFromSortedSet[Int](set, None) match {
              case -\/(f) => fail(f)
              case \/-(s) => s should be(1)
            }
          }
        }

        describe("with last element") {
          it("should return the next logical element") {
            for (last <- 1 to 9) {
              if (last < 9)
                withClue(s"f($set, $last) should return ${last + 1}!") {
                  GenericHelpers.roundRobinFromSortedSet[Int](set, Option(last)) match {
                    case -\/(f) => fail(f)
                    case \/-(s) => s should be(last + 1)
                  }
                } else
                withClue(s"f($set, $last) should return the first element!") {
                  GenericHelpers.roundRobinFromSortedSet[Int](set, Option(last)) match {
                    case -\/(f) => fail(f)
                    case \/-(s) => s should be(1)
                  }
                }
            }
          }
        }
      }
    }
  }
}
