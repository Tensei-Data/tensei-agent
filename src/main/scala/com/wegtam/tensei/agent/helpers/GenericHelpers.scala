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

import net.openhft.hashing.LongHashFunction

import scala.collection.SortedSet
import scalaz._
import Scalaz._

/**
  * Generic helper functions.
  */
trait GenericHelpers {

  /**
    * Calculates a hash for the given element id and it's parent sequence data.
    * If the element is a simple data element then the id of the element is hashed.
    * For sequence elements the hash function merges the parent sequence ids and row
    * counters into a string which will be hashed.
    *
    * <p>This function uses the xxHash (http://www.xxhash.com/) algorithm.</p>
    *
    * @param elementId The id of the element.
    * @param parentSequenceIdsAndCounters A list of tuples holding the parent sequence ids and their row counters.
    * @return A long value representing the hash.
    */
  final def calculateDataElementStorageHash(
      elementId: String,
      parentSequenceIdsAndCounters: List[(String, Long)]
  ): Long = {
    val hasher = LongHashFunction.xx()
    if (parentSequenceIdsAndCounters.isEmpty)
      hasher.hashBytes(elementId.getBytes("UTF-8"))
    else {
      val seqRowsString = parentSequenceIdsAndCounters.mkString("-")
      val bytes         = seqRowsString.getBytes("UTF-8")
      hasher.hashBytes(bytes)
    }
  }
}

/**
  * A companion object that holds helper functions that can be accessed without using the trait.
  */
object GenericHelpers {

  /**
    * This is a helper function to create a `ValidationNel[String, Type]` from a given exception.
    *
    * @param e  An exception.
    * @tparam T A type parameter that defines the success side of the validation.
    * @return A validation holding the exception message and the cause message if it exists.
    */
  def createValidationFromException[T](e: Throwable): ValidationNel[String, T] = {
    val failureMessage = e.getMessage
    if (e.getCause == null)
      failureMessage.failNel[T]
    else
      (failureMessage.failNel[List[T]] |@| e.getCause.getMessage.failNel[List[T]]) {
        case (a, b) => a.head
      }
  }

  /**
    * Return an element from the given set by using the round robin algorithm.
    *
    * @param set           A sorted set that holds all possible candidates.
    * @param lastUsedEntry An option to the last used entry from the set.
    * @tparam T A type parameter that specifies the data types within the sorted set.
    * @return Either the next logical element or an exception if an empty map was given.
    */
  def roundRobinFromSortedSet[T](set: SortedSet[T], lastUsedEntry: Option[T]): Throwable \/ T =
    try {
      if (lastUsedEntry.isEmpty)
        set.head.right // Return the first element of the original set.
      else {
        val candidates = set.dropWhile(_ != lastUsedEntry.get) // Drop all entries up to the last used one.
        if (candidates.isEmpty || candidates.size == 1)
          set.head.right // Return the first element of the original set because no candidates are left.
        else
          candidates.tail.head.right // Return the first element of the rest of the candidates.
      }
    } catch {
      case e: Throwable =>
        e.left
    }
}
