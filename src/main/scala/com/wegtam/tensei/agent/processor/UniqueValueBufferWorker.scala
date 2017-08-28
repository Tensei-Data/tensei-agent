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

import akka.actor.{ Actor, ActorLogging, Props }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.wegtam.tensei.adt.ElementReference
import com.wegtam.tensei.agent.helpers.LoggingHelpers
import com.wegtam.tensei.agent.processor.UniqueValueBuffer.UniqueValueBufferMessages

/**
  * Stores the written values for a unique element.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  * @param elementReference The reference for the element for which this actors stores values.
  */
class UniqueValueBufferWorker(agentRunIdentifier: Option[String],
                              elementReference: ElementReference)
    extends Actor
    with ActorLogging {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  val buffer: scala.collection.mutable.Set[Any] = scala.collection.mutable.Set.empty[Any]

  override def receive: Receive = {
    case UniqueValueBufferMessages.Store(ref, value) =>
      if (ref != elementReference)
        log.error(
          s"Received store unique element value message for wrong element ($ref instead of $elementReference)!"
        )
      else {
        if (buffer.contains(value))
          log.warning(s"Given unique element value for $ref already stored!")
        buffer += value
        log.debug("Stored unique value for element {}.", ref)
      }

    case UniqueValueBufferMessages.StoreS(ref, values) =>
      if (ref != elementReference)
        log.error(
          s"Received store unique element value message for wrong element ($ref instead of $elementReference)!"
        )
      else {
        if (values.exists(v => buffer.contains(v)))
          log.warning(s"Given unique element value for $ref already stored!")
        buffer ++= values
        log.debug("Stored {} unique values for element {}.", values.size, ref)
      }

    case UniqueValueBufferMessages.CheckIfValueExists(ref, value) =>
      if (ref != elementReference)
        sender() ! UniqueValueBufferMessages.ValueDoesNotExist(ref, value)
      else if (buffer.contains(value))
        sender() ! UniqueValueBufferMessages.ValueExists(ref, value)
      else
        sender() ! UniqueValueBufferMessages.ValueDoesNotExist(ref, value)

  }

}

object UniqueValueBufferWorker {

  def props(agentRunIdentifier: Option[String], ref: ElementReference): Props =
    Props(classOf[UniqueValueBufferWorker], agentRunIdentifier, ref)

}
