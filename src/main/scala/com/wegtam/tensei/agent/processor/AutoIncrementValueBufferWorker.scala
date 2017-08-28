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
import com.wegtam.tensei.agent.adt.TenseiForeignKeyValueType
import com.wegtam.tensei.agent.helpers.LoggingHelpers
import com.wegtam.tensei.agent.processor.AutoIncrementValueBuffer.AutoIncrementValueBufferMessages

/**
  * Stores the auto-increment values for an element.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  * @param elementReference The reference for the element for which this actors stores values.
  */
class AutoIncrementValueBufferWorker(agentRunIdentifier: Option[String],
                                     elementReference: ElementReference)
    extends Actor
    with ActorLogging {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  val buffer: scala.collection.mutable.Map[TenseiForeignKeyValueType, TenseiForeignKeyValueType] =
    scala.collection.mutable.Map.empty[TenseiForeignKeyValueType, TenseiForeignKeyValueType]

  override def receive: Receive = {
    case AutoIncrementValueBufferMessages.Return(ref, value) =>
      if (ref != elementReference)
        sender() ! AutoIncrementValueBufferMessages.ValueNotFound(ref, value)
      else
        buffer.get(value) match {
          case Some(v) =>
            v match {
              case newValue: TenseiForeignKeyValueType.FkDate =>
                sender() ! AutoIncrementValueBufferMessages.ChangedValue(ref,
                                                                         oldValue = value,
                                                                         newValue = newValue)
              case newValue: TenseiForeignKeyValueType.FkLong =>
                sender() ! AutoIncrementValueBufferMessages.ChangedValue(ref,
                                                                         oldValue = value,
                                                                         newValue = newValue)
              case newValue: TenseiForeignKeyValueType.FkString =>
                sender() ! AutoIncrementValueBufferMessages.ChangedValue(ref,
                                                                         oldValue = value,
                                                                         newValue = newValue)
            }
          case None => sender() ! AutoIncrementValueBufferMessages.ValueNotFound(ref, value)
        }

    case AutoIncrementValueBufferMessages.Store(ref, values) =>
      log.debug("Received {} auto increment values to store for {}.", values.size, ref)
      if (ref != elementReference)
        log.error("Received store foreign key value message for wrong element ({} instead of {})!",
                  ref,
                  elementReference)
      else
        values.foreach(v => buffer.put(v.oldValue, v.newValue))
  }

}

object AutoIncrementValueBufferWorker {

  def props(agentRunIdentifier: Option[String], ref: ElementReference): Props =
    Props(classOf[AutoIncrementValueBufferWorker], agentRunIdentifier, ref)

}
