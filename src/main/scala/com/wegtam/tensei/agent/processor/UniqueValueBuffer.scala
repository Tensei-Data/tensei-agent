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

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.cluster.pubsub.DistributedPubSubMediator.Unsubscribe
import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.wegtam.tensei.adt.ElementReference
import com.wegtam.tensei.adt.GlobalMessages.{ ReportToCaller, ReportingTo }
import com.wegtam.tensei.agent.helpers.LoggingHelpers
import com.wegtam.tensei.agent.processor.UniqueValueBuffer.UniqueValueBufferMessages

/**
  * This actor buffers values that were written for elements that have the `unique` attribute set to `true`.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  */
class UniqueValueBuffer(agentRunIdentifier: Option[String]) extends Actor with ActorLogging {
  // Create a distributed pub sub mediator.
  import DistributedPubSubMediator.{ Subscribe, SubscribeAck }
  val mediator = DistributedPubSub(context.system).mediator

  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  val buffer: scala.collection.mutable.Map[ElementReference, ActorRef] =
    scala.collection.mutable.Map.empty[ElementReference, ActorRef]

  // Subscribe to our event channel.
  mediator ! Subscribe(UniqueValueBuffer.UNIQUE_VALUE_BUFFER_CHANNEL, self) // Subscribe to the pub sub channel.

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    mediator ! Unsubscribe(UniqueValueBuffer.UNIQUE_VALUE_BUFFER_CHANNEL, self) // Unsubscribe from the pub sub channel.
    super.postStop()
  }

  override def receive: Receive = {
    case SubscribeAck(msg) =>
      if (msg.topic == UniqueValueBuffer.UNIQUE_VALUE_BUFFER_CHANNEL)
        log.debug("Successfully subscribed to unique value buffer channel.")
      else
        log.warning("Got subscribe ack for unknown channel topic!")

    case UniqueValueBufferMessages.Store(ref, value) =>
      log.debug("Received request to store a value for reference {}.", ref)
      if (buffer.get(ref).isEmpty)
        buffer.put(ref, context.actorOf(UniqueValueBufferWorker.props(agentRunIdentifier, ref)))
      buffer(ref) forward UniqueValueBufferMessages.Store(ref, value)

    case UniqueValueBufferMessages.StoreS(ref, values) =>
      log.debug("Received request to store {} values for reference {}.", values.size, ref)
      if (buffer.get(ref).isEmpty)
        buffer.put(ref, context.actorOf(UniqueValueBufferWorker.props(agentRunIdentifier, ref)))
      buffer(ref) forward UniqueValueBufferMessages.StoreS(ref, values)

    case UniqueValueBufferMessages.CheckIfValueExists(ref, value) =>
      log.debug("Received check if value exists for reference {} and value {}.", ref, value)
      if (buffer.get(ref).isEmpty)
        sender() ! UniqueValueBufferMessages.ValueDoesNotExist(ref, value)
      else
        buffer(ref) forward UniqueValueBufferMessages.CheckIfValueExists(ref, value)

    case ReportToCaller =>
      log.debug("Received report to caller request from {}.", sender().path)
      sender() ! ReportingTo(ref = self)
  }

}

object UniqueValueBuffer {

  val UNIQUE_VALUE_BUFFER_CHANNEL = "UniqueValueBufferChannel" // The channel name for the pub sub mediator.

  val UNIQUE_VALUE_BUFFER_NAME = "UniqueValueBuffer" // The actor name that should be used for this actor.

  def props(agentRunIdentifier: Option[String]): Props =
    Props(classOf[UniqueValueBuffer], agentRunIdentifier)

  /**
    * A sealed trait for the messages for storing and checking of unique values.
    */
  sealed trait UniqueValueBufferMessages

  /**
    * A companion object for the sealed trait to keep the namespace clean.
    */
  object UniqueValueBufferMessages {

    /**
      * Check if the given value for the provided element reference was already stored.
      *
      * @param ref The element reference of the dfasdl element.
      * @param value The value that shall be checked.
      */
    final case class CheckIfValueExists(ref: ElementReference, value: Any)
        extends UniqueValueBufferMessages

    /**
      * Store the given value for the provided element reference.
      *
      * @param ref The element reference of the dfasdl element.
      * @param value The value that should be stored.
      */
    final case class Store(ref: ElementReference, value: Any) extends UniqueValueBufferMessages

    /**
      * Store the given values for the provided element reference.
      *
      * @param ref The element reference of the dfasdl element.
      * @param values A set of values that should be stored.
      */
    final case class StoreS(ref: ElementReference, values: Set[Any])
        extends UniqueValueBufferMessages

    /**
      * The "answer" to the [[CheckIfValueExists]] message that indicates that the value was already stored.
      *
      * @param ref The element reference of the dfasdl element.
      * @param value The value that is already stored.
      */
    final case class ValueExists(ref: ElementReference, value: Any)
        extends UniqueValueBufferMessages

    /**
      * The "answer" to the [[CheckIfValueExists]] message that indicates the the value was not stored.
      *
      * @param ref The element reference of the dfasdl element.
      * @param value The value that was not already stored.
      */
    final case class ValueDoesNotExist(ref: ElementReference, value: Any)
        extends UniqueValueBufferMessages

  }

}
