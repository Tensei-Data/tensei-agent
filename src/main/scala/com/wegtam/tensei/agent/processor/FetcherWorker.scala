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

import akka.actor._
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.wegtam.tensei.adt.{ AtomicTransformationDescription, TransformationDescription }
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.adt.ParserDataContainer
import com.wegtam.tensei.agent.helpers.LoggingHelpers
import com.wegtam.tensei.agent.processor.FetcherWorker.FetcherWorkerMessages
import com.wegtam.tensei.agent.processor.TransformationWorker.TransformationWorkerMessages
import org.dfasdl.utils.ElementHelpers
import org.w3c.dom.Element

object FetcherWorker {

  /**
    * A sealed trait for the messages of this actor.
    */
  sealed trait FetcherWorkerMessages

  /**
    * A companion object for the messages trait to keep the namespace clean.
    */
  object FetcherWorkerMessages {

    /**
      * Upon this message the worker will try to receive the described data
      * and apply all given atomic transformations to it.
      *
      * @param element         The element describing the data.
      * @param locator         A helper class holding the parameters that specify the "location" of the data.
      * @param target          An actor ref that defines the target actor that shall receive the result.
      * @param transformations The list of atomic transformations to apply.
      */
    case class Fetch(
        element: Element,
        locator: FetchDataLocator,
        target: ActorRef,
        transformations: List[AtomicTransformationDescription]
    ) extends FetcherWorkerMessages

    /**
      * A simple wrapper message to pass a possible sequence row counter to ourself.
      *
      * @param sequenceRowCounter An option to a sequence row counter.
      */
    case class SequenceRowCounter(sequenceRowCounter: Option[Long]) extends FetcherWorkerMessages

    /**
      * Make the actor stop itself.
      */
    case object Stop extends FetcherWorkerMessages

    /**
      * Instruct the actor to apply the list of stored atomic transformations to
      * the given data.
      *
      * @param container The container holding the parsed data.
      * @param element   The data element description.
      */
    case class Transform(container: ParserDataContainer, element: Element)
        extends FetcherWorkerMessages

  }

  /**
    * A factory method to create an actor.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @param source The actor ref of the data tree document holding the data.
    * @return The props to create an actor.
    */
  def props(agentRunIdentifier: Option[String], source: ActorRef): Props =
    Props(classOf[FetcherWorker], agentRunIdentifier, source)

}

/**
  * A fetcher worker does the actual work of querying the storage actors.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  * @param source The actor ref of the data tree document holding the data.
  */
class FetcherWorker(agentRunIdentifier: Option[String], source: ActorRef)
    extends Actor
    with ActorLogging
    with ElementHelpers {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  // We need to buffer the current message.
  var currentFetchMessage: Option[FetcherWorkerMessages.Fetch] = None
  // We may need to buffer a sequence hash value.
  var currentHash: Option[Long] = None

  override def receive: Receive = {
    case FetcherWorkerMessages.Stop =>
      log.debug("Received stop message.")
      context.stop(self)

    case FetcherWorkerMessages.Fetch(element, locator, target, transformations) =>
      // We only process the message if the buffer is free.
      if (currentFetchMessage.isEmpty) {
        // Buffer the message.
        currentFetchMessage = Option(
          FetcherWorkerMessages.Fetch(element, locator, target, transformations)
        )
        // First we need to retrieve the corrent sequence row.
        val sequenceRow          = locator.sequenceRow
        val mappingKeyFieldValue = locator.mappingKeyFieldValue
        val mappingKeyFieldId    = locator.mappingKeyFieldId
        if (mappingKeyFieldId.isDefined && mappingKeyFieldValue.isDefined)
          source ! DataTreeDocumentMessages.FindDataContainer(mappingKeyFieldId.get,
                                                              mappingKeyFieldValue.get)
        else
          self ! FetcherWorkerMessages.SequenceRowCounter(sequenceRow)
      } else
        log.warning("Ignoring fetch message because of already buffered one.")

    case DataTreeNodeMessages.FoundContent(container) =>
      // Pipe the result of the data tree node to ourself.
      val counter =
        if (container.sequenceRowCounter >= 0)
          Option(container.sequenceRowCounter)
        else
          None
      self ! FetcherWorkerMessages.SequenceRowCounter(counter)

    case FetcherWorkerMessages.SequenceRowCounter(sequenceRowCounter) =>
      if (currentFetchMessage.isDefined) {
        val parentSeq = getParentSequence(currentFetchMessage.get.element)
        if (parentSeq.isDefined && getParentSequence(parentSeq.get).isDefined) {
          // We are within a stacked sequence therefore we need to get the element by it's hash.
          source ! DataTreeDocumentMessages.ReturnSequenceHash(parentSeq.get.getAttribute("id"),
                                                               sequenceRowCounter.getOrElse(0L))
        } else {
          // Try to fetch the data.
          val elementId = currentFetchMessage.get.element.getAttribute("id")
          source ! DataTreeDocumentMessages.ReturnData(elementId, sequenceRowCounter)
        }
      } else
        log.error("No buffered fetch message!")

    case DataTreeDocumentMessages.SequenceHash(sequenceId, hash, row) =>
      if (currentFetchMessage.isDefined) {
        // Buffer the hash and request the return of the hashed data.
        val elementId = currentFetchMessage.get.element.getAttribute("id")
        currentHash = Option(hash)
        source ! DataTreeDocumentMessages.ReturnHashedData(elementId, hash, Option(row))
      } else
        log.error("No buffered fetch message!")

    case DataTreeNodeMessages.Content(data) =>
      if (currentFetchMessage.isDefined) {
        val element   = currentFetchMessage.get.element
        val elementId = element.getAttribute("id")
        if (data.isEmpty) {
          log.error("Data fetcher returned no data for element '{}'!", elementId)
          context stop self
        } else {
          if (currentHash.isDefined) {
            // We may need to find the element by its hash...
            if (data.size > 1) {
              val f = data.find(_.dataElementHash.getOrElse("") == currentHash.get)
              if (f.isDefined)
                self ! FetcherWorkerMessages.Transform(f.get, element) // Use the found element.
              else {
                log.error("No data found for hash {}!", currentHash.get)
                context stop self
              }
            } else
              self ! FetcherWorkerMessages.Transform(data.head, element) // Use the one element that was received.
          } else {
            if (data.size > 1)
              log.warning(
                "Data fetcher returned more than one entry for element '{}'! Using first one!",
                elementId
              )

            self ! FetcherWorkerMessages.Transform(data.head, element)
          }
        }
      } else
        log.error("No buffered fetch message!")

    case FetcherWorkerMessages.Transform(data, element) =>
      if (currentFetchMessage.isDefined) {
        // We start applying atomic transformations.
        val element   = currentFetchMessage.get.element
        val target    = currentFetchMessage.get.target
        val elementId = element.getAttribute("id")
        val elementTransformations = currentFetchMessage.get.transformations
          .filter(_.element.elementId == elementId)
          .map(
            t =>
              TransformationDescription(transformerClassName = t.transformerClassName,
                                        options = t.options)
          )
        if (elementTransformations.nonEmpty) {
          log.debug("Creating atomic worker and instructing it to apply transformations.")
          val worker = context.actorOf(TransformationWorker.props(agentRunIdentifier))
          worker ! TransformationWorkerMessages.Start(
            container = data,
            element = element,
            target = target,
            transformations = elementTransformations
          )
        } else {
          log.debug("No atomic transformations defined. Relaying data directly.")
          target ! data
        }
        // Reset stored data.
        currentHash = None
        currentFetchMessage = None
      } else
        log.error("No buffered fetch message!")

  }

}
