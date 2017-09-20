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

import akka.actor._
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.DataTreeNode.{
  DataTreeNodeContent,
  DataTreeNodeMessages,
  DataTreeNodeState
}
import com.wegtam.tensei.agent.adt.ParserDataContainer
import com.wegtam.tensei.agent.helpers.LoggingHelpers

object DataTreeNode {

  sealed trait DataTreeNodeMessages

  object DataTreeNodeMessages {

    /**
      * Append the given data to this data tree node.
      *
      * @param data The data to append.
      */
    final case class AppendData(data: ParserDataContainer) extends DataTreeNodeMessages

    /**
      * The response for a `ReturnContent` message.
      *
      * @param data The actual content of the data tree node that is returned.
      */
    final case class Content(data: Vector[ParserDataContainer]) extends DataTreeNodeMessages

    /**
      * Use the data from the stored element and create a `SaveData` message that will be send back to the caller.
      *
      * @param data              A data container for the actual data element.
      * @param referenceId       The id of the referenced data element that contains the actual data.
      * @param sourceSequenceRow An option to the sequence row in which the ref is located.
      */
    final case class CreateSaveDataForReference(data: ParserDataContainer,
                                                referenceId: String,
                                                sourceSequenceRow: Option[Long] = None)
        extends DataTreeNodeMessages

    /**
      * The response for a `HasContent` message.
      *
      * @param container The container holding the found data.
      */
    final case class FoundContent(container: ParserDataContainer) extends DataTreeNodeMessages

    /**
      * Return the content if it matches the given `elementId` and `data`.
      *
      * @param elementId The ID of the data element.
      * @param data      The actual data which must match the data hold by the container.
      * @param receiver  An optional receiver.
      */
    final case class HasContent(elementId: String, data: Any, receiver: Option[ActorRef])
        extends DataTreeNodeMessages

    /**
      * Return the content of the data node.
      * If the receiver is given it will receive the content else the sender will receive it.
      * If the sequence row is specified the specific row will be returned.
      * If the element id is specified the appropriate "column" will be returned.
      * If both the sequence row and an element id are given then the specific "cell" will be returned.
      *
      * @param receiver    An optional receiver.
      * @param sequenceRow An option to a desired sequence row.
      * @param elementId   An option to an element id.
      */
    final case class ReturnContent(receiver: Option[ActorRef],
                                   sequenceRow: Option[Long] = None,
                                   elementId: Option[String] = None)
        extends DataTreeNodeMessages

  }

  /**
    * Helper method for creating a data tree node actor.
    *
    * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
    * @return The props to generate an actor.
    */
  def props(agentRunIdentifier: Option[String]): Props =
    Props(new DataTreeNode(agentRunIdentifier))

  /**
    * A sealed trait holding the data tree node states.
    */
  sealed trait DataTreeNodeState

  object DataTreeNodeState {

    /**
      * The node does not hold any data, e.g. it was just created.
      */
    case object Empty extends DataTreeNodeState

    /**
      * The node holds data.
      */
    case object Full extends DataTreeNodeState

  }

  /**
    * The state data of the data tree node.
    *
    * @param content The actual parser data.
    */
  final case class DataTreeNodeContent(
      content: Vector[ParserDataContainer] = Vector.empty[ParserDataContainer]
  )
}

/**
  * A data tree node can store several parsed entities of data.
  * It is initialized with an empty state and moves to a full state after the first received append data message.
  * Within the full state it can append more data and respond to several messages like `HasContent` or `ReturnContent`.
  *
  * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
  */
class DataTreeNode(agentRunIdentifier: Option[String])
    extends Actor
    with FSM[DataTreeNodeState, DataTreeNodeContent]
    with ActorLogging {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  override def postStop(): Unit = {
    log.clearMDC()
    super.postStop()
  }

  startWith(DataTreeNodeState.Empty, new DataTreeNodeContent())

  when(DataTreeNodeState.Empty) {
    case Event(DataTreeNodeMessages.AppendData(newContent), data) =>
      log.debug("Saving content.")
      goto(DataTreeNodeState.Full) using data.copy(data.content :+ newContent)
  }

  when(DataTreeNodeState.Full) {
    case Event(DataTreeNodeMessages.AppendData(newContent), data) =>
      stay() using data.copy(data.content :+ newContent)

    case Event(DataTreeNodeMessages.HasContent(elementId, content, receiver), data) =>
      val foundContent = data.content.find(c => c.elementId == elementId && c.data == content)
      foundContent.foreach { fc =>
        log.debug("Matched HasContent request for '{}'.", elementId)
        receiver.getOrElse(sender()) ! DataTreeNodeMessages.FoundContent(fc)
      }
      stay() using data

    case Event(DataTreeNodeMessages.ReturnContent(receiver, sequenceRow, elementId), data) =>
      val content = findContent(elementId, sequenceRow, data.content)
      if (content.isEmpty) log.debug("CONTENT: {}", data.content) // DEBUG
      receiver.getOrElse(sender()) ! DataTreeNodeMessages.Content(
        content.sortBy(_.sequenceRowCounter)
      )
      stay() using data

    case Event(
        DataTreeNodeMessages.CreateSaveDataForReference(givenData, referenceId, sourceSequenceRow),
        data
        ) =>
      val sequenceRow =
        if (givenData.sequenceRowCounter >= 0)
          Option(givenData.sequenceRowCounter)
        else
          None
      val content = findContent(Option(referenceId), sequenceRow, data.content)
      if (content.lastOption.isEmpty)
        log.warning("No data found for {}({})!", referenceId, sequenceRow)

      val saveData =
        content.lastOption.fold(givenData)(d => d.copy(elementId = givenData.elementId))

      if (givenData.dataElementHash.isEmpty)
        log.error("No data element hash present in given data element!")

      givenData.dataElementHash.foreach { hash =>
        sender() ! DataTreeDocumentMessages.SaveData(
          saveData.copy(sequenceRowCounter = sourceSequenceRow.getOrElse(-1L),
                        dataElementHash = givenData.dataElementHash),
          hash
        )
      }
      stay() using data
  }

  initialize()

  /**
    * Try to find matching content.
    *
    * @param elementId   An optional element id.
    * @param sequenceRow An optional sequence row.
    * @param content     The list of stored data to search.
    * @return An vector of data which may be empty or hold one or multiple entries.
    */
  private def findContent(elementId: Option[String],
                          sequenceRow: Option[Long],
                          content: Vector[ParserDataContainer]): Vector[ParserDataContainer] =
    (elementId, sequenceRow) match {
      case (Some(e), Some(r)) =>
        val cs    = content.filter(_.elementId == e)
        val exact = cs.filter(_.sequenceRowCounter == r)
        if (exact.nonEmpty)
          exact // Return the matching "cell".
        else
          cs.take(1) // Return the first element of the matching "column".
      case (Some(e), None) =>
        content.filter(_.elementId == e) // Return the matching "column".
      case (None, Some(r)) =>
        content.filter(_.sequenceRowCounter == r) // Return the whole matching sequence row.
      case _ => content // Return all content elements.
    }
}
