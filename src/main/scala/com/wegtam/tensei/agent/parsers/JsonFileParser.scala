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

package com.wegtam.tensei.agent.parsers

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import argonaut.Argonaut._
import argonaut._
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.adt._
import com.wegtam.tensei.agent.helpers.LoggingHelpers
import org.dfasdl.utils._
import org.w3c.dom.Element

import scala.annotation.tailrec
import scalaz._

object JsonFileParser {

  /**
    * Helper method to create a json file parser actor.
    *
    * @param source              The source connection to retrieve the data from.
    * @param cookbook            The cookbook holding the source dfasdl.
    * @param dataTreeRef         The actor ref to the data tree e.g. where to put the parsed data.
    * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
    * @return The props to create the actor.
    */
  def props(source: ConnectionInformation,
            cookbook: Cookbook,
            dataTreeRef: ActorRef,
            agentRunIdentifier: Option[String]): Props =
    Props(classOf[JsonFileParser], source, cookbook, dataTreeRef, agentRunIdentifier)

}

/**
  * A parser for JSON files that traverses the given json via argonaut.
  *
  * @param source              The source connection to retrieve the data from.
  * @param cookbook            The cookbook holding the source dfasdl.
  * @param dataTreeRef         The actor ref to the data tree e.g. where to put the parsed data.
  * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
  */
class JsonFileParser(source: ConnectionInformation,
                     cookbook: Cookbook,
                     dataTreeRef: ActorRef,
                     agentRunIdentifier: Option[String])
    extends Actor
    with ActorLogging
    with BaseParser {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.clearMDC()
    super.postStop()
  }

  // This is a global variable to be able to buffer the argonaut cursor state.
  var cursor: Option[ACursor] = None
  // A global mutable map that buffers the sequence counters.
  val sequenceCounters: scala.collection.mutable.Map[String, Int] =
    scala.collection.mutable.Map.empty[String, Int]

  def receive: Receive = {
    case BaseParserMessages.SubParserInitialize =>
      val path = source.uri
      log.debug("Preparing parsing of json file at {}.", path)
      \/.fromTryCatch(scala.io.Source.fromURI(path).mkString) match {
        case -\/(failure) =>
          log.error(failure, "An error occurred while trying to read the json file at {}!", path)
          val cause =
            if (failure.getCause != null)
              Option(StatusMessage(None, failure.getCause.getMessage, StatusType.FatalError, None))
            else
              None
          sender() ! GlobalMessages.ErrorOccured(
            StatusMessage(None, failure.getMessage, StatusType.FatalError, cause)
          )
        case \/-(jsonString) =>
          Parse.parse(jsonString) match {
            case -\/(parseError) =>
              log.error(parseError)
              sender() ! GlobalMessages.ErrorOccured(
                StatusMessage(None, parseError, StatusType.FatalError, None)
              )
            case \/-(json) =>
              cursor = Option(json.acursor)
              sender() ! BaseParserMessages.SubParserInitialized
          }
      }

    case BaseParserMessages.Start =>
      log.debug("Starting json parser.")
      if (cursor.isDefined) {
        parseJson()
        sender() ! ParserStatusMessage(ParserStatus.COMPLETED, Option(self))
      } else {
        log.error("Cursor not defined!")
        sender() ! ParserStatusMessage(ParserStatus.ABORTED, Option(self))
      }
    case BaseParserMessages.Stop =>
      log.debug("Stopping json parser.")
      context stop self
    case BaseParserMessages.Status =>
      log.error("Status request not yet implemented!")
  }

  private def parseJson(): Unit =
    if (source.dfasdlRef.isDefined && cookbook.findDFASDL(source.dfasdlRef.get).isDefined) {
      val xml = createNormalizedDocument(cookbook.findDFASDL(source.dfasdlRef.get).get.content)
      traverseTree(xml, log)
    } else
      log.error("No DFASDL defined for {} in cookbook {}", source.uri, cookbook.id)

  /**
    * Determines if the given column in the parent sequence of the given element
    * exists.
    * This function also returns `false` if there is no parent sequence!
    *
    * @param c      An [[ACursor]] that represents the current position in the json file.
    * @param n      The node representing the element that matches the cursors position.
    * @param column A counter that indicates the column that has to exist.
    * @return Either `true` or `false`.
    */
  @tailrec
  private def jsonArrayHasMoreElements(c: ACursor, n: org.w3c.dom.Node)(column: Int): Boolean = {
    val p = n.getParentNode
    if (p == null)
      false
    else {
      val t = getStructureElementType(p.getNodeName)
      if (StructureElementType.isSequence(t))
        (c =\ column).succeeded
      else
        jsonArrayHasMoreElements(c.up, p)(column)
    }
  }

  /**
    * Return the next sibling of the given node that is a valid
    * DFASDL element.
    *
    * @param n A node.
    * @return An option to a sibling node if it exists.
    */
  @tailrec
  private def getSibling(n: org.w3c.dom.Node): Option[org.w3c.dom.Node] = {
    val s = n.getNextSibling
    if (s == null)
      None
    else {
      if (getElementType(s.getNodeName) != ElementType.UnknownElement)
        Option(s)
      else
        getSibling(s)
    }
  }

  override def readDataElement(structureElement: Element,
                               useOffset: Long,
                               isInChoice: Boolean): BaseParserResponse =
    cursor
      .map { c =>
        getDataElementType(structureElement.getNodeName) match {
          case DataElementType.BinaryDataElement =>
            log.error("Binary data types not supported!")
            BaseParserResponse(
              data = None,
              elementType = DataElementType.BinaryDataElement,
              status = BaseParserResponseStatus.ERROR
            )
          case DataElementType.StringDataElement =>
            val decodeResult =
              if (structureElement.hasAttribute(AttributeNames.JSON_ATTRIBUTE_NAME))
                structureElement.getNodeName match {
                  case ElementNames.NUMBER =>
                    (c --\ structureElement.getAttribute(AttributeNames.JSON_ATTRIBUTE_NAME))
                      .as[Long]
                  case ElementNames.FORMATTED_NUMBER =>
                    if ((c --\ structureElement.getAttribute(AttributeNames.JSON_ATTRIBUTE_NAME)).focus
                          .exists(s => s.isNumber))
                      (c --\ structureElement.getAttribute(AttributeNames.JSON_ATTRIBUTE_NAME))
                        .as[Double] // FIXME This should be done via BigDecimal!
                    else
                      (c --\ structureElement.getAttribute(AttributeNames.JSON_ATTRIBUTE_NAME))
                        .as[String]
                  case _ =>
                    (c --\ structureElement.getAttribute(AttributeNames.JSON_ATTRIBUTE_NAME))
                      .as[String]
                } else {
                structureElement.getNodeName match {
                  case ElementNames.NUMBER => c.as[Long]
                  case ElementNames.FORMATTED_NUMBER =>
                    if (c.focus.exists(s => s.isNumber))
                      c.as[Double] // FIXME This should be done via BigDecimal!
                    else
                      c.as[String]
                  case _ => c.as[String]
                }
              }
            decodeResult.result match {
              case -\/(readError) =>
                log.error("An error occurred while trying to parse the element {}: {} ({})",
                          structureElement.getAttribute("id"),
                          readError._1,
                          readError._2)
                log.error("Current json focus: {}", c.focus)
                BaseParserResponse(
                  data = None,
                  elementType = DataElementType.StringDataElement,
                  status =
                    if (state.isInSequence) BaseParserResponseStatus.END_OF_DATA
                    else BaseParserResponseStatus.ERROR
                )
              case \/-(data) =>
                val d = data.toString
                val status = if (state.isInSequence) {
                  val seqId = state.getCurrentSequence.asInstanceOf[Element].getAttribute("id")
                  if (jsonArrayHasMoreElements(c, structureElement)(
                        sequenceCounters.getOrElse(seqId, 0) + 1
                      ))
                    BaseParserResponseStatus.OK
                  else {
                    /*
                     * We are at the last entry of the current json array but if we are not on the last
                     * data element of the current sequence row, we must omit `END_OF_SEQUENCE`.
                     */
                    val dataElements = getParentSequence(structureElement).map(
                      s => getChildDataElementsFromElement(s)
                    )
                    if (dataElements.exists(
                          es =>
                            es.nonEmpty && es.reverse.head.getAttribute("id") == structureElement
                              .getAttribute("id")
                        ))
                      BaseParserResponseStatus.END_OF_SEQUENCE
                    else
                      BaseParserResponseStatus.OK
                  }
                } else
                  BaseParserResponseStatus.OK

                BaseParserResponse(
                  data = Option(d),
                  elementType = DataElementType.StringDataElement,
                  status = status
                )
            }
          case DataElementType.UnknownElement =>
            log.error("Unknown data element type {}!", structureElement.getNodeName)
            BaseParserResponse(
              data = None,
              elementType = DataElementType.UnknownElement,
              status = BaseParserResponseStatus.ERROR
            )
        }
      }
      .getOrElse(
        BaseParserResponse(
          data = None,
          elementType = DataElementType.UnknownElement,
          status = BaseParserResponseStatus.ERROR
        )
      )

  override def save(data: ParserDataContainer, dataHash: Long, referenceId: Option[String]): Unit = {
    val sourceSequenceRow =
      if (state.isInSequence)
        Option(state.getCurrentSequenceRowCount)
      else
        None
    if (referenceId.isDefined)
      dataTreeRef ! DataTreeDocumentMessages.SaveReferenceData(data,
                                                               dataHash,
                                                               referenceId.get,
                                                               sourceSequenceRow)
    else
      dataTreeRef ! DataTreeDocumentMessages.SaveData(data, dataHash)
  }

  override def parserStructuralElementHandler(e: Element): Unit =
    if (getElementType(e.getNodeName) == ElementType.StructuralElement) {
      val nextCursor: Option[ACursor] = cursor.map(
        c =>
          getStructureElementType(e.getNodeName) match {
            case StructureElementType.Element =>
              val workCursor =
                if (state.isInSequence && getParentSequence(e).exists(
                      s =>
                        e.getParentNode.asInstanceOf[Element].getAttribute("id") == s
                          .getAttribute("id")
                    ))
                  if (sequenceCounters.getOrElse(
                        state.getCurrentSequence.asInstanceOf[Element].getAttribute("id"),
                        0
                      ) > 0)
                    c :->- 1
                  else
                    c
                else
                  c
              if (e.hasAttribute(AttributeNames.JSON_ATTRIBUTE_NAME))
                workCursor --\ e.getAttribute(AttributeNames.JSON_ATTRIBUTE_NAME)
              else
                workCursor
            case StructureElementType.FixedSequence | StructureElementType.Sequence =>
              if (sequenceCounters.get(e.getAttribute("id")).isEmpty)
                sequenceCounters.put(e.getAttribute("id"), 0)
              c =\ sequenceCounters(e.getAttribute("id")) // Move to the currently active entry in the array.
            case _ => c // FIXME We may need to handle more element types here.
        }
      )
      cursor = nextCursor
    }

  override def parserFinishSequenceRowHandler(s: Element): Unit = {
    // We increase the counter variable for the current sequence because we just finished a row.
    val _ = sequenceCounters.put(s.getAttribute("id"),
                                 sequenceCounters.getOrElse(s.getAttribute("id"), -1) + 1)
  }

  override def parserResolveRecursionHandler(p: Element): Unit = {
    // Determine if the given element is a sequence.
    val isSeq = StructureElementType.isSequence(getStructureElementType(p.getNodeName))

    // If we have a parent sequence then move the cursor depending on the state of the sequence stack.
    getParentSequence(p).foreach(
      s =>
        cursor = cursor.map(
          c =>
            state.sequenceStack.size match {
              case 0 =>
                // No sequence on the stack.
                c.up
              case 1 =>
                // Exactly one sequence on the stack.
                if (isSeq) {
                  // The current element is a sequence itself.
                  // Reset the sequence counter when leaving it.
                  sequenceCounters.remove(p.getAttribute("id"))
                  c.up
                } else if (p.getParentNode.asInstanceOf[Element].getAttribute("id") == s
                             .getAttribute("id") && (c :->- 1).failed) {
                  c.up // We're directly "under" a sequence and the json array has no more "columns".
                } else
                  c
              case _ =>
                // We have a stacked sequence.
                if (isSeq) {
                  // The current element is a sequence itself.
                  // Reset the stacked sequences counter when leaving it.
                  sequenceCounters.remove(p.getAttribute("id"))
                  c.up
                } else if (p.getParentNode.asInstanceOf[Element].getAttribute("id") == s
                             .getAttribute("id") && (c :->- 1).failed)
                  c.up // We're directly "under" a sequence and the json array has no more "columns".
                else
                  c
          }
      )
    )

    // If the current element has a sibling dfasdl element then we move one field upwards.
    getSibling(p).foreach(s => cursor = cursor.map(c => c.up))
  }
}
