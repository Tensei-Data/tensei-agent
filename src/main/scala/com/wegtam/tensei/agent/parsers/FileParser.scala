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

import java.io.File
import java.nio.charset.Charset

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.google.common.base.Charsets
import com.google.common.io.{ ByteSource, Files }
import com.wegtam.tensei.adt.{ ConnectionInformation, Cookbook }
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.adt._
import com.wegtam.tensei.agent.helpers.LoggingHelpers
import com.wegtam.tensei.agent.parsers.FileParserFunctions.FileParserReadElementOptions
import org.dfasdl.utils.{ AttributeNames, DataElementType, ElementNames }
import org.w3c.dom.Element

/**
  * A simple file parser that parses a given file byte per byte and tries to match it to a given dfasdl.
  *
  * @param source              The source connection to retrieve the data from.
  * @param cookbook            The cookbook holding the source dfasdl.
  * @param dataTreeRef         The actor ref to the data tree e.g. where to put the parsed data.
  * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
  */
class FileParser(
    source: ConnectionInformation,
    cookbook: Cookbook,
    dataTreeRef: ActorRef,
    agentRunIdentifier: Option[String]
) extends Actor
    with ActorLogging
    with BaseParser
    with FileParserFunctions {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.clearMDC()
    super.postStop()
  }

  val byteSource: ByteSource   = Files.asByteSource(new File(source.uri.getSchemeSpecificPart))
  var currentOffset: Long      = 0
  var defaultEncoding: Charset = Charsets.UTF_8

  def receive: Receive = {
    case BaseParserMessages.SubParserInitialize =>
      sender() ! BaseParserMessages.SubParserInitialized
    case BaseParserMessages.Start =>
      log.debug("Starting FileParser")
      parseFile()
      sender() ! ParserStatusMessage(ParserStatus.COMPLETED, Option(context.self))
    case BaseParserMessages.Stop =>
      log.debug("Stopping FileParser")
      context stop self
    case BaseParserMessages.Status =>
      log.error("Status request not yet implemented!")
  }

  private def parseFile(): Unit =
    if (source.dfasdlRef.isDefined && cookbook.findDFASDL(source.dfasdlRef.get).isDefined) {
      val xml  = createNormalizedDocument(cookbook.findDFASDL(source.dfasdlRef.get).get.content)
      val root = xml.getDocumentElement
      if (root.hasAttribute(AttributeNames.DEFAULT_ENCODING))
        defaultEncoding = Charset.forName(root.getAttribute(AttributeNames.DEFAULT_ENCODING))
      else
        defaultEncoding = Charsets.UTF_8
      traverseTree(xml, log)
    } else
      log.error("No DFASDL defined for {} in cookbook {}", source.uri, cookbook.id)

  override def parserFinishSequenceRowHandler(s: Element): Unit =
    if (state.getCurrentSequenceRowCount > 0 && state.getCurrentSequenceRowCount % 10000 == 0)
      log.info("Parsed {} rows of sequence '{}'.",
               state.getCurrentSequenceRowCount,
               s.getAttribute("id")) // DEBUG

  override def readDataElement(structureElement: Element,
                               useOffset: Long = -1,
                               isInChoice: Boolean = false): BaseParserResponse = {
    implicit val loggingAdapter = log

    // Reset offset if desired
    if (useOffset > -1) currentOffset = useOffset

    // Get start sign or set it to an empty string.
    val start_sign =
      if (structureElement.hasAttribute(AttributeNames.START_SIGN))
        structureElement.getAttribute(AttributeNames.START_SIGN)
      else
        ""
    // Get stop sign or set it to default.
    val stop_sign =
      if (structureElement.hasAttribute(AttributeNames.STOP_SIGN))
        structureElement.getAttribute(AttributeNames.STOP_SIGN)
      else
        DEFAULT_STOP_SIGN
    // Get encoding or set it to default.
    val encoding = if (structureElement.hasAttribute(AttributeNames.ENCODING)) {
      Charset.forName(structureElement.getAttribute(AttributeNames.ENCODING))
    } else
      defaultEncoding
    // If we are within a sequence we have to set it's stop sign if present.
    val sequence_stop_sign =
      if (state.isInSequence && state.getCurrentSequence
            .asInstanceOf[Element]
            .hasAttribute(AttributeNames.STOP_SIGN)) {
        state.getCurrentSequence.asInstanceOf[Element].getAttribute(AttributeNames.STOP_SIGN)
      } else
        ""
    // Do we have an offset correction set?
    val correct_offset =
      if (structureElement.hasAttribute(AttributeNames.CORRECT_OFFSET))
        structureElement.getAttribute(AttributeNames.CORRECT_OFFSET).toLong
      else
        0

    // Get response.
    val options = FileParserReadElementOptions(encoding,
                                               start_sign,
                                               stop_sign,
                                               isInChoice = isInChoice,
                                               sequence_stop_sign = sequence_stop_sign,
                                               correct_offset = correct_offset)

    val response = getDataElementType(structureElement.getTagName) match {
      case DataElementType.BinaryDataElement =>
        readNextByteElement(byteSource, currentOffset, options, DEFAULT_STOP_SIGN, state)
      case DataElementType.StringDataElement =>
        structureElement.getTagName match {
          case ElementNames.FORMATTED_STRING | ElementNames.FORMATTED_NUMBER =>
            if (structureElement.hasAttribute(AttributeNames.FORMAT))
              readNextStringElement(
                byteSource,
                currentOffset,
                FileParserReadElementOptions(options.encoding,
                                             options.start_sign,
                                             options.stop_sign,
                                             structureElement.getAttribute(AttributeNames.FORMAT),
                                             isInChoice = options.isInChoice),
                DEFAULT_STOP_SIGN,
                state
              )
            else
              throw new RuntimeException(
                s"${structureElement.getTagName} (${structureElement.getAttribute("id")}) without format attribute!"
              )
          case ElementNames.STRING =>
            readNextStringElement(byteSource, currentOffset, options, DEFAULT_STOP_SIGN, state)
          case _ =>
            readNextStringElement(byteSource, currentOffset, options, DEFAULT_STOP_SIGN, state) //TODO
        }

      case DataElementType.UnknownElement =>
        throw new RuntimeException(s"Unknown data element type: ${structureElement.getTagName}")
    }
    // Save offset.
    currentOffset = response.offset
    // Return response.
    response
  }

  override def save(data: ParserDataContainer,
                    dataHash: Long,
                    referenceId: Option[String] = None): Unit = {
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

}

object FileParser {

  /**
    * Helper method to create a file parser actor.
    *
    * @param source              The source connection to retrieve the data from.
    * @param cookbook            The cookbook holding the source dfasdl.
    * @param dataTreeRef         The actor ref to the data tree e.g. where to put the parsed data.
    * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
    * @return The props to create the actor.
    */
  def props(
      source: ConnectionInformation,
      cookbook: Cookbook,
      dataTreeRef: ActorRef,
      agentRunIdentifier: Option[String]
  ): Props = Props(new FileParser(source, cookbook, dataTreeRef, agentRunIdentifier))

}
