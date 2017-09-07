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

import java.io.{ BufferedInputStream, IOException }
import java.nio.charset.Charset

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams
import com.jcraft.jsch.Session
import com.wegtam.tensei.adt.{ ConnectionInformation, Cookbook }
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.adt._
import com.wegtam.tensei.agent.helpers.{ LoggingHelpers, NetworkFileHelpers }
import com.wegtam.tensei.agent.parsers.NetworkFileParser.{
  ExtractDataWithRegExResponse,
  NetworkFileParserReadElementOptions
}
import org.apache.commons.net.ftp.{ FTPClient, FTPSClient }
import org.apache.http.client.methods.CloseableHttpResponse
import org.dfasdl.utils.{ AttributeNames, DataElementType, ElementNames }
import org.w3c.dom.Element

import scala.util.matching.Regex

object NetworkFileParser {
  final case class NetworkFileParserReadElementOptions(encoding: Charset,
                                                       start_sign: String = "",
                                                       stop_sign: String = "",
                                                       format: String = "",
                                                       isInChoice: Boolean = false,
                                                       sequence_stop_sign: String = "",
                                                       correct_offset: Long = 0,
                                                       preferSequenceStopSign: Boolean = false)

  /**
    * Return value for data extraction using regular expressions.
    *
    * @param data                    An option to the `Regex.Match` of the data.
    * @param sequencePatternMatched  A flag that is set to `true` if  the pattern that matched was the sequence pattern.
    */
  final case class ExtractDataWithRegExResponse(data: Option[Regex.Match],
                                                sequencePatternMatched: Boolean = false)

  /**
    * Helper method to create a network file parser actor.
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
    Props(classOf[NetworkFileParser], source, cookbook, dataTreeRef, agentRunIdentifier)
}

/**
  * A simple network file parser that parses a given file and tries to match it to a given dfasdl.
  *
  * @param source              The source connection to retrieve the data from.
  * @param cookbook            The cookbook holding the source dfasdl.
  * @param dataTreeRef         The actor ref to the data tree e.g. where to put the parsed data.
  * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
  */
class NetworkFileParser(source: ConnectionInformation,
                        cookbook: Cookbook,
                        dataTreeRef: ActorRef,
                        agentRunIdentifier: Option[String])
    extends Actor
    with ActorLogging
    with BaseParser
    with NetworkFileHelpers {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.clearMDC()
    super.postStop()
  }

  var bufferedInput: Option[BufferedInputStream] = None

  var currentOffset: Long = 0

  var clientConnection: Option[Any] = None

  var defaultEncoding: Charset = Charsets.UTF_8

  def receive = {
    case BaseParserMessages.SubParserInitialize =>
      log.info("Initialize the NetworkFileParser")
      initializeInputStream()
      sender() ! BaseParserMessages.SubParserInitialized
    case BaseParserMessages.Start =>
      log.debug("Starting NetworkFileParser")
      parseNetworkFile()
      sender() ! ParserStatusMessage(ParserStatus.COMPLETED, Option(context.self))
    case BaseParserMessages.Stop =>
      log.debug("Stopping NetworkFileParser")
      context stop self
    case BaseParserMessages.Status =>
      log.error("Status request not yet implemented!")
  }

  def initializeInputStream(): Unit = {
    if (clientConnection.isDefined) {
      clientConnection.get match {
        case chr: CloseableHttpResponse =>
          val c = clientConnection.get.asInstanceOf[CloseableHttpResponse]
          c.close()
        case ftpsClient: FTPSClient =>
          val c = clientConnection.get.asInstanceOf[FTPSClient]
          if (c.isConnected)
            c.disconnect()
        case ftpClient: FTPClient =>
          val c = clientConnection.get.asInstanceOf[FTPClient]
          if (c.isConnected)
            c.disconnect()
        case sftpClient: Session =>
          val c = clientConnection.get.asInstanceOf[Session]
          if (c.isConnected)
            c.disconnect()
      }
      clientConnection = None
    }

    val createdStream = createStream(source, context)
    bufferedInput = createdStream._1
    clientConnection = createdStream._2
  }

  private def parseNetworkFile(): Unit =
    if (source.dfasdlRef.isDefined && cookbook.findDFASDL(source.dfasdlRef.get).isDefined) {
      if (clientConnection.isDefined) {
        if (bufferedInput.isDefined) {
          val xml  = createNormalizedDocument(cookbook.findDFASDL(source.dfasdlRef.get).get.content)
          val root = xml.getDocumentElement
          if (root.hasAttribute(AttributeNames.DEFAULT_ENCODING))
            defaultEncoding = Charset.forName(root.getAttribute(AttributeNames.DEFAULT_ENCODING))
          else
            defaultEncoding = Charsets.UTF_8
          traverseTree(xml, log)
        } else
          log.error("No input stream enabled for connection: {}", source.uri)
      } else
        log.error("No client connection successfully enabled with: {}", source.uri)
    } else
      log.error("No DFASDL defined for {} in cookbook {}", source.uri, cookbook.id)

  override def readDataElement(structureElement: Element,
                               useOffset: Long = -1,
                               isInChoice: Boolean = false): BaseParserResponse = {
    // Reset offset if desired
    if (useOffset > -1) {
      currentOffset = useOffset
    }

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
    val options = NetworkFileParserReadElementOptions(encoding,
                                                      start_sign,
                                                      stop_sign,
                                                      isInChoice = isInChoice,
                                                      sequence_stop_sign = sequence_stop_sign,
                                                      correct_offset = correct_offset)
    val response = getDataElementType(structureElement.getTagName) match {
      case DataElementType.BinaryDataElement =>
        readNextByteElement(bufferedInput.get, currentOffset, options)
      case DataElementType.StringDataElement =>
        structureElement.getTagName match {
          case ElementNames.FORMATTED_STRING | ElementNames.FORMATTED_NUMBER =>
            if (structureElement.hasAttribute(AttributeNames.FORMAT)) {
              readNextStringElement(
                bufferedInput.get,
                currentOffset,
                NetworkFileParserReadElementOptions(
                  options.encoding,
                  options.start_sign,
                  options.stop_sign,
                  structureElement.getAttribute(AttributeNames.FORMAT),
                  isInChoice = options.isInChoice
                )
              )
            } else
              throw new RuntimeException(
                s"${structureElement.getTagName} (${structureElement.getAttribute("id")}) without format attribute!"
              )
          case ElementNames.STRING =>
            readNextStringElement(bufferedInput.get, currentOffset, options)
          case _ =>
            readNextStringElement(bufferedInput.get, currentOffset, options) //TODO
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

  /**
    * Create the regular expression for matching our data with start and stop sign.
    *
    * @param options The file parser options.
    * @param ignoreStopSign Set this to `true` to ignore the stop sign option.
    * @param useSequenceStopSign Set this to `true` to use the sequence stop sign if present.
    * @return A regular expression to extract the actual data.
    * @throws IllegalArgumentException If `useSequenceStopSign` is `true` but no `sequence_stop_sign` is set.
    */
  private def buildRegularExpression(options: NetworkFileParserReadElementOptions,
                                     ignoreStopSign: Boolean = false,
                                     useSequenceStopSign: Boolean = false): Regex = {
    if (useSequenceStopSign)
      require(options.sequence_stop_sign.length > 0, "No sequence stop sign set!")

    // We use this trick (prepending `)` or `?)`) to switch between greedy and non greedy matching.
    // If the stop sign is ignored we actually need greedy matching.
    val stop_sign =
      if (ignoreStopSign)
        ")"
      else {
        if (useSequenceStopSign)
          s"?)(${options.sequence_stop_sign})"
        else
          s"?)(${options.stop_sign})"
      }

    options.isInChoice match {
      case true =>
        if (options.start_sign.isEmpty)
          s"(?ms)(.*$stop_sign".r
        else
          s"(?ms)(${options.start_sign}.*$stop_sign".r
      case false =>
        if (options.start_sign.isEmpty)
          s"(?ms)(.*$stop_sign".r
        else
          s"(?ms)(${options.start_sign}.*$stop_sign".r
    }
  }

  /**
    * Extract the data from a given string via the given regular expressions and return a tuple containing
    * the regex match option and a boolean flag. The flag is set to `true` if the sequence pattern matched.
    *
    * @param src The source string containing the data.
    * @param options The file parser options.
    * @return A case class holding an option to the matched data and a flag indicating if the sequence pattern matched.
    */
  private def extractDataWithRegularExpression(
      src: String,
      options: NetworkFileParserReadElementOptions
  ): ExtractDataWithRegExResponse = {
    val pattern =
      buildRegularExpression(options, useSequenceStopSign = options.preferSequenceStopSign)

    val simpleMatch =
      if (options.isInChoice)
        pattern.findPrefixMatchOf(src)
      else
        pattern.findFirstMatchIn(src)
    val sequenceMatch = if (options.sequence_stop_sign.length > 0) {
      val sequencePattern = buildRegularExpression(options, useSequenceStopSign = true)
      if (options.isInChoice)
        sequencePattern.findPrefixMatchOf(src)
      else
        sequencePattern.findFirstMatchIn(src)
    } else
      None

    // Check which match and flag to return.
    if (sequenceMatch.isDefined)
      if (simpleMatch.isDefined) {
        val diff = simpleMatch.get.group(1).length - sequenceMatch.get.group(1).length

        if (diff < 0)
          ExtractDataWithRegExResponse(data = simpleMatch, sequencePatternMatched = false) // Return the element match if the element matched "before" the sequence.
        else if (diff == 0)
          ExtractDataWithRegExResponse(data = sequenceMatch, sequencePatternMatched = true) // Return the sequence match and set the "end sequence" flag if both matched equally.
        else
          ExtractDataWithRegExResponse(data = None, sequencePatternMatched = true) // Return nothing and set the "end sequence" flag if the sequence matched "before" the element.
      } else
        ExtractDataWithRegExResponse(data = None, sequencePatternMatched = true) // We have only a sequence match.
    else
      ExtractDataWithRegExResponse(data = simpleMatch, sequencePatternMatched = false) // We have only a normal match.
  }

  private def readNextByteElement(
      src: BufferedInputStream,
      offset: Long = 0,
      options: NetworkFileParserReadElementOptions
  ): BaseParserResponse = {
    val chunkSize = 100
    val encoding  = options.encoding

    var loop                                                          = true
    var myChunkSize                                                   = chunkSize
    var matchedElement: Array[Byte]                                   = new Array[Byte](0)
    var matchedStopSign: Array[Byte]                                  = new Array[Byte](0)
    var status: BaseParserResponseStatus.BaseParserResponseStatusType = BaseParserResponseStatus.OK
    src.mark(10000)
    try {
      do {
        // we skip the offset at the beginning
        src.reset()
        src.skip(offset)
        val bytes: Array[Byte] = new Array[Byte](myChunkSize)
        val readBytes          = ByteStreams.read(src, bytes, 0, myChunkSize)

        if (options.stop_sign == "EOF") {
          val bytesBuf: Array[Byte] = new Array[Byte](chunkSize + myChunkSize)
          val con                   = ByteStreams.read(src, bytesBuf, myChunkSize, chunkSize)
          src.reset()

          if (con == 0) {
            // take only the really `readBytes` from the beginnin of the function
            // otherwise, the rest aof the initialized array will be returned , too
            matchedElement = bytes.take(readBytes)
            status = BaseParserResponseStatus.END_OF_DATA
            loop = false
          }
        } else {
          val str              = new String(bytes, options.encoding)
          val extractWithRegEx = extractDataWithRegularExpression(str, options)

          // Check if we have reached the end of a sequence.
          if (extractWithRegEx.sequencePatternMatched) {
            status = BaseParserResponseStatus.END_OF_SEQUENCE
            if (extractWithRegEx.data.isDefined) {
              matchedElement = extractWithRegEx.data.get.group(1).getBytes(encoding)
              matchedStopSign = extractWithRegEx.data.get
                .group(extractWithRegEx.data.get.groupCount)
                .getBytes(encoding)
            } else {
              // If we didn't match, we try to return the content matching to the element using the sequence stop sign.
              val extractWithSeqRegEx =
                extractDataWithRegularExpression(str, options.copy(preferSequenceStopSign = true))
              if (extractWithSeqRegEx.data.isDefined) {
                matchedElement = extractWithSeqRegEx.data.get.group(1).getBytes(encoding)
                matchedStopSign = extractWithSeqRegEx.data.get
                  .group(extractWithSeqRegEx.data.get.groupCount)
                  .getBytes(encoding)
              }
            }
            loop = false
          } else {
            // If we have a match, buffer it and end the loop.
            if (extractWithRegEx.data.isDefined) {
              matchedElement = extractWithRegEx.data.get.group(1).getBytes(encoding)
              matchedStopSign = extractWithRegEx.data.get
                .group(extractWithRegEx.data.get.groupCount)
                .getBytes(encoding)
              loop = false
            } else {
              // Check if we have reached the end of the file.
              val bytesBuf: Array[Byte] = new Array[Byte](myChunkSize + chunkSize)
              val con                   = ByteStreams.read(src, bytesBuf, myChunkSize, chunkSize)
              src.reset()
              if (con == 0) {
                if (state.isInChoice) {
                  // We are within a choice.
                  status = BaseParserResponseStatus.ERROR
                  // If the stop sign equals the default stop sign, e.g. a line break.
                  if (options.stop_sign == DEFAULT_STOP_SIGN) {
                    // We need to ignore the stop sign in this case because usually there is no line break at the end of the file.
                    val endPattern = buildRegularExpression(options, ignoreStopSign = true)
                    val endMatch   = endPattern.findPrefixMatchOf(str)
                    if (endMatch.isDefined) {
                      // take only the really `readBytes` from the beginnin of the function
                      // otherwise, the rest aof the initialized array will be returned , too
                      matchedElement =
                        if (endMatch.get.groupCount > 0)
                          endMatch.get.group(1).getBytes.take(readBytes)
                        else {
                          log.warning("No data extracted using end pattern '{}'!",
                                      endPattern.toString())
                          Array.empty[Byte]
                        }
                      status = BaseParserResponseStatus.END_OF_DATA
                    }
                  }
                } else {
                  // take only the really `readBytes` from the beginnin of the function
                  // otherwise, the rest aof the initialized array will be returned , too
                  matchedElement =
                    if (readBytes > 0)
                      bytes.take(readBytes)
                    else
                      bytes
                  status = BaseParserResponseStatus.END_OF_DATA
                }
                loop = false
              }
            }
          }
        }
        if (loop) {
          myChunkSize += chunkSize
          src.reset()
        }
      } while (loop)
    } catch {
      case e: IOException =>
        log.error(e, "An error occurred while trying to read the next byte element!")
        log.debug("Using offset {} and read element options: {}", offset, options)
    }
    val lastOffset =
      if (matchedElement.length == 0)
        offset + 1
      else
        offset + matchedElement.length + matchedStopSign.length

    if (matchedElement.length > 0) {
      src.reset()
      BaseParserResponse(Option(matchedElement),
                         DataElementType.BinaryDataElement,
                         lastOffset,
                         status)
    } else
      BaseParserResponse(None, DataElementType.BinaryDataElement, lastOffset, status)
  }

  private def readNextStringElement(
      src: BufferedInputStream,
      offset: Long = 0,
      options: NetworkFileParserReadElementOptions
  ): BaseParserResponse = {
    val response = readNextByteElement(src, offset, options)
    if (response.status != BaseParserResponseStatus.ERROR) {
      val e =
        if (response.data.isDefined) {
          val bytes: Array[Byte] = response.data.get.asInstanceOf[Array[Byte]]
          if (options.format.isEmpty)
            Option(new String(bytes, options.encoding))
          else {
            val tmpString = new String(bytes, options.encoding)
            val pattern   = s"(?s)${options.format}".r
            val m         = pattern.findFirstMatchIn(tmpString)
            if (m.isDefined)
              if (m.get.groupCount > 0)
                Option(m.get.group(1))
              else {
                log.warning("No data could be extracted using element format '{}'!", options.format)
                None
              } else {
              log.warning("Format '{}' did not match for parsed element!", options.format)
              None
            }
          }
        } else
          None

      BaseParserResponse(e, DataElementType.StringDataElement, response.offset, response.status)
    } else
      BaseParserResponse(None, DataElementType.StringDataElement, response.offset, response.status)
  }
}
