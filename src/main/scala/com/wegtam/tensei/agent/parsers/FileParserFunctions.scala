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

import java.io.IOException
import java.nio.charset.Charset

import akka.event.DiagnosticLoggingAdapter
import com.google.common.io.ByteSource
import com.wegtam.tensei.agent.adt.{ BaseParserResponse, BaseParserResponseStatus }
import com.wegtam.tensei.agent.parsers.FileParserFunctions.ExtractDataWithRegExResponse
import org.dfasdl.utils.DataElementType

import scala.util.matching.Regex

/**
  * Provide functionalities for the file parser.
  */
trait FileParserFunctions {
  import FileParserFunctions.FileParserReadElementOptions

  /**
    * Create the regular expression for matching our data with start and stop sign.
    *
    * @param options The file parser options.
    * @param ignoreStopSign Set this to `true` to ignore the stop sign option.
    * @param useSequenceStopSign Set this to `true` to use the sequence stop sign if present.
    * @return A regular expression to extract the actual data.
    * @throws IllegalArgumentException If `useSequenceStopSign` is `true` but no `sequence_stop_sign` is set.
    */
  def buildRegularExpression(options: FileParserReadElementOptions,
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

    if (options.isInChoice) {
      if (options.start_sign.isEmpty)
        s"(?ms)(.*$stop_sign".r
      else
        s"(?ms)(${options.start_sign}.*$stop_sign".r
    } else {
      if (options.start_sign.isEmpty)
        s"(?ms)(.*$stop_sign".r
      else
        s"(?ms)(${options.start_sign}.*$stop_sign".r
    }
  }

  /**
    * Extract the data from a given string via the given regular expressions and return it.
    *
    * @param src     The source string containing the data.
    * @param options The file parser options.
    * @return A case class holding an option to the matched data and a flag indicating if the sequence pattern matched.
    */
  def extractDataWithRegularExpression(
      src: String,
      options: FileParserReadElementOptions
  ): ExtractDataWithRegExResponse = {
    val pattern: Regex =
      buildRegularExpression(options, useSequenceStopSign = options.preferSequenceStopSign)
    extractDataWithRegularExpression(options, pattern, src)
  }

  /**
    * Extract the data from a given string via the given regular expressions and return it.
    *
    * @param options The file parser options.
    * @param pattern The regular expression pattern used for extraction.
    * @param src     The source string containing the data.
    * @return A case class holding an option to the matched data and a flag indicating if the sequence pattern matched.
    */
  def extractDataWithRegularExpression(options: FileParserReadElementOptions,
                                       pattern: Regex,
                                       src: String): ExtractDataWithRegExResponse = {
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

  /**
    * Read the bytes of the next element from the file data source.
    *
    * @param src             A bytesource containing the data.
    * @param offset          The current offset.
    * @param options         The file parser options.
    * @param defaultStopSign The global default stop sign.
    * @param state           The current base parser state.
    * @param log             A logging adapter that is used for logging.
    * @return A base parser response with the result of the operation.
    */
  def readNextByteElement(
      src: ByteSource,
      offset: Long = 0,
      options: FileParserReadElementOptions,
      defaultStopSign: String,
      state: BaseParserState
  )(implicit log: DiagnosticLoggingAdapter): BaseParserResponse = {
    val chunkSize = 256L
    val encoding  = options.encoding

    var loop                                                          = true
    var myChunkSize                                                   = chunkSize
    var matchedElement: Array[Byte]                                   = new Array[Byte](0)
    var matchedStopSign: Array[Byte]                                  = new Array[Byte](0)
    var status: BaseParserResponseStatus.BaseParserResponseStatusType = BaseParserResponseStatus.OK
    try {
      do {
        val byteSource = src.slice(offset, myChunkSize)
        val bytes      = byteSource.read()
        if (options.stop_sign == "EOF") {
          if (src.slice(offset + myChunkSize, chunkSize).isEmpty) {
            matchedElement = bytes
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
              if (src.slice(offset + myChunkSize, chunkSize).isEmpty) {
                if (state.isInChoice) {
                  // We are within a choice.
                  status = BaseParserResponseStatus.ERROR
                  // If the stop sign equals the default stop sign, e.g. a line break.
                  if (options.stop_sign == defaultStopSign) {
                    // We need to ignore the stop sign in this case because usually there is no line break at the end of the file.
                    val endPattern = buildRegularExpression(options, ignoreStopSign = true)
                    val endMatch   = endPattern.findPrefixMatchOf(str)
                    if (endMatch.isDefined) {
                      matchedElement =
                        if (endMatch.get.groupCount > 0)
                          endMatch.get.group(1).getBytes(encoding)
                        else {
                          log.warning("No data extracted using end pattern '{}'!",
                                      endPattern.toString())
                          Array.empty[Byte]
                        }

                      status = BaseParserResponseStatus.END_OF_DATA
                    }
                  }
                } else {
                  matchedElement = bytes
                  status = BaseParserResponseStatus.END_OF_DATA
                }
                loop = false
              }
            }
          }
        }
        if (loop) myChunkSize += chunkSize
      } while (loop)
    } catch {
      case e: IOException =>
        log.error(e, "An error occurred while trying to read the next byte element!")
        log.debug("Using offset {} and read element options: {}", offset, options)
    }
    val lastOffset =
      if (matchedElement.length == 0) {
        // If the stop-sign has matched, we must increase the offset by the length
        // of the stop-sign that can be bigger than 1.
        if (matchedStopSign.length > 0)
          offset + matchedStopSign.length
        else
          offset + 1
      } else
        offset + matchedElement.length + matchedStopSign.length

    if (matchedElement.length > 0)
      BaseParserResponse(Option(matchedElement),
                         DataElementType.BinaryDataElement,
                         lastOffset,
                         status)
    else
      BaseParserResponse(None, DataElementType.BinaryDataElement, lastOffset, status)
  }

  /**
    * Read the next string element from the data source.
    *
    * @param src             A bytesource containing the data.
    * @param offset          The current offset.
    * @param options         The file parser options.
    * @param defaultStopSign The global default stop sign.
    * @param state           The current base parser state.
    * @param log             A logging adapter that is used for logging.
    * @return A base parser response with the result of the operation.
    */
  def readNextStringElement(
      src: ByteSource,
      offset: Long = 0,
      options: FileParserReadElementOptions,
      defaultStopSign: String,
      state: BaseParserState
  )(implicit log: DiagnosticLoggingAdapter): BaseParserResponse = {
    val response = readNextByteElement(src, offset, options, defaultStopSign, state)

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
                log.warning("No data could be extracted using element format '{}'!",
                            options.format)
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

object FileParserFunctions {

  /**
    * Options for the functions that are supposed to read the next element.
    *
    * @param encoding               A charset specifying the encoding to use for decoding string data.
    * @param start_sign             The start sign.
    * @param stop_sign              The stop sign.
    * @param format                 A format string for elements like formatnum and formatstr.
    * @param isInChoice             A flag indicating if the parser is currently within a choice.
    * @param sequence_stop_sign     The stop sign for the parent sequence of the element.
    * @param correct_offset         Number of bytes that shall be used to correct the offset.
    * @param preferSequenceStopSign A flag that if set to `true` requires the sequence stop sign to take precedence over the regular stop sign.
    */
  final case class FileParserReadElementOptions(
      encoding: Charset,
      start_sign: String = "",
      stop_sign: String = "",
      format: String = "",
      isInChoice: Boolean = false,
      sequence_stop_sign: String = "",
      correct_offset: Long = 0,
      preferSequenceStopSign: Boolean = false
  )

  /**
    * Return value for data extraction using regular expressions.
    *
    * @param data                    An option to the `Regex.Match` of the data.
    * @param sequencePatternMatched  A flag that is set to `true` if  the pattern that matched was the sequence pattern.
    */
  final case class ExtractDataWithRegExResponse(
      data: Option[Regex.Match],
      sequencePatternMatched: Boolean = false
  )

}
