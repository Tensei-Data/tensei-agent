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

import java.io.{ BufferedInputStream, File }
import java.util.Locale

import akka.actor.ActorContext
import com.wegtam.tensei.adt.{ ConnectionInformation, DFASDL, ExtractSchemaOptions }
import com.wegtam.tensei.agent.SchemaExtractor.ExtractorMetaData
import org.dfasdl.utils.{ AttributeNames, DocumentHelpers, ElementNames }
import org.w3c.dom.{ Document, Element }

import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource
import scalaz.Scalaz._
import scalaz._

/**
  * The CSV schema extractor extracts the content of the CSV file and
  * creates a DFASDL.
  */
trait CSVSchemaExtractor
    extends DocumentHelpers
    with XmlHelpers
    with ExtractorHelpers
    with NetworkFileHelpers {
  final val LINES_TO_EXTRACT = 50

  /**
    * Get a file object of the CSV file.
    *
    * @param con  The provided connection information for the CSV file.
    * @return A new java.io.File
    */
  def getCSVFile(con: ConnectionInformation): ValidationNel[String, java.io.File] =
    try {
      val file = new File(con.uri.getSchemeSpecificPart)

      if (file.exists() && file.canRead)
        file.successNel
      else
        s"Can't access file: ${file.getAbsolutePath}".failNel
    } catch {
      case e: Throwable => GenericHelpers.createValidationFromException[java.io.File](e)
    }

  /**
    * Get a network stream for a network CSV file.
    *
    * @param con  The provided connection information for the CSV file.
    * @return A new java.io.File
    */
  def getNetworkCSVStream(
      con: ConnectionInformation,
      context: ActorContext
  ): ValidationNel[String, (Option[BufferedInputStream], Option[Any])] =
    try {
      val createdStream = createStream(con, context)

      if (createdStream._1.isDefined && createdStream._2.isDefined)
        createdStream.successNel
      else
        s"Can't access the network file: ${con.uri.getSchemeSpecificPart}".failNel
    } catch {
      case e: Throwable =>
        GenericHelpers.createValidationFromException[(Option[BufferedInputStream], Option[Any])](e)
    }

  /**
    * Entry point for network files.
    *
    * @param bufferedInput        The buffered input stream of the network file.
    * @param params               Parameters that are relevant for the extraction.
    * @param extractorMetaData    Additional information for the extraction process.
    * @return The created DFASDL.
    */
  def extractFromNetworkCSVFile(
      bufferedInput: BufferedInputStream,
      params: ExtractSchemaOptions,
      extractorMetaData: ExtractorMetaData
  ): Option[DFASDL] = {
    val bufferedSource = new BufferedSource(bufferedInput)
    createDFASDL(bufferedSource, params, extractorMetaData)
  }

  /**
    * Entry point for local files.
    *
    * @param file                 The local file.
    * @param params               Parameters that are relevant for the extraction.
    * @param extractorMetaData    Additional information for the extraction process.
    * @return The created DFASDL or None.
    */
  def extractFromCSV(file: File,
                     params: ExtractSchemaOptions,
                     extractorMetaData: ExtractorMetaData): Option[DFASDL] = {
    val bufferedSource =
      scala.io.Source.fromFile(file, params.encoding.getOrElse(DEFAULT_ENCODING))
    createDFASDL(bufferedSource, params, extractorMetaData)
  }

  /**
    * Create the DFASDL from the provided buffered source and the parameters.
    *
    * @param sourceFile           The buffered source of the file.
    * @param params               Parameters that are relevant for the extraction.
    * @param extractorMetaData    Additional information for the extraction process.
    * @return The created DFASDL or None.
    */
  def createDFASDL(
      sourceFile: BufferedSource,
      params: ExtractSchemaOptions,
      extractorMetaData: ExtractorMetaData
  ): Option[DFASDL] = {
    val hasHeader: Boolean = params.csvHeader
    val separator: String  = params.csvSeparator.getOrElse(",")

    // Just read the lines from the CSV into a buffer
    val extractedLines = readLinesFromSource(sourceFile)

    val extractedColumns =
      extractColumns(extractedLines, separator, hasHeader)

    // The document skeleton
    val loader   = createDocumentBuilder()
    val document = loader.newDocument()
    val dfasdl   = document.createElement(ElementNames.ROOT)
    dfasdl.setAttribute("xmlns", "http://www.dfasdl.org/DFASDL")
    dfasdl.setAttribute(AttributeNames.SEMANTIC_SCHEMA, "custom")
    params.encoding.foreach(e => dfasdl.setAttribute(AttributeNames.DEFAULT_ENCODING, e))
    document.appendChild(dfasdl)

    val headerLine: Option[List[String]] =
      if (hasHeader)
        Option(
          extractedLines(0).split(separator).map(entry => cleanElementId(entry.trim())).toList
        )
      else
        None

    // If we have a header line and can successfully create an element for it, the element will be appended to the DFASDL.
    headerLine.foreach(
      line =>
        createHeaderElement(line, document, separator)
          .foreach(header => dfasdl.appendChild(header))
    )

    val sequence =
      createSequence(document, extractedColumns, headerLine, separator, extractorMetaData)

    if (sequence.isDefined)
      dfasdl.appendChild(sequence.get)

    Option(new DFASDL(s"${extractorMetaData.dfasdlNamePart}-file", prettifyXml(document), "1"))
  }

  /**
    * Create the header element for the DFASDL.
    *
    * @param headers    List of header names.
    * @param document   The document object.
    * @param separator  The separator.
    * @return The header element.
    */
  def createHeaderElement(headers: List[String],
                          document: Document,
                          separator: String): Option[Element] = {
    val headerElement = document.createElement(ElementNames.ELEMENT)
    headerElement.setAttribute("id", "csv_header")

    for (headerPos <- headers.indices) {
      val elem = document.createElement(ElementNames.STRING)
      elem.setAttribute(
        "id",
        s"csv_header_${headers(headerPos).replaceAll("^[\"']", "").replaceAll("[\"']$", "").toLowerCase(Locale.ROOT)}"
      )

      if (headerPos < headers.length - 1)
        elem.setAttribute(AttributeNames.STOP_SIGN, separator)

      headerElement.appendChild(elem)
    }
    Option(headerElement)
  }

  /**
    * Read the single lines into a buffer and return a Map that maps the number of the line
    * to the content.
    *
    * @param source     A buffered source of the CSV file.
    * @return A map that contains the number of the line with the content of the line.
    */
  def readLinesFromSource(source: BufferedSource): scala.collection.mutable.HashMap[Int, String] = {
    val iterator = source.getLines()
    var position = 0
    val buffer   = new scala.collection.mutable.HashMap[Int, String]
    iterator.toStream
      .takeWhile(_ => position < LINES_TO_EXTRACT)
      .foreach(line => {
        buffer.put(position, line)
        position += 1
      })
    buffer
  }

  /**
    * Extract the single columns of the CSV file. If a the file contains a header, the header will
    * also be extracted from the file.
    *
    * @param lines      The single lines of the CSV file.
    * @param separator  The separator between the single entries of a row.
    * @param hasHeader  Whether the file contains header information for each column.
    * @return A mapping for each column to the content of the column.
    */
  def extractColumns(
      lines: scala.collection.mutable.HashMap[Int, String],
      separator: String,
      hasHeader: Boolean
  ): scala.collection.mutable.HashMap[Int, ListBuffer[String]] = {
    val buffer = new scala.collection.mutable.HashMap[Int, ListBuffer[String]]()
    val start  = if (hasHeader) 1 else 0
    // Get the number of entries in the header
    val headerSize =
      if (hasHeader) Option(lines.getOrElse(0, "").split(separator).length) else None

    for (position <- start until lines.size) {
      val lineEntries = lines(position).split(separator)
      val fields      =
        // If the element size in the header is bigger than the element size in the line,
        // the line will be filled up with an empty string. Therefore, we do not loose
        // columns that have empty entries, especially at the end of a CSV file.
        if (headerSize.isDefined && headerSize.getOrElse(0) > lineEntries.size) {
          val lb = new ListBuffer[String]
          lb.appendAll(lineEntries)
          for (_ <- lineEntries.size until headerSize.get) {
            lb.append("")
          }
          lb.toArray[String]
        } else
          lineEntries

      for (fieldPosition <- 0 until fields.size) {
        val field = fields(fieldPosition).replaceAll("^[\"']", "").replaceAll("[\"']$", "")
        if (buffer.contains(fieldPosition))
          buffer.put(fieldPosition, buffer(fieldPosition) += field)
        else {
          val lbuffer = new ListBuffer[String]()
          lbuffer += field
          buffer.put(fieldPosition, lbuffer)
        }
      }
    }
    buffer
  }

  /**
    * Create a complete sequence with the specific columns of the CSV file.
    *
    * @param document           The XML document object.
    * @param columns            The single columns if the CSV file.
    * @param headerLine         The entries of the header line.
    * @param separator          The separator of the entries of a line.
    * @param extractorMetaData  Additional information for the extraction process.
    * @return An XML element.
    */
  def createSequence(document: Document,
                     columns: scala.collection.mutable.HashMap[Int, ListBuffer[String]],
                     headerLine: Option[List[String]],
                     separator: String,
                     extractorMetaData: ExtractorMetaData): Option[Element] = {
    val sequence = document.createElement(ElementNames.SEQUENCE)
    sequence.setAttribute("id", "lines")
    val row = document.createElement(ElementNames.ELEMENT)
    row.setAttribute("id", "row")
    sequence.appendChild(row)

    for (rowPosition <- 0 until columns.size) {
      val header: Option[String] =
        if (headerLine.isDefined && headerLine.get.size > rowPosition)
          Option(headerLine.get(rowPosition).replaceAll("^[\"']", "").replaceAll("[\"']$", ""))
        else
          None
      val isLast =
        rowPosition == columns.size - 1
      val element = createRow(rowPosition.toLong,
                              document,
                              columns.get(rowPosition),
                              header,
                              separator,
                              isLast,
                              extractorMetaData)
      if (element.isDefined)
        row.appendChild(element.get)
    }

    Option(sequence)
  }

  /**
    * Create a single entry of the DFASDL with the correct data tyoe.
    *
    * @param rowPosition        The column position in the CSV file.
    * @param document           The XML document object.
    * @param entries            The entries of the column that will be used to determine the data type.
    * @param header             The name of the column if available.
    * @param separator          The separator of the entries of the lines.
    * @param lastRow            Whether this is the last row of the CSV file.
    * @param extractorMetaData  Additional information for the extraction process.
    * @return An XML element.
    */
  def createRow(rowPosition: Long,
                document: Document,
                entries: Option[ListBuffer[String]],
                header: Option[String],
                separator: String,
                lastRow: Boolean,
                extractorMetaData: ExtractorMetaData): Option[Element] = {
    if (entries.isDefined) {
      val isLong: Boolean = entries.get.forall(entry => {
        if (entry.length > 1)
          !entry.startsWith("0") && parseLong(entry)
        else
          parseLong(entry)
      })
      val isFormattedNumeric: Boolean = entries.get.forall(entry => {
        if (entry.length > 1)
          // formatnum werden nur Werte, die zwar mit 0 beginnen, aber dann auch mindestens ein Komma
          // oder einen Punkt enthalten. Wir schliessen Werte aus die Ganzzahlen mit einer 0 am
          // Anfang sind, da diese die 0 verlieren würden.
          !(entry.startsWith("0") && !entry.contains(DECIMAL_SEPARATOR_POINT) && !entry.contains(
            DECIMAL_SEPARATOR_COMMA
          ) && !entry.contains(DECIMAL_SEPARATOR_UNICODE)) && parseDouble(entry)
        else
          parseDouble(entry)
      })

      // Check all entries and extract the possible separators
      // transform into a set and get the number
      val numberOfSeparators = entries.get
        .map(entry => {
          if (entry.contains(DECIMAL_SEPARATOR_POINT)) DECIMAL_SEPARATOR_POINT
          else if (entry.contains(DECIMAL_SEPARATOR_COMMA)) DECIMAL_SEPARATOR_COMMA
          else if (entry.contains(DECIMAL_SEPARATOR_UNICODE)) DECIMAL_SEPARATOR_UNICODE
          else ""
        })
        .toSet
        .size

      val element =
        if (isLong) {
          val dateFormat =
            determineSpecificFormat(entries.get, extractorMetaData.formatsFormattime.get.date)
          if (dateFormat.isDefined) {
            val e = document.createElement(ElementNames.FORMATTED_TIME)
            e.setAttribute(AttributeNames.FORMAT, dateFormat.get)
            e
          } else
            document.createElement(ElementNames.NUMBER)
        }
        // If we have more than 1 possible decimal separator, the parsing of the
        // entries can`t be correct -> Only one `decimal-separator` can be defined
        // in the DFASDL.
        else if (isFormattedNumeric && numberOfSeparators < 2) {
          val elem = document.createElement(ElementNames.FORMATTED_NUMBER)
          elem.setAttribute(AttributeNames.FORMAT, createGeneralFormatnumRegex)
          val decimalSeparator = {
            val decSep = determineSeparator(entries.get.toList)
            if (decSep.isDefined)
              decSep.get
            else
              DECIMAL_SEPARATOR_POINT
          }
          val precisionLength = determinePrecisionLength(entries.get.toList, decimalSeparator)

          elem.setAttribute(AttributeNames.DECIMAL_SEPARATOR, decimalSeparator)
          if (precisionLength.isDefined) {
            elem.setAttribute(AttributeNames.MAX_PRECISION, precisionLength.get.toString)
            elem.setAttribute(AttributeNames.MAX_DIGITS, (38 - precisionLength.get).toString)
          } else
            elem.setAttribute(AttributeNames.MAX_DIGITS, "38")
          elem
        } else {
          val isDate: Boolean      = entries.get.forall(entry => parseDate(entry))
          val isTime: Boolean      = entries.get.forall(entry => parseTime(entry))
          val isTimestamp: Boolean = entries.get.forall(entry => parseTimestamp(entry))

          if (isDate) {
            document.createElement(ElementNames.DATE)
          } else if (isTime) {
            document.createElement(ElementNames.TIME)
          } else if (isTimestamp) {
            document.createElement(ElementNames.DATETIME)
          } else {
            // Check the Strings for possibilities of the `formattime` element
            val timestampFormat = determineSpecificFormat(
              entries.get,
              extractorMetaData.formatsFormattime.get.timestamp
            )
            if (timestampFormat.isDefined) {
              val e = document.createElement(ElementNames.FORMATTED_TIME)
              e.setAttribute(AttributeNames.FORMAT, timestampFormat.getOrElse(""))
              e
            } else {
              val dateFormat =
                determineSpecificFormat(entries.get, extractorMetaData.formatsFormattime.get.date)
              if (dateFormat.isDefined) {
                val e = document.createElement(ElementNames.FORMATTED_TIME)
                e.setAttribute(AttributeNames.FORMAT, dateFormat.get)
                e
              } else {
                val timeFormat = determineSpecificFormat(
                  entries.get,
                  extractorMetaData.formatsFormattime.get.time
                )
                if (timeFormat.isDefined) {
                  val e = document.createElement(ElementNames.FORMATTED_TIME)
                  e.setAttribute(AttributeNames.FORMAT, timeFormat.get)
                  e
                } else
                  document.createElement(ElementNames.STRING)
              }
            }
          }

        }
      if (!lastRow)
        element.setAttribute(AttributeNames.STOP_SIGN, separator)

      if (header.isDefined)
        element.setAttribute("id", header.get.toLowerCase(Locale.ROOT))
      else
        element.setAttribute("id", s"entry-$rowPosition")

      Option(element)
    } else
      None
  }
}
