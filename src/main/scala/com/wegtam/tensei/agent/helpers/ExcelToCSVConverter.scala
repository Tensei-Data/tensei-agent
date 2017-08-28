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

import java.io._
import java.net.URI
import java.nio.file.Files
import java.sql.Time
import java.time.{ LocalDate, LocalTime }
import java.util
import java.util.{ Date, Locale }

import akka.actor.{ Actor, ActorLogging, Props }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.wegtam.tensei.adt.ConnectionInformation
import com.wegtam.tensei.agent.exceptions.AccessValidationException
import com.wegtam.tensei.agent.helpers.ExcelToCSVConverter.ExcelConverterMessages.{
  Convert,
  ConvertResult,
  Stop
}
import org.apache.poi.ss.usermodel._

import scala.util.{ Failure, Success, Try }

/**
  * This actor converts a given Excel file into a corresponding CSV file.
  *
  * Code based on:
  * http://svn.apache.org/repos/asf/poi/trunk/src/examples/src/org/apache/poi/ss/examples/ToCSV.java
  *
  * @param source              The source connection information of the file.
  * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
  */
class ExcelToCSVConverter(source: ConnectionInformation, agentRunIdentifier: Option[String])
    extends Actor
    with ActorLogging {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  // The separator for the single rows that is used when converting the
  // content from Excel to CSV
  val separator: String =
    context.system.settings.config.getString("tensei.agents.parser.excel-row-separator")

  /*
   * EXCEL_STYLE_ESCAPING (0)
   * ========================
   * Identifies that the CSV file should obey Excel's formatting conventions
   * with regard to escaping certain embedded characters - the field separator,
   * speech mark and end of line (EOL) character
   *
   * UNIX_STYLE_ESCAPING  (1)
   * ========================
   * Identifies that the CSV file should obey UNIX formatting conventions
   * with regard to escaping certain embedded characters - the field separator
   * and end of line (EOL) character
   *
   **/
  val usedStyleEscaping = 0

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.clearMDC()
    super.postStop()
  }

  override def receive: Receive = {
    case Convert =>
      log.info(s"Received `Convert` message for file {}", source.uri.getSchemeSpecificPart)

      // Determine the locale for the source that is delivered with the connection information
      val locale = source.languageTag.fold(Locale.ROOT)(l => Locale.forLanguageTag(l))

      val updatedSource =
        ExcelToCSVConverter.processConversion(source, locale, usedStyleEscaping, separator)

      updatedSource match {
        case Failure(e) =>
          // FIXME -> Return correct error
          sender() ! e
        case Success(newSource) =>
          sender() ! ConvertResult(newSource)
      }
      context stop self
    case Stop =>
      log.info("Received `Stop` message")
      context stop self
  }

}

object ExcelToCSVConverter {

  val EXCEL_STYLE_ESCAPING = 0
  val UNIX_STYLE_ESCAPING  = 1

  /**
    * Helper method to create an ExcelToCSVConverter.
    *
    * @param source              The source connection information for the file.
    * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
    *
    * @return The props to create the actor.
    */
  def props(source: ConnectionInformation, agentRunIdentifier: Option[String]): Props =
    Props(classOf[ExcelToCSVConverter], source, agentRunIdentifier)

  sealed trait ExcelConverterMessages

  object ExcelConverterMessages {

    /**
      * Start the conversion of the Excel file into the temporary CSV file.
      */
    final case object Convert extends ExcelConverterMessages

    /**
      * Return the modified connection information with the updated file path for
      * the created CSV file.
      *
      * @param source The updated connection information.
      */
    final case class ConvertResult(source: ConnectionInformation) extends ExcelConverterMessages

    /**
      * Stop the actor
      */
    final case object Stop extends ExcelConverterMessages

  }

  final case class ConvertedData(csvData: util.ArrayList[util.ArrayList[String]], maxRowWidth: Int)

  def processConversion(source: ConnectionInformation,
                        locale: Locale,
                        usedStyleEscaping: Int,
                        separator: String): Try[ConnectionInformation] =
    for {
      file <- ExcelToCSVConverter.openFile(source.uri)
      data <- ExcelToCSVConverter.createCSVData(file, locale)
      newUri <- ExcelToCSVConverter.saveExcelFile(file,
                                                  data.csvData,
                                                  data.maxRowWidth,
                                                  usedStyleEscaping,
                                                  separator)
    } yield {
      source.copy(uri = newUri)
    }

  /**
    * Try to open the file defined in the source connection information and validate
    * whether the file exists and is readable.
    *
    * @param uri  The source uri of the file.
    * @return
    */
  def openFile(uri: URI): Try[File] =
    Try {
      val extension = uri.getSchemeSpecificPart.split("\\.(?=[^\\.]+$)")
      if (extension.length != 2)
        Failure(
          new AccessValidationException(
            s"File ${uri.getSchemeSpecificPart} does not end on valid extension."
          )
        )
      else if (!extension(1).equalsIgnoreCase("xls") && !extension(1).equalsIgnoreCase("xlsx"))
        Failure(
          new AccessValidationException(
            s"File ${uri.getSchemeSpecificPart} does not end on `xls` or `xlsx`."
          )
        )
      else {
        val f: File = new File(uri.getSchemeSpecificPart)
        if (!f.exists())
          Failure(
            new FileNotFoundException(s"File on path ${uri.getSchemeSpecificPart} not exists.")
          )
        else if (!f.canRead)
          Failure(
            new IllegalAccessError(s"File on path ${uri.getSchemeSpecificPart} not accessible.")
          )
        else Success(f)
      }
    } match {
      case Success(file)  => file
      case Failure(error) => Failure(error)
      case _ =>
        Failure(
          new AccessValidationException(
            s"Error during access to file ${uri.getSchemeSpecificPart}"
          )
        )
    }

  /**
    * Convert the source data from the Excel file into a CSV file.
    *
    * @param file The source file that should be converted
    *
    * @return The converted data type
    */
  def createCSVData(file: File, local: Locale): Try[ConvertedData] = {
    // workbook relevant variables
    val fis                         = Files.newInputStream(file.toPath)
    val workbook                    = WorkbookFactory.create(fis)
    val evaluator: FormulaEvaluator = workbook.getCreationHelper.createFormulaEvaluator()
    val formatter: DataFormatter    = new DataFormatter(local, true)

    // buffer variables
    val csvData     = new util.ArrayList[util.ArrayList[String]]()
    var maxRowWidth = 0

    // FIXME: Currently, we only process the first sheet of an Excel file
    //val numSheets = workbook.getNumberOfSheets
    for (i <- 0 until 1) {
      val sheet = workbook.getSheetAt(i)
      if (sheet.getPhysicalNumberOfRows > 0) {
        for (r <- 0 to sheet.getLastRowNum) {
          val result = rowToCSV(sheet.getRow(r), formatter, evaluator)
          csvData.add(result._1)
          if (maxRowWidth < result._2)
            maxRowWidth = result._2
        }
      }
    }
    Success(ConvertedData(csvData, maxRowWidth))
  }

  /**
    * Store the content of the CSV target file.
    *
    * @param file                   The source file.
    * @param csvData                The data that was extracted from the source file.
    * @param maxRowWidth            The maximum number of rows of all.
    * @param formattingConvention   Formatting convention for the source file.
    * @param separator              The separator of the columns in the target file.
    * @return The updated URI for the newly created target file.
    */
  def saveExcelFile(file: File,
                    csvData: util.ArrayList[util.ArrayList[String]],
                    maxRowWidth: Int,
                    formattingConvention: Int,
                    separator: String): Try[URI] = {
    val destinationFileURI           = File.createTempFile(s"${file.getName}-", ".csv").toURI
    val fw: FileWriter               = new FileWriter(destinationFileURI.getSchemeSpecificPart)
    val bw: BufferedWriter           = new BufferedWriter(fw)
    var line: util.ArrayList[String] = new util.ArrayList[String]()
    var buffer: StringBuffer         = null
    var csvLineElement: String       = null

    val s = csvData.size
    for (i <- 0 until s) {
      buffer = new StringBuffer()
      line = csvData.get(i)
      for (j <- 0 until maxRowWidth) {
        if (line.size() > j) {
          csvLineElement = line.get(j)
          if (csvLineElement != null)
            buffer.append(
              escapeEmbeddedCharacters(csvLineElement, formattingConvention, separator)
            )
        }
        if (j < (maxRowWidth - 1))
          buffer.append(separator)
      }

      // Once the line is built, write it away to the CSV file.
      bw.write(buffer.toString.trim())

      // Condition the inclusion of new line characters so as to
      // avoid an additional, superfluous, new line at the end of
      // the file.
      if (i < (csvData.size() - 1)) {
        bw.newLine()
      }
    }
    bw.flush()
    bw.close()
    Success(destinationFileURI)
  }

  def rowToCSV(row: Row,
               formatter: DataFormatter,
               evaluator: FormulaEvaluator): (util.ArrayList[String], Int) = {
    var lastCellNum = 0
    var maxRowWidth = 0
    val csvLine     = new util.ArrayList[String]()

    // Check to ensure that a row was recovered from the sheet as it is
    // possible that one or more rows between other populated rows could be
    // missing - blank. If the row does contain cells then...
    if (row != null) {

      // Get the index for the right most cell on the row and then
      // step along the row from left to right recovering the contents
      // of each cell, converting that into a formatted String and
      // then storing the String into the csvLine ArrayList.
      lastCellNum = row.getLastCellNum.toInt
      for (i <- 0 to lastCellNum) {
        val cell = row.getCell(i)
        if (cell == null) {
          csvLine.add("")
        } else {
          if (Try(cell.getCellFormula.nonEmpty).toOption.getOrElse(false))
            csvLine.add(formatter.formatCellValue(cell, evaluator))
          else {
            val isDate: Option[Date] = Try {
              if (DateUtil.isCellDateFormatted(cell))
                cell.getDateCellValue match {
                  case null =>
                    throw new IllegalArgumentException("Null returned by getDateCellValue!")
                  case d => d
                } else
                throw new IllegalArgumentException("Cell not date formatted!")
            }.toOption
            val isSqlDate
              : Option[java.sql.Date]          = Try(java.sql.Date.valueOf(cell.toString)).toOption
            val isLocalDate: Option[LocalDate] = Try(LocalDate.parse(cell.toString)).toOption
            val isSqlTime
              : Option[Time] = Try(Time.valueOf(formatter.formatCellValue(cell))).toOption
            val isLocalTime: Option[LocalTime] = Try(
              LocalTime.parse(formatter.formatCellValue(cell))
            ).toOption
            // If we have a `Date`, we must write the string raw to the target file. Otherwise, the
            // Excel date format can not be evaluated with a DFASDL.
            // We must check the cell content for LocalTime or SqlTime. Time values are as raw like a Date,
            // therefore we must distinguish the time values and they MUST be added to the target file
            // with the `formatter`.
            // Date
            if ((isDate.isDefined || isSqlDate.isDefined || isLocalDate.isDefined) && isSqlTime.isEmpty && isLocalTime.isEmpty) {
              val date: java.sql.Date =
                isDate.fold {
                  isSqlDate.fold {
                    java.sql.Date.valueOf(LocalDate.parse(cell.toString))
                  } { _ =>
                    java.sql.Date.valueOf(cell.toString)
                  }
                } { _ =>
                  new java.sql.Date(cell.getDateCellValue.getTime)
                }
              csvLine.add(date.toString)
            }
            // Time
            else if (isSqlTime.isDefined || isLocalTime.isDefined) {
              val time: Time =
                isSqlTime.getOrElse(Time.valueOf(LocalTime.parse(formatter.formatCellValue(cell))))
              csvLine.add(time.toString)
            } else
              csvLine.add(formatter.formatCellValue(cell))
          }
        }
      }
      // Make a note of the index number of the right most cell. This value
      // will later be used to ensure that the matrix of data in the CSV file
      // is square.
      if (lastCellNum > maxRowWidth) {
        maxRowWidth = lastCellNum
      }
    }
    (csvLine, maxRowWidth)
  }

  /**
    * Helper method that escaped characters depending on the given formatting convention.
    *
    * @param theField               The field that should be escaped
    * @param formattingConvention   The formatting convention for the field
    * @param separator              The separator for the elements
    * @return The escaped content of the field
    */
  def escapeEmbeddedCharacters(theField: String,
                               formattingConvention: Int,
                               separator: String): String = {
    var buffer: StringBuffer = null
    var field: String        = theField

    // If the fields contents should be formatted to conform with Excel's
    // convention....
    if (formattingConvention == EXCEL_STYLE_ESCAPING) {

      // Firstly, check if there are any speech marks (") in the field;
      // each occurrence must be escaped with another set of spech marks
      // and then the entire field should be enclosed within another
      // set of speech marks. Thus, "Yes" he said would become
      // """Yes"" he said"
      if (field.contains("\"")) {
        buffer = new StringBuffer(field.replaceAll("\"", "\\\"\\\""))
        buffer.insert(0, "\"")
        buffer.append("\"")
      } else {
        // If the field contains either embedded separator or EOL
        // characters, then escape the whole field by surrounding it
        // with speech marks.
        buffer = new StringBuffer(field)
        if (buffer.indexOf(separator) > -1 ||
            buffer.indexOf("\n") > -1) {
          buffer.insert(0, "\"")
          buffer.append("\"")
        }
      }
      buffer.toString.trim()
    }
    // The only other formatting convention this class obeys is the UNIX one
    // where any occurrence of the field separator or EOL character will
    // be escaped by preceding it with a backslash.
    else {
      if (field.contains(separator))
        field = field.replaceAll(separator, "\\\\" + separator)
      if (field.contains("\n"))
        field = field.replaceAll("\n", "\\\\\n")
      field
    }
  }

}
