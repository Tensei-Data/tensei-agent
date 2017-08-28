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
import argonaut.Argonaut._
import argonaut._
import com.wegtam.tensei.adt.{ ConnectionInformation, DFASDL }
import com.wegtam.tensei.agent.SchemaExtractor.ExtractorMetaData
import org.dfasdl.utils.{ AttributeNames, DocumentHelpers, ElementNames }
import org.w3c.dom.{ Document, Element }

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource
import scalaz.Scalaz._
import scalaz._

/**
  * The JSONSchemaExtractor creates the DFASDL from the parsed JSON string.
  */
trait JSONSchemaExtractor
    extends DocumentHelpers
    with XmlHelpers
    with ExtractorHelpers
    with NetworkFileHelpers {

  val usedIdNames: ListBuffer[String] = new ListBuffer[String]

  /**
    * Entry point for local files.
    *
    * @param con The connection information for the JSON file.
    * @return The created DFASDL or None.
    */
  def readJSONFile(con: ConnectionInformation): ValidationNel[String, java.io.File] =
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
  def getNetworkJSONStream(
      con: ConnectionInformation,
      context: ActorContext
  ): ValidationNel[String, (Option[BufferedInputStream], Option[Any])] =
    try {
      val createdStream = createStream(con, context)

      if (createdStream._1.isDefined && createdStream._2.isDefined)
        createdStream.successNel
      else
        s"Can't access the network json file: ${con.uri.getSchemeSpecificPart}".failNel
    } catch {
      case e: Throwable =>
        GenericHelpers.createValidationFromException[(Option[BufferedInputStream], Option[Any])](e)
    }

  /**
    * Start the extraction of the DFASDL from a local JSON file.
    *
    * @param file                 The file object to the JSON file.
    * @param extractorMetaData    Additional information for the extraction process.
    * @return An option to the generated DFASDL.
    */
  def extractFromLocalJSON(file: File, extractorMetaData: ExtractorMetaData): Option[DFASDL] = {
    val bufferedSource = scala.io.Source.fromFile(file)
    traverseJSON(bufferedSource, extractorMetaData)
  }

  /**
    * Entry point for network files.
    *
    * @param bufferedInput        The buffered input stream of the network file.
    * @param extractorMetaData    Additional information for the extraction process.
    * @return The created DFASDL.
    */
  def extractFromNetworkJSONFile(bufferedInput: BufferedInputStream,
                                 extractorMetaData: ExtractorMetaData): Option[DFASDL] = {
    val bufferedSource = new BufferedSource(bufferedInput)
    traverseJSON(bufferedSource, extractorMetaData)
  }

  /**
    * Start the traversal of the JSON and create the DFASDL skeleton.
    *
    * @param bufferedJSON         A buffered source of the JSON file content.
    * @param extractorMetaData    Additional information for the extraction process.
    * @return An option to a DFASDL.
    */
  def traverseJSON(
      bufferedJSON: BufferedSource,
      extractorMetaData: ExtractorMetaData
  ): Option[DFASDL] = {
    val jsonString = Parse.parseOption(bufferedJSON.mkString)
    if (jsonString.isDefined) {
      val json   = jsonString.get
      val cursor = json.cursor

      // The document skeleton
      val loader   = createDocumentBuilder()
      val document = loader.newDocument()
      val dfasdl   = document.createElement(ElementNames.ROOT)
      dfasdl.setAttribute("xmlns", "http://www.dfasdl.org/DFASDL")
      dfasdl.setAttribute(AttributeNames.SEMANTIC_SCHEMA, "custom")
      val jsonRoot = document.createElement(ElementNames.ELEMENT)

      val dfasdlStructure = traverseRec(Option(cursor), document, jsonRoot, extractorMetaData)

      jsonRoot.setAttribute("id", "json-root")
      dfasdl.appendChild(dfasdlStructure)

      document.appendChild(dfasdl)

      Option(new DFASDL(s"${extractorMetaData.dfasdlNamePart}-json", prettifyXml(document), "1"))
    } else
      None
  }

  /**
    * Recursive traversal through the JSON and creation of the DFASDL.
    *
    * @param cursor             An Argonaut cursor on the JSON.
    * @param document           The document of the DFASDL.
    * @param parentElement      The parent element of the current DFASDL element.
    * @param extractorMetaData  Additional information for the extraction process.
    * @return An XML Element for the DFASDL.
    */
  def traverseRec(cursor: Option[Cursor],
                  document: Document,
                  parentElement: Element,
                  extractorMetaData: ExtractorMetaData): Element =
    if (cursor.isDefined) {
      val currentCursor = cursor.get
      val currentFields = currentCursor.fields
      // traverse the fields
      if (currentFields.isDefined && currentFields.get.nonEmpty) {
        @tailrec
        def loop(pos: Int): Boolean =
          if (pos >= currentFields.get.length || cursor.isEmpty) false
          else {
            val currentFieldJson = currentFields.get(pos)
            val currentField     = currentFieldJson.jencode
            val newElement = createElem(document,
                                        parentElement,
                                        currentField.string.getOrElse(""),
                                        extractorMetaData,
                                        cursor.get.downField(currentFieldJson))
            if (newElement.isDefined) {
              parentElement.appendChild(newElement.get)
              traverseRec(cursor.get.downField(currentFieldJson),
                          document,
                          newElement.get,
                          extractorMetaData)
            }
            loop(pos + 1)
          }
        loop(0)
      }
      // no fields, but could be an array
      else if (cursor.get.focus.isArray) {
        // The array contains an Object
        if (cursor.get.downN(0).isDefined && cursor.get.downN(0).get.focus.isObject) {
          val newElementSeq = createSeq(document, parentElement, "seq")
          parentElement.appendChild(newElementSeq)
          traverseRec(cursor.get.downArray,
                      document,
                      newElementSeq.getFirstChild.asInstanceOf[Element],
                      extractorMetaData)
        } else {
          parentElement.appendChild(
            createArray(document, parentElement, cursor, extractorMetaData)
          )
        }
      } else {
        // here are the values
      }
      parentElement
    } else
      parentElement

  /**
    * Determine all important attributes and the element name of the current element.
    *
    * @param cursor                The current cursor of the JSON.
    * @param id                    The determined ID for the current element.
    * @param parentElement         The parentElement for this new element.
    * @param extractorMetaData     Additional information for the extraction process.
    * @param setJsonAttributeName  Whether the `JSON_ATTRIBUTE_NAME` must be set to the element.
    * @return An option to a tuple that contains the element name and a list of created attributes.
    */
  def determineElementParameter(
      cursor: Option[Cursor],
      id: String,
      parentElement: Element,
      extractorMetaData: ExtractorMetaData,
      setJsonAttributeName: Boolean = true
  ): Option[(String, List[(String, String)])] =
    if (cursor.isDefined) {
      val valueObject      = cursor.get.focus
      val valueObjectClean = valueObject.toString().replaceAll("\"", "")

      val elementName =
        if (valueObject.isNumber) {
          if (valueObjectClean.length > 1 && valueObjectClean.startsWith("0")) {
            ElementNames.STRING
          } else if (parseLong(valueObject.toString()))
            ElementNames.NUMBER
          else if (parseDouble(valueObject.toString()))
            ElementNames.FORMATTED_NUMBER
          else
            ElementNames.STRING
        } else if (valueObject.isString) {
          if (parseLong(valueObjectClean))
            if (valueObjectClean.length > 1 && valueObjectClean.startsWith("0"))
              ElementNames.STRING
            else
              ElementNames.NUMBER
          else if (parseDouble(valueObjectClean))
            if (valueObjectClean.length > 1 && valueObjectClean.startsWith("0"))
              ElementNames.STRING
            else
              ElementNames.FORMATTED_NUMBER
          else if (parseDate(valueObjectClean))
            ElementNames.DATE
          else if (parseTime(valueObjectClean))
            ElementNames.TIME
          else if (parseTimestamp(valueObjectClean))
            ElementNames.DATETIME
          else
            ElementNames.STRING
        } else
          ElementNames.ELEMENT

      val parameters: ListBuffer[(String, String)] = new ListBuffer[(String, String)]

      val finalElementName =
        elementName match {
          case ElementNames.STRING | ElementNames.NUMBER =>
            val isFT = isFormattedTime(valueObjectClean, extractorMetaData)
            if (isFT.isDefined) {
              parameters.append((AttributeNames.FORMAT, isFT.getOrElse("")))
              ElementNames.FORMATTED_TIME
            } else
              elementName
          case _ =>
            elementName
        }

      finalElementName match {
        case ElementNames.FORMATTED_NUMBER =>
          parameters.append((AttributeNames.FORMAT, createGeneralFormatnumRegex))
          parameters.append((AttributeNames.DECIMAL_SEPARATOR, DECIMAL_SEPARATOR_POINT))
          val precisionLength =
            determinePrecisionLength(List(valueObjectClean), DECIMAL_SEPARATOR_POINT)
          if (precisionLength.isDefined) {
            parameters.append((AttributeNames.MAX_PRECISION, precisionLength.get.toString))
            parameters.append((AttributeNames.MAX_DIGITS, (38 - precisionLength.get).toString))
          } else
            parameters.append((AttributeNames.MAX_DIGITS, "38"))
        case _ =>
      }

      if (parentElement != null && parentElement.getAttribute("id").nonEmpty)
        if (id.nonEmpty)
          parameters.append(
            ("id",
             parentElement
               .getAttribute("id") + "-" + cleanElementId(id.toLowerCase(Locale.ROOT).trim))
          )
        else
          parameters.append(("id", parentElement.getAttribute("id")))
      else if (id.nonEmpty)
        parameters.append(("id", cleanElementId(id.toLowerCase(Locale.ROOT).trim)))

      if (id.nonEmpty && setJsonAttributeName)
        parameters.append((AttributeNames.JSON_ATTRIBUTE_NAME, cleanElementId(id.trim)))

      Option((finalElementName, parameters.toList))
    } else
      None

  /**
    * Check a value for the specified `formattime` formats
    *
    * @param value              The value that should be checked.
    * @param extractorMetaData  Additional information for the extraction process.
    * @return The format for the `formattime` element or `None`
    */
  def isFormattedTime(value: String, extractorMetaData: ExtractorMetaData): Option[String] = {
    val entries = ListBuffer(value)
    val timestampFormat =
      determineSpecificFormat(entries = entries, extractorMetaData.formatsFormattime.get.timestamp)
    if (timestampFormat.isDefined) {
      timestampFormat
    } else {
      val dateFormat =
        determineSpecificFormat(entries, extractorMetaData.formatsFormattime.get.date)
      if (dateFormat.isDefined) {
        dateFormat
      } else {
        val timeFormat =
          determineSpecificFormat(entries, extractorMetaData.formatsFormattime.get.time)
        if (timeFormat.isDefined) {
          timeFormat
        } else
          None
      }
    }
  }

  /**
    * Create the DFASDL syntax for the element.
    *
    * @param document            The current Document for the DFASDL.
    * @param parentElement       The parent element of the current element.
    * @param extractorMetaData   Additional information for the extraction process.
    * @param id                  The determined ID of the current element.
    * @param childCursor         The current cursor that is positioned on the child element of the current element.
    * @param setJsonAttribueName Whether the `JSON_ATTRIBUTE_NAME` must be set to the element.
    * @return An option to a created DFASDL element.
    */
  def createElem(document: Document,
                 parentElement: Element,
                 id: String,
                 extractorMetaData: ExtractorMetaData,
                 childCursor: Option[Cursor],
                 setJsonAttribueName: Boolean = true): Option[Element] =
    if (childCursor.isDefined) {
      val elementParameters = determineElementParameter(childCursor,
                                                        id,
                                                        parentElement,
                                                        extractorMetaData,
                                                        setJsonAttribueName)
      if (elementParameters.isDefined) {
        val element = document.createElement(elementParameters.get._1)
        elementParameters.get._2
          .foreach(parameter => element.setAttribute(parameter._1, parameter._2))
        Option(element)
      } else
        None
    } else
      None

  /**
    * Create the DFASDL syntax for a sequence.
    *
    * @param document  The current DFASDL document.
    * @param id        The determined ID of the sequence.
    * @return The sequence element for the DFASDL.
    */
  def createSeq(document: Document, parentElement: Element, id: String): Element = {
    val sequence = document.createElement(ElementNames.SEQUENCE)
    if (parentElement != null && parentElement.getAttribute("id").nonEmpty)
      sequence.setAttribute(
        "id",
        parentElement.getAttribute("id") + "-" + id.toLowerCase(Locale.ROOT).trim
      )
    else
      sequence.setAttribute("id", id.toLowerCase(Locale.ROOT).trim)
    sequence.setAttribute(AttributeNames.KEEP_ID, "true")

    val sequenceElement = document.createElement(ElementNames.ELEMENT)
    sequenceElement.setAttribute("id", sequence.getAttribute("id") + "-row")
    sequence.appendChild(sequenceElement)

    sequence
  }

  /**
    * Create the DFASDL syntax for a sequence that represents an array in Json.
    *
    * @param document           The current DFASDL document.
    * @param parentElement      The parent element of the current element.
    * @param cursor             An option to the current cursor.
    * @param extractorMetaData  Additional information for the extraction process.
    * @return The sequence element for the DFASDL.
    */
  def createArray(document: Document,
                  parentElement: Element,
                  cursor: Option[Cursor],
                  extractorMetaData: ExtractorMetaData): Element = {
    val newElementSeq = createSeq(document, parentElement, "seq")
    val newElementSeqElementValue = createElem(
      document,
      newElementSeq.getFirstChild.asInstanceOf[Element],
      "element",
      extractorMetaData,
      cursor.map(_.downArray).getOrElse(None),
      setJsonAttribueName = false
    )
    if (newElementSeqElementValue.isDefined)
      newElementSeq.getFirstChild.appendChild(newElementSeqElementValue.get)
    newElementSeq
  }
}
