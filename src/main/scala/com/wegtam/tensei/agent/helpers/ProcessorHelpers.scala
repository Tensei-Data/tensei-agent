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

import javax.xml.xpath.{ XPath, XPathFactory }

import akka.util.ByteString
import org.dfasdl.utils._
import org.w3c.dom.Element
import org.w3c.dom.traversal.{ DocumentTraversal, NodeFilter }

/**
  * A trait that contains helpers for the `Processor` and mixes in several other needed traits.
  */
trait ProcessorHelpers
    extends XmlHelpers
    with DocumentHelpers
    with DataElementExtractors
    with DataElementProcessors {
  lazy val xpath
    : XPath = XPathFactory.newInstance().newXPath() // We initialize this globally because it will be needed often.

  /**
    * The return value for the `fetchDataAndKeyFieldValue` function.
    *
    * @param dataElement   An option to the possibly fetched data element.
    * @param keyFieldValue An option to a possibly existing key field value.
    * @param elementsLeft  The number of elements left in the container e.g. the number of elements that match the queried one and are located "after" the returned one.
    */
  case class FetchDataResponse(dataElement: Option[Element],
                               keyFieldValue: Option[String],
                               elementsLeft: Long)

  /**
    * Creates a vector of elements from the given container element.
    * This method only makes sense if called with a sequence element.
    *
    * @param containerElement The container element which should be a sequence.
    * @return A vector containing the container element and it's children.
    */
  def convertContainerElementToVector(containerElement: Element): Vector[Element] = {
    val doc       = containerElement.getOwnerDocument
    val traversal = doc.asInstanceOf[DocumentTraversal]
    val iterator =
      traversal.createNodeIterator(containerElement, NodeFilter.SHOW_ELEMENT, null, true)
    val elements    = Vector.newBuilder[Element]
    var currentNode = iterator.nextNode()
    elements += currentNode
      .asInstanceOf[Element] // We need to buffer the container node as first element.
    currentNode = iterator.nextNode()
    while (currentNode != null) {
      if (isDataElement(currentNode.getNodeName))
        elements += currentNode.asInstanceOf[Element]
      currentNode = iterator.nextNode()
    }
    elements.result()
  }

  /**
    * Creates a map of vectors from the given container element.
    * This method only makes sense if called with a sequence element.
    *
    * @param containerElement The container element which should be a sequence.
    * @return A map containing several vectors e.g. the "data columns" mapped to the id of the data element. The first element in each vector is always the container element.
    */
  def convertContainerElementToVectorMap(containerElement: Element): Map[String, Vector[Element]] = {
    // FIXME We should use the vector builders throughout the process to avoid copying of immutable resources.
    val mappedColumns = scala.collection.mutable.Map.empty[String, Vector[Element]]

    val doc       = containerElement.getOwnerDocument
    val traversal = doc.asInstanceOf[DocumentTraversal]
    val iterator =
      traversal.createNodeIterator(containerElement, NodeFilter.SHOW_ELEMENT, null, true)
    var currentNode = iterator.nextNode()
    while (currentNode != null) {
      if (isDataElement(currentNode.getNodeName)) {
        val e = currentNode.asInstanceOf[Element]
        val id =
          if (e.hasAttribute("id"))
            e.getAttribute("id")
          else {
            e.getAttribute("class")
              .split("\\s")
              .find(_.startsWith("id:"))
              .getOrElse("id:UNDEFINED")
              .substring(3)
          }
        if (mappedColumns.contains(id)) {
          val elements = mappedColumns(id) :+ e
          mappedColumns += (id -> elements)
        } else {
          val elements = Vector.newBuilder[Element]
          elements += containerElement // We need to buffer the container node as first element.
          elements += e
          mappedColumns += (id -> elements.result())
        }
      }
      currentNode = iterator.nextNode()
    }

    mappedColumns.toMap
  }

  /**
    * Return the default value of the given data element.
    *
    * @param element The element.
    * @return An option to a default value of the type `String` if it is supported and defined.
    */
  def getDefaultValue(element: Element): Option[String] =
    getDataElementType(element.getNodeName) match {
      case DataElementType.BinaryDataElement =>
        // Currently we do not support default values for binary data elements.
        None
      case DataElementType.StringDataElement =>
        if (element.hasAttribute(AttributeNames.DEFAULT_NUMBER))
          Option(element.getAttribute(AttributeNames.DEFAULT_NUMBER))
        else if (element.hasAttribute(AttributeNames.DEFAULT_STRING))
          Option(element.getAttribute(AttributeNames.DEFAULT_STRING))
        else
          None
      case DataElementType.UnknownElement =>
        // TODO Maybe we should produce an exception here?
        None
    }

  /**
    * Process the given source data and bring it into the proper target format.
    * This function assumes that the passed in data has already the proper type.
    *
    * @todo Currently we only process string data element types!
    *
    * @param data    The actual data.
    * @param target  The target data description.
    * @return The processed data.
    */
  def processTargetData(data: Any, target: Element): Any =
    if (getDataElementType(target.getNodeName) == DataElementType.StringDataElement && data != None) {
      val dataString: String = data match {
        case bd: java.math.BigDecimal => bd.toPlainString
        case bs: ByteString           => bs.utf8String
        case _                        => data.toString
      }
      target.getNodeName match {
        case ElementNames.FORMATTED_STRING | ElementNames.STRING =>
          val ps = processStringData(dataString, target)
          ByteString(ps)
        case ElementNames.FORMATTED_NUMBER =>
          extractData(processFormattedNumberData(dataString, target), target)
        case ElementNames.NUMBER =>
          val processedNumber =
            data match {
              case decimal: java.math.BigDecimal =>
                val longValue: String =
                  if (target.hasAttribute(AttributeNames.PRECISION)) {
                    val desiredPrecision = target.getAttribute(AttributeNames.PRECISION).toInt
                    if (desiredPrecision < decimal.precision()) {
                      val s        = decimal.toString
                      val fraction = s.substring(s.indexOf(".") + 1)
                      val newFraction =
                        if (fraction.length > desiredPrecision)
                          fraction.substring(0, desiredPrecision)
                        else
                          fraction
                      val rawString = s"${s.substring(0, s.indexOf("."))}$newFraction"
                      rawString
                    } else
                      decimal.toString.replace(".", "")
                  } else
                    decimal.toString.replace(".", "")
                processNumberData(longValue, target)
              case bs: ByteString =>
                processNumberData(bs.utf8String, target)
              case long: Long =>
                processNumberData(long.toString, target)
              case string: String =>
                processNumberData(string, target)
            }
          if (processedNumber.isEmpty)
            processedNumber
          else
            extractData(processedNumber, target)
        case _ =>
          data
      }
    } else
      data
}
