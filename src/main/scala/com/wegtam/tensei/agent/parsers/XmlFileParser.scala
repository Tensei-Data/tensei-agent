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

import javax.xml.stream.{ XMLInputFactory, XMLStreamConstants }

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.fasterxml.aalto.{ AsyncByteArrayFeeder, AsyncXMLStreamReader }
import com.fasterxml.aalto.stax.InputFactoryImpl
import com.wegtam.tensei.adt.{ ConnectionInformation, Cookbook }
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.adt.BaseParserResponseStatus.BaseParserResponseStatusType
import com.wegtam.tensei.agent.adt._
import com.wegtam.tensei.agent.helpers.LoggingHelpers
import org.dfasdl.utils.{ AttributeNames, DataElementType, ElementNames }
import org.w3c.dom.traversal.{ DocumentTraversal, NodeFilter, TreeWalker }
import org.w3c.dom.{ Element, Node }

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

import scalaz._, Scalaz._

object XmlFileParser {

  /**
    * Helper method to create an actor for xml file parsing.
    *
    * @param source              The source connection to retrieve the data from.
    * @param cookbook            The cookbook holding the source dfasdl.
    * @param dataTreeRef         The actor ref to the data tree e.g. where to put the parsed data.
    * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
    * @return The props to generate the actor.
    */
  def props(source: ConnectionInformation,
            cookbook: Cookbook,
            dataTreeRef: ActorRef,
            agentRunIdentifier: Option[String]): Props =
    Props(classOf[XmlFileParser], source, cookbook, dataTreeRef, agentRunIdentifier)

}

/**
  * An xml file parser.
  *
  * @param source              The source connection to retrieve the data from.
  * @param cookbook            The cookbook holding the source dfasdl.
  * @param dataTreeRef         The actor ref to the data tree e.g. where to put the parsed data.
  * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
  */
class XmlFileParser(source: ConnectionInformation,
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

  val xmlString = scala.io.Source.fromFile(source.uri.getSchemeSpecificPart).mkString

  val xmlInputFactory = new InputFactoryImpl
  xmlInputFactory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, false)
  val asyncXmlStreamReader = xmlInputFactory.createAsyncForByteArray()
  val wrapper              = new AsyncXmlReaderWrapper(asyncXmlStreamReader, 1, xmlString)

  // We need the DFASDL and a tree walker for some checks later on.
  lazy val dfasdlXml = createNormalizedDocument(
    cookbook.findDFASDL(source.dfasdlRef.get).get.content
  )
  lazy val xmlTreeWalker: TreeWalker = {
    val traversal = dfasdlXml.asInstanceOf[DocumentTraversal]
    traversal.createTreeWalker(dfasdlXml.getDocumentElement, NodeFilter.SHOW_ELEMENT, null, true)
  }

  override def receive: Receive = {
    case BaseParserMessages.SubParserInitialize =>
      sender() ! BaseParserMessages.SubParserInitialized
    case BaseParserMessages.Start =>
      log.debug("Starting XmlFileParser")
      parseXmlFile()
      asyncXmlStreamReader.close()
      sender() ! ParserStatusMessage(ParserStatus.COMPLETED, Option(context.self))
    case BaseParserMessages.Stop =>
      log.debug("Stopping FileParser")
      asyncXmlStreamReader.close()
      context stop self
    case BaseParserMessages.Status =>
      log.error("Status request not yet implemented!")
  }

  def parseXmlFile(): Unit =
    if (source.dfasdlRef.isDefined && cookbook.findDFASDL(source.dfasdlRef.get).isDefined) {
      val xml = createNormalizedDocument(cookbook.findDFASDL(source.dfasdlRef.get).get.content)
      traverseTree(xml, log)
    } else
      log.error("No DFASDL defined for {} in cookbook {}", source.uri, cookbook.id)

  override def save(data: ParserDataContainer,
                    dataHash: Long,
                    referenceId: Option[String] = None): Unit = {
    val sourceSequenceRow =
      if (state.isInSequence)
        Option(state.getCurrentSequenceRowCount)
      else
        None
    // FIXME Currently we parse one element to many using the pull logic. We should switch to push someday.
    if (state.dataStatus == BaseParserResponseStatus.END_OF_SEQUENCE) {
      val currentNode = dfasdlXml.getElementById(data.elementId)
      xmlTreeWalker.setCurrentNode(currentNode)
      if (state.isLastChildOfCurrentStructure(xmlTreeWalker)) {
        // Save the data because we are the last element within a sequence.
        if (referenceId.isDefined)
          dataTreeRef ! DataTreeDocumentMessages.SaveReferenceData(
            data.copy(dataElementHash = Option(dataHash)),
            dataHash,
            referenceId.get,
            sourceSequenceRow
          )
        else
          dataTreeRef ! DataTreeDocumentMessages.SaveData(
            data.copy(dataElementHash = Option(dataHash)),
            dataHash
          )
      }
    } else if (state.dataStatus != BaseParserResponseStatus.END_OF_DATA) {
      if (referenceId.isDefined)
        dataTreeRef ! DataTreeDocumentMessages.SaveReferenceData(
          data.copy(dataElementHash = Option(dataHash)),
          dataHash,
          referenceId.get,
          sourceSequenceRow
        )
      else
        dataTreeRef ! DataTreeDocumentMessages.SaveData(
          data.copy(dataElementHash = Option(dataHash)),
          dataHash
        )
    }
  }

  /**
    * Extract the given attribute from the current element of the xml stream.
    * If the attribute is not found or some other error occurs the left side of the `\/` holds
    * the error message.
    *
    * @param attributeName The name of the desired attribute.
    * @return Either a string containing an error message or an option to the attribute's value.
    */
  private def getAttributeFromCurrentElement(
      attributeName: String,
      myStream: AsyncXMLStreamReader[AsyncByteArrayFeeder]
  ): String \/ Option[String] = {
    val currentElement = myStream.getLocalName
    if (myStream.getAttributeCount > 0) {
      val values =
        for (counter <- 0 until myStream.getAttributeCount) yield {
          if (myStream.getAttributeLocalName(counter) == attributeName)
            Option(myStream.getAttributeValue(counter))
          else
            None
        }
      if (values.isEmpty || !values.exists(_.isDefined))
        s"Attribute $attributeName not found in element $currentElement!".left
      else
        values.filter(_.isDefined).head.right
    } else
      s"No attributes in element $currentElement!".left
  }

  override def readDataElement(structureElement: Element,
                               useOffset: Long,
                               isInChoice: Boolean): BaseParserResponse = {
    val myReaders: (AsyncXmlReaderWrapper, AsyncXMLStreamReader[AsyncByteArrayFeeder]) =
      if (useOffset > -1L && state.isInChoice) {
        // We are within a choice and got an offset therefore we have to setup the reader anew.
        val choiceXmlInputFactory = new InputFactoryImpl
        choiceXmlInputFactory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, false)
        val choiceAsyncXmlStreamReader = choiceXmlInputFactory.createAsyncForByteArray()
        val choiceWrapper              = new AsyncXmlReaderWrapper(choiceAsyncXmlStreamReader, 1, xmlString)
        while (choiceWrapper.getOffset < useOffset.toInt) {
          choiceWrapper.nextToken()
        }
        (choiceWrapper, choiceAsyncXmlStreamReader)
      } else
        (wrapper, asyncXmlStreamReader)

    val myWrapper = myReaders._1
    val myStream  = myReaders._2

    @tailrec
    def getParentElements(e: Element)(ps: List[Element]): List[Element] = {
      val p = e.getParentNode
      if (p == null || p == e.getOwnerDocument.getDocumentElement)
        ps
      else
        getParentElements(p.asInstanceOf[Element])(p.asInstanceOf[Element] :: ps)
    }

    val parentSequence     = getParentSequence(structureElement)
    val parentElements     = getParentElements(structureElement)(List.empty[Element])
    val parentElementNames = parentElements.map(e => getXmlElementName(e))
    val parentElementRootPrevSiblings = parentElements.headOption
      .map { e =>
        val siblings = Vector.newBuilder[Element]
        val n        = xmlTreeWalker.getCurrentNode
        var sibling  = xmlTreeWalker.previousSibling()
        while (sibling != null) {
          siblings += sibling.asInstanceOf[Element]
          sibling = xmlTreeWalker.previousSibling()
        }
        xmlTreeWalker.setCurrentNode(n)
        siblings.result()
      }
      .getOrElse(Vector.empty[Element])
    val isLastSequenceElement =
      if (parentSequence.isDefined) {
        xmlTreeWalker.setCurrentNode(structureElement)
        if (!state.isLastChildOfCurrentStructure(xmlTreeWalker)) {
          xmlTreeWalker.setCurrentNode(structureElement)
          val sibling = xmlTreeWalker.nextSibling()
          sibling.getNodeName == ElementNames.REFERENCE // Our next sibling is a reference.
        } else
          true // We are the last element.
      } else
        false

    val isAttributeData = structureElement.hasAttribute(AttributeNames.XML_ATTRIBUTE_NAME) && structureElement
      .hasAttribute(AttributeNames.XML_ATTRIBUTE_PARENT)
    val matchToken =
      if (isAttributeData)
        structureElement.getAttribute(AttributeNames.XML_ATTRIBUTE_PARENT)
      else
        getXmlElementName(structureElement)

    val dataBuffer                               = new ListBuffer[String]
    var dataStatus: BaseParserResponseStatusType = BaseParserResponseStatus.OK

    var readXml        = true
    var matchedElement = false
    while (readXml) {
      val currentToken = myWrapper.currentToken()
      currentToken match {
        case XMLStreamConstants.START_ELEMENT =>
          val currentElement = myStream.getLocalName
          if (currentElement == matchToken) {
            if (isAttributeData) {
              getAttributeFromCurrentElement(
                structureElement.getAttribute(AttributeNames.XML_ATTRIBUTE_NAME),
                myStream
              ) match {
                case -\/(errorMessage) =>
                  log.warning(errorMessage) // DEBUG
                case \/-(attributeData) =>
                  if (attributeData.isDefined) {
                    dataBuffer += attributeData.get // Append found data.
                    matchedElement = true
                  }
                  if (isLastSequenceElement)
                    myWrapper
                      .nextToken() // Skip to next token because we just read the last attribute.
              }
              readXml = false
            } else {
              // Read the data until the element is closed.
              var nextToken = myWrapper.nextToken()
              while (nextToken != XMLStreamConstants.END_ELEMENT) {
                // We prefer `CDATA` over simple text.
                if (nextToken == XMLStreamConstants.CDATA)
                  dataBuffer += myStream.getText
                else if (nextToken == XMLStreamConstants.CHARACTERS)
                  dataBuffer += myStream.getText
                nextToken = myWrapper.nextToken()
              }
              matchedElement = true
              readXml = false
            }
          } else if (parentElementNames.contains(currentElement) || parentElementRootPrevSiblings
                       .exists(s => s.getLocalName == currentElement)) {
            // TODO This check may be insufficient.
            myWrapper
              .nextToken() // We assume that we are still on our correct way through the xml tree to the destined token.
          } else {
            val matchAndId = s"$matchToken (${structureElement.getAttribute("id")})"
            log.warning(
              "Element {} seems to be missing. Parser at offset {}, element '{}', token {}.",
              matchAndId,
              myWrapper.getOffset,
              currentElement,
              currentToken
            )
            if (isLastSequenceElement && parentElementNames.contains(currentElement))
              dataStatus = BaseParserResponseStatus.END_OF_SEQUENCE // We signal an end of sequence if we are the last element within the correct structure.

            readXml = false
          }
        case XMLStreamConstants.END_ELEMENT =>
          // If we come accross the closing element of our parent sequence then we must end the loop.
          val closedElement = myStream.getLocalName
          if (parentSequence.isDefined && closedElement == getXmlElementName(parentSequence.get)) {
            // We need to check if we either matched the element or we are the last element of the sequence row.
            if (matchedElement || isLastSequenceElement || isAttributeData || (parentSequence.isDefined && getParentSequence(
                  parentSequence.get
                ).isDefined)) {
              dataStatus = BaseParserResponseStatus.END_OF_SEQUENCE
              readXml = false
            }
          }
          myWrapper.nextToken()
        // FIXME Handle choices here!
        case XMLStreamConstants.END_DOCUMENT =>
          // We reached the end of the input data.
          dataStatus = BaseParserResponseStatus.END_OF_DATA
          readXml = false
        case _ =>
          // We are only interested in START and END_ELEMENT tokens and in the END_DOCUMENT token.
          myWrapper.nextToken()
      }
    }

    val data =
      if (dataBuffer.isEmpty)
        None
      else {
        structureElement.getTagName match {
          case ElementNames.FORMATTED_NUMBER | ElementNames.FORMATTED_STRING =>
            val format: String =
              if (structureElement.hasAttribute(AttributeNames.FORMAT))
                structureElement.getAttribute(AttributeNames.FORMAT)
              else
                ""
            if (format.nonEmpty) {
              val tmpString = dataBuffer.mkString
              val pattern   = s"(?s)$format".r
              val m         = pattern.findFirstMatchIn(tmpString)
              if (m.isDefined)
                Option(m.get.group(1))
              else {
                log.warning("Could not apply format of element {}!",
                            structureElement.getAttribute("id"))
                log.debug("Element input was: {}", dataBuffer.mkString)
                None
              }
            } else
              Option(dataBuffer.mkString)
          case _ =>
            Option(dataBuffer.mkString)
        }
      }

    log.debug("Parsed {} with '{}' at {} with {}.",
              structureElement.getAttribute("id"),
              data,
              myWrapper.getOffset,
              dataStatus)
    BaseParserResponse(data,
                       DataElementType.StringDataElement,
                       myWrapper.getOffset.toLong,
                       dataStatus)
  }

  override def parseDataElementExceptionHandler(
      e: Throwable,
      currentNode: Node,
      log: DiagnosticLoggingAdapter
  ): Option[ParserDataContainer] =
    if (state.dataStatus != BaseParserResponseStatus.END_OF_DATA)
      super.parseDataElementExceptionHandler(e, currentNode, log)
    else
      None

  /**
    * Get the xml element name from the given dfasdl element which may be stored within an attribute
    * or simply the id of dfasdl element.
    *
    * @param e A dfasdl element.
    * @return A string holding the xml element name.
    */
  private def getXmlElementName(e: Element): String =
    if (e.hasAttribute(AttributeNames.XML_ELEMENT_NAME))
      e.getAttribute(AttributeNames.XML_ELEMENT_NAME)
    else
      e.getAttribute("id")
}
