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

import javax.xml.xpath.{ XPathConstants, XPathFactory }

import akka.event.DiagnosticLoggingAdapter
import akka.util.ByteString
import com.wegtam.tensei.adt.{ Cookbook, DFASDLReference }
import com.wegtam.tensei.agent.adt.BaseParserResponseStatus.{ END_OF_DATA, END_OF_SEQUENCE }
import com.wegtam.tensei.agent.adt.{
  BaseParserChoiceStatus,
  BaseParserResponse,
  BaseParserResponseStatus,
  ParserDataContainer
}
import com.wegtam.tensei.agent.exceptions.{ BaseParserFormatException, DataParseException }
import com.wegtam.tensei.agent.helpers.{ GenericHelpers, XmlHelpers }
import org.dfasdl.utils._
import org.dfasdl.utils.exceptions.{ DataElementValidationException, LengthValidationException }
import org.w3c.dom.traversal.{ DocumentTraversal, NodeFilter, TreeWalker }
import org.w3c.dom.{ Document, Element, Node, NodeList }

import scala.util.Try

/**
  * The BaseParser traverses the DFASDL document and constructs the needed data nodes.
  */
trait BaseParser
    extends DocumentHelpers
    with DataElementProcessors
    with DataElementExtractors
    with XmlHelpers
    with GenericHelpers {
  val DEFAULT_STOP_SIGN = "\r\n?|\n"

  val state = new BaseParserState

  val DEFAULT_DATETIME = "1970-01-01 00:00:00"

  val DEFAULT_DATE = "1970-01-01"

  /**
    * Read the actual data from the data stream and return a `BaseParserResponse`.
    *
    * @param structureElement The data description.
    * @param useOffset An optional offset to use.
    * @param isInChoice Determines if we are within a choice.
    * @return A base parser response holding the data.
    */
  def readDataElement(structureElement: Element,
                      useOffset: Long = -1,
                      isInChoice: Boolean = false): BaseParserResponse

  /**
    * Save the given data element.
    *
    * @param data        The data element to save.
    * @param dataHash    The hash that specifies the data storage location.
    * @param referenceId An optional id of a referenced data element.
    */
  def save(data: ParserDataContainer, dataHash: Long, referenceId: Option[String] = None): Unit

  /**
    * A handler function that is called whenever the parser discovers a structural element.
    * Sub parsers may override this function to enable additional functionality.
    *
    * @param e The element discovered.
    */
  def parserStructuralElementHandler(e: Element): Unit = {}

  /**
    * A handler function that is called from [[finishSequence]] if the sequence is not yet finished.
    * Because `finishSequence` is only called if the parser reaches the last child beneath a sequence
    * this means that we basically reached the end of a "sequence row".
    *
    * @param s The element that describes the sequence.
    */
  def parserFinishSequenceRowHandler(s: Element): Unit = {}

  /**
    * A handler function that is called from [[traverseRecursive]] if the currently
    * parsed element has no more siblings meaning that is is the last child of the
    * current parent element. This happens when the recursion is resolved e.g.
    * the tree is traversed back upwards.
    * Sub parsers may override this function to enable additional functionality.
    *
    * @param p The parent element that has no more children.
    */
  def parserResolveRecursionHandler(p: Element): Unit = {}

  /**
    * Clean and validate the given data using the describing element.
    *
    * @param container The data to be cleaned and validated.
    * @param element   The element describing the data.
    * @return The cleaned data.
    */
  @throws[IllegalArgumentException]
  @throws[LengthValidationException]
  @throws[NumberFormatException]
  def cleanAndValidateData(container: ParserDataContainer, element: Element): ParserDataContainer = {
    if (container.elementId != element.getAttribute("id"))
      throw new IllegalArgumentException(
        s"Container element id and structure element id do not match (${container.elementId} != ${element
          .getAttribute("id")})!"
      )

    val cleanedData =
      if (getDataElementType(element.getNodeName) == DataElementType.StringDataElement && container.data != None) {
        val rawDataString = container.data.asInstanceOf[String]
        val processedData =
          element.getNodeName match {
            case ElementNames.FORMATTED_NUMBER =>
              processFormattedNumberData(rawDataString, element)
            case ElementNames.FORMATTED_STRING => processStringData(rawDataString, element)
            case ElementNames.NUMBER           => processNumberData(rawDataString, element)
            case ElementNames.STRING           => processStringData(rawDataString, element)
            case _                             => rawDataString
          }
        if (processedData.isEmpty)
          processedData
        else
          Try(extractData(processedData, element)) match {
            case scala.util.Failure(e) =>
              element.getTagName match {
                case ElementNames.DATETIME => java.sql.Timestamp.valueOf(DEFAULT_DATETIME)
                case ElementNames.DATE     => java.sql.Date.valueOf(DEFAULT_DATE)
                case _ =>
                  throw BaseParserFormatException.create(
                    s"Format exception on element: ${element.getAttribute("id")} in DFASDL: ${container.dfasdlId}",
                    e
                  )
              }
            case scala.util.Success(d) =>
              element.getTagName match {
                case ElementNames.FORMATTED_STRING | ElementNames.STRING =>
                  ByteString(d.asInstanceOf[String]) // Convert String to ByteString
                case _ => d
              }
          }
      } else
        container.data

    container.copy(data = cleanedData)
  }

  /**
    * Analyse the mappings in the given cookbook and return a set of sequence ids
    * that are the parent sequences of the used source ids.
    *
    * @param c A cookbook.
    * @param r A reference to a DFASDL.
    * @return A set of sequence ids.
    */
  def getSourceParentSequences(c: Cookbook, r: DFASDLReference): Set[String] =
    c.findDFASDL(r)
      .map { dfasdl =>
        val dfasdlTree = createNormalizedDocument(dfasdl.content)
        val sourceIds  = c.recipes.flatMap(r => r.mappings.flatMap(t => t.sources.map(_.elementId)))
        sourceIds
          .map { id =>
            val e = dfasdlTree.getElementById(id)
            if (e != null)
              getParentSequence(e).map(_.getAttribute("id")).getOrElse("")
            else
              ""
          }
          .filter(!_.isEmpty)
          .toSet
      }
      .getOrElse(Set.empty[String])

  /**
    * Extract data from a given string. Using the stop sign parameter.
    * If the stop sign is empty then the whole input string is returned.
    * The stop sign may contain a single character, a concrete "stop word" or
    * a collection of "stop characters". A collection has to be started with
    * "[" and ended with "]".
    *
    * @param src The data string.
    * @param stop_sign An optional stop sign.
    * @return An option to the extracted data or `None`.
    */
  def extractDataUsingStopSign(src: String, stop_sign: String): Option[String] =
    stop_sign.length match {
      case 0 =>
        Option(src)
      case 1 =>
        val tmp = src takeWhile (_ != stop_sign.head)
        if (tmp.length < src.length)
          Option(tmp)
        else
          None
      case _ =>
        if (stop_sign.startsWith("[") && stop_sign.endsWith("]")) {
          val collection = stop_sign.substring(1, stop_sign.length - 1)
          val tmp        = src takeWhile (!collection.contains(_))
          if (tmp.length < src.length)
            Option(tmp)
          else
            None
        } else {
          val pos = src.indexOf(stop_sign)
          if (pos > 0)
            Option(src.substring(0, pos))
          else
            None
        }
    }

  /**
    * Traverse the given dfasdl tree and parse the source data.
    *
    * @param dfasdl A DFASDL description tree.
    * @param log    The logger of the respective parser for a correct logging into the database
    *               depending on the uuid.
    */
  def traverseTree(dfasdl: Document, log: DiagnosticLoggingAdapter): Unit = {
    val traversal = dfasdl.asInstanceOf[DocumentTraversal]
    val treeWalker =
      traversal.createTreeWalker(dfasdl.getDocumentElement, NodeFilter.SHOW_ELEMENT, null, true)

    val _ = traverseRecursive(treeWalker, log)
  }

  /**
    * Handle the state of a choice if we are within one.
    * This method sets flags in the `BaseParserState` and cleans up obsolete data and structural elements.
    *
    * @param currentNode   The currently parsed node.
    * @param previousNode  The previously parsed node which can be a sibling or a parent.
    */
  def handleChoice(currentNode: Node, previousNode: Node): Unit =
    if (state.isInChoice) {
      // If we are a direct child of a choice and have no further children.
      if (previousNode.getParentNode != null && previousNode.getParentNode.getNodeName == ElementNames.CHOICE && currentNode == null) {
        // Set the choice status to the next logical state.
        val choiceStatus = state.choiceStatus.get
        choiceStatus.status match {
          case BaseParserChoiceStatus.UNMATCHED =>
            state.choiceStatus = Option(choiceStatus.copy(status = BaseParserChoiceStatus.MATCHED))
          case BaseParserChoiceStatus.BROKEN =>
            state.choiceStatus = Option(
              choiceStatus.copy(status = BaseParserChoiceStatus.UNMATCHED)
            )
            state.currentOffset = choiceStatus.offset // Reset offset.
          case _ =>
        }
        // If we are the last direct child of the choice.
        if (currentNode == null) {
          state.cleanUpTillLastChoice()
        }
      }
    }

  /**
    * Regular handler for parsed nodes.
    * It modifies the parser state, tries to parse the data and may call the `cleanupAndSave` function.
    *
    * @param currentNode The currently parsed node.
    * @param treeWalker  The treewalker.
    * @param log         The logger of the respective parser for a correct logging into the database
    *                    depending on the uuid.
    */
  def handleNode(currentNode: Node, treeWalker: TreeWalker, log: DiagnosticLoggingAdapter): Unit =
    getElementType(currentNode.getNodeName) match {
      case ElementType.StructuralElement =>
        if (getStructureElementType(currentNode.getNodeName) == StructureElementType.Reference) {
          val currentId = currentNode.asInstanceOf[Element].getAttribute("id")
          val referenceId =
            currentNode.asInstanceOf[Element].getAttribute(AttributeNames.SOURCE_ID)
          val sequenceData =
            for (s <- state.sequenceStack) yield {
              val sid = s.asInstanceOf[Element].getAttribute("id")
              //(sid, state.sequenceRows.getOrElse(sid, 0L))
              (sid, state.sequenceRows(sid)) // TODO This will crash if the sequence was not properly initialised!
            }

          val grandParentSequenceRowCounter =
            if (sequenceData.size > 1)
              sequenceData(1)._2
            else if (sequenceData.nonEmpty)
              sequenceData.head._2
            else
              -1L

          val hash = calculateDataElementStorageHash(currentId, sequenceData.toList)
          val data = ParserDataContainer(
            data = "",
            elementId = currentId,
            dfasdlId = None,
            sequenceRowCounter = grandParentSequenceRowCounter,
            dataElementHash = Option(hash)
          )
          save(data, hash, Option(referenceId))
        } else {
          state.add(currentNode)
          parserStructuralElementHandler(currentNode.asInstanceOf[Element])
        }
      case ElementType.DataElement =>
        if (state.isInChoice && state.choiceStatus.get.status != BaseParserChoiceStatus.UNMATCHED) {
          // The choice has already been matched or is broken therefore we don't need to handle the elements.
        } else {
          // Try to parse the current data element.
          val data: Option[ParserDataContainer] = try {
            Option(parseDataElement(currentNode.asInstanceOf[Element]))
          } catch {
            case e: Throwable => parseDataElementExceptionHandler(e, currentNode, log)
          }

          if (data.isDefined) {
            val sequenceData =
              for (s <- state.sequenceStack) yield {
                val sid = s.asInstanceOf[Element].getAttribute("id")
                //(sid, state.sequenceRows.getOrElse(sid, 0L))
                (sid, state.sequenceRows(sid)) // TODO This will crash if the sequence was not properly initialised!
              }

            val mustSave: Boolean =
              if (state.dataStatus == END_OF_DATA && state.isInSequence) {
                // Determine the last element of the parent element of the current node
                val lastElementOfSequence: Option[Element] =
                  if (currentNode.getParentNode != null)
                    getChildDataElementsFromElement(
                      currentNode.getParentNode.asInstanceOf[Element]
                    ).reverse.headOption
                  else
                    None

                // If the element is the last element of a sequence, we must save it
                lastElementOfSequence.exists(
                  le =>
                    data.exists(
                      d =>
                        d.elementId.nonEmpty && le.hasAttribute("id") && le
                          .getAttribute("id") == d.elementId
                  )
                )
              } else
                true

            if (mustSave) {
              data.fold(throw new RuntimeException("No parser data to save!"))(d => {
                val hash = calculateDataElementStorageHash(d.elementId, sequenceData.toList)
                save(d, hash)
              })
            }
          }
        }
      case ElementType.ExpressionElement =>
      case ElementType.RootElement       =>
      case ElementType.UnknownElement    => throw new RuntimeException("Unknown element type!")
    }

  /**
    * Handle the current sequence if we are within one.
    * It cleans up obsolete elements and returns an option to a node if the sequence is not finished.
    *
    * @param currentNode   The currently parsed node.
    * @param previousNode  The previously parsed node which can be a sibling or a parent.
    * @param treeWalker    The treewalker.
    * @return An option to the next node if the sequence is not yet finished.
    */
  def handleSequence(currentNode: Node, previousNode: Node, treeWalker: TreeWalker): Option[Node] =
    if (state.isInSequence) {
      // Check if we have reached the end of the current level...
      if (currentNode == null) {
        // ...and if we have a parent node.
        if (previousNode.getParentNode != null) {
          // If we have reached the top of one sequence subtree.
          if (previousNode.getParentNode == state.getCurrentSequence) {
            // We check if the our parent (`currentNode`) has siblings.
            val n = treeWalker.getCurrentNode
            treeWalker.setCurrentNode(previousNode)
            val lastChild = state.isLastChildOfCurrentStructure(treeWalker)
            treeWalker.setCurrentNode(n)
            // If our parent has no siblings.
            if (lastChild) {
              val nextNode = finishSequence(treeWalker) // Finish the sequence row and return the next logical node or null.
              if (nextNode != null)
                Option(nextNode)
              else
                None
            } else
              None
          } else {
            if (state.size() > 0 && state.getCurrentStructuralElement == previousNode)
              state.removeCurrentStructuralElement()
            None
          }
        } else
          throw new RuntimeException("Node without parent in sequence!")
      } else
        None
    } else
      None

  /**
    * Parse the current level of the tree by using recursion. This function returns `true` if the current structure
    * should be parsed again. This is needed for the handling of sequences. The default return value is `false`.
    *
    * @param treeWalker A tree walker.
    * @param log        The logger of the respective parser for a correct logging into the database
    *                   depending on the uuid.
    * @return Returns a boolean that indicates if the current structure should be parsed again (`true`) or not (`false`).
    */
  def traverseRecursive(treeWalker: TreeWalker, log: DiagnosticLoggingAdapter): Boolean = {
    var reEntryOnParentNode = false

    val currentNode = treeWalker.getCurrentNode
    var nextNode    = treeWalker.firstChild()

    if (state.dataStatus != BaseParserResponseStatus.END_OF_DATA || state.isInChoice)
      handleNode(currentNode, treeWalker, log)

    while (nextNode != null) {
      if (traverseRecursive(treeWalker, log)) {
        treeWalker.setCurrentNode(currentNode)
        nextNode = treeWalker.firstChild()
      } else
        nextNode =
          if (state.isInSequence && state.dataStatus == END_OF_SEQUENCE)
            null
          else
            treeWalker.nextSibling()

      // Handle a choice.
      handleChoice(currentNode = nextNode, previousNode = currentNode)
      // If we are within a sequence, we need special handling.
      val seqNode =
        handleSequence(currentNode = nextNode, previousNode = currentNode, treeWalker = treeWalker)
      if (seqNode.isDefined) {
        reEntryOnParentNode = true
      }
    }

    treeWalker.setCurrentNode(currentNode)

    if (isStructuralElement(currentNode.getNodeName))
      parserResolveRecursionHandler(currentNode.asInstanceOf[Element])

    if (state.size() > 0 && state.getCurrentStructuralElement == currentNode)
      state
        .removeCurrentStructuralElement() // We need to remove ourselfs from the state if we go back upwards in the recursion.

    state.lastNode = currentNode

    reEntryOnParentNode
  }

  /**
    * Clean up the `BaseParserState` upon the end of a sequence.
    */
  def cleanupSequenceState(): Unit = {
    // We get the element from the state which is the sequence which is our parent.
    state.removeCurrentStructuralElement()

    // We have to remove the sequence from the state.
    // This step is important for the isInSequence check.
    val s              = state.removeCurrentSequence()
    val parentSequence = getParentSequence(s) // Check if we have a parent sequence.

    // Reset the status to avoid recursive clean up of sequences that are stacked
    if (state.dataStatus == BaseParserResponseStatus.END_OF_SEQUENCE && parentSequence.isDefined)
      state.dataStatus = BaseParserResponseStatus.OK
  }

  /**
    * Check if the sequence has been completed. If that is the case the sequence is removed from the stack.
    * If it has not been completed the sequence node is returned so the tree walker can be set back correctly.
    *
    * @todo We should use an `Option[Node]` instead of `null` here.
    * @param treeWalker The actual tree walker.
    * @return A node for re-entry in the tree or `null`.
    */
  def finishSequence(treeWalker: TreeWalker): Node = {
    var reEntryNode: Node = null

    state.cleanUpTillLastSequence() // Clean up until we reach our parent sequence.

    state.incrementCurrentSequenceRowCount // increment the row counter
    // If we have reached the count limit or the end of the data or the end of the sequence, we save the whole sequence data and clean up.
    if (state.currentCountLimitReached || state.dataStatus == BaseParserResponseStatus.END_OF_DATA || state.dataStatus == BaseParserResponseStatus.END_OF_SEQUENCE) {
      cleanupSequenceState()
    } else {
      // The end of the sequence has not been reached, therefore we have to set the tree walker
      // again onto the sequence node and `nextNode` to it's first child.
      reEntryNode = state.getCurrentSequence
      val currentCounter = state.getCurrentSequenceRowCount
      // Call the handler function because we have just finished a "row".
      parserFinishSequenceRowHandler(reEntryNode.asInstanceOf[Element])
      val newCounter = state.getCurrentSequenceRowCount
      if (newCounter == 0 && currentCounter > 0) {
        // Clean the sequence that is finished now
        // This is important to have a clean `state`
        cleanupSequenceState()
        reEntryNode = null
      }
    }

    reEntryNode
  }

  /**
    * Parse the actual data described by the given element.
    * The given element must be a `DataElementType`.
    * Before returned the data is cleaned and validated.
    *
    * @param structureElement The data element description.
    * @return A container holding the parsed data and some meta information.
    */
  def parseDataElement(structureElement: Element): ParserDataContainer = {
    val subParserResponse =
      if (state.isInChoice)
        readDataElement(structureElement, state.currentOffset, state.isInChoice)
      else
        readDataElement(structureElement)

    val rowCount: Option[Long] =
      if (state.isInSequence)
        Option(state.getCurrentSequenceRowCount)
      else
        None

    val dataContainer =
      subParserResponse.elementType match {
        case DataElementType.BinaryDataElement =>
          ParserDataContainer(data = subParserResponse.data.getOrElse(None),
                              elementId = structureElement.getAttribute("id"),
                              sequenceRowCounter = rowCount.getOrElse(-1L))
        case DataElementType.StringDataElement =>
          ParserDataContainer(data = subParserResponse.data.getOrElse(None),
                              elementId = structureElement.getAttribute("id"),
                              sequenceRowCounter = rowCount.getOrElse(-1L))
        case DataElementType.UnknownElement =>
          throw new RuntimeException("Unknown data element type!")
      }

    state.dataStatus = subParserResponse.status    // Buffer the response of the underlying data reader.
    state.currentOffset = subParserResponse.offset // Buffer the current offset of the actual read data.

    if (subParserResponse.status == BaseParserResponseStatus.ERROR)
      throw new DataParseException("Parser returned an error status!")

    // We had no error, therefore we have to modify the offset if it is desired.
    if (structureElement.hasAttribute(AttributeNames.CORRECT_OFFSET) && structureElement
          .getAttribute(AttributeNames.CORRECT_OFFSET)
          .toLong != 0)
      state.currentOffset = state.currentOffset + structureElement
        .getAttribute(AttributeNames.CORRECT_OFFSET)
        .toLong

    if (subParserResponse.data.isEmpty && subParserResponse.status == BaseParserResponseStatus.END_OF_DATA && state.isInSequence) {
      // We are within a sequence and have received no data and the data status is `END_OF_DATA`.
      val xpath = XPathFactory.newInstance().newXPath().compile("descendant::*[count(./*) = 0]")
      val nodes = xpath
        .evaluate(state.getCurrentSequence, XPathConstants.NODESET)
        .asInstanceOf[NodeList] // Get leaf e.g. data nodes from current sequence.
      val lastColumn = nodes.item(nodes.getLength - 1).asInstanceOf[Element]
      if (lastColumn != structureElement) {
        // We are not the last child of the sequence, therefore we must not save.
        // TODO Maybe we should add a custom exception.
        throw new DataParseException("No more data within sequence!")
      }
    }

    cleanAndValidateData(dataContainer, structureElement)
  }

  /**
    * Handle an exception that was thrown while trying to parse the next data element.
    *
    * @param e           The exception that was thrown.
    * @param currentNode The current node in the structure tree.
    * @param log         The logger of the respective parser for the correct logging into the database
    *                    regarding to the unique uuid.
    * @return An element that can be used further by the parsing algorithm.
    * @throws Throwable Unhandled exceptions are "passed through" or encapsulated within a runtime exception.
    */
  def parseDataElementExceptionHandler(
      e: Throwable,
      currentNode: Node,
      log: DiagnosticLoggingAdapter
  ): Option[ParserDataContainer] =
    e match {
      case t @ (_: DataElementValidationException | _: DataParseException |
          _: NumberFormatException) =>
        if (state.isInChoice) {
          // Mark the choice broken.
          state.choiceStatus = Option(
            state.choiceStatus.get.copy(status = BaseParserChoiceStatus.BROKEN)
          )
          None
        } else if (state.dataStatus == END_OF_DATA || state.dataStatus == END_OF_SEQUENCE) {
          // This is currently a workaround for parsing to often. We return an empty data element here because there can't be any more data.
          None
        } else {
          log.error(t, "An error occurred at parser byte offset {}!", state.currentOffset)
          if (currentNode != null)
            Try(currentNode.asInstanceOf[Element])
              .foreach(elem => log.error("Current element id: {}", elem.getAttribute("id")))

          throw new RuntimeException(t) // Escalate the exception upwards.
        }
      case _ =>
        log.error(e, "An unexpected error occurred at parser byte offset {}!", state.currentOffset)
        if (currentNode != null)
          Try(currentNode.asInstanceOf[Element])
            .foreach(elem => log.error("Current element id: {}", elem.getAttribute("id")))
        throw new RuntimeException("An unexpected error occurred!", e) // Escalate the exception upwards.
    }
}
