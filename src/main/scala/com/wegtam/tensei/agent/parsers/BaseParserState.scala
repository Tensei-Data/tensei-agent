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

import scala.collection.mutable
import org.w3c.dom.{ Element, Node }
import org.w3c.dom.traversal.TreeWalker
import com.wegtam.tensei.agent.helpers.XmlHelpers
import com.wegtam.tensei.agent.adt.BaseParserResponseStatus
import com.wegtam.tensei.agent.adt.BaseParserResponseStatus.BaseParserResponseStatusType
import org.dfasdl.utils._

class BaseParserState extends ElementHelpers with XmlHelpers {
  // Stack for structural elements.
  val elementStack = new mutable.Stack[Node]()
  // Stack for sequences.
  val sequenceStack = new mutable.Stack[Node]()
  // Buffer for keeping track of how many rows for each sequence have been parsed.
  val sequenceRows = new mutable.HashMap[String, Long]()
  // Buffer the last parsed Node
  var lastNode: Node = null
  // Status of the last read element (BaseParserResponseStatus)
  var dataStatus: BaseParserResponseStatusType = BaseParserResponseStatus.OK
  // Status of the current choice element if any.
  var choiceStatus: Option[BaseParserChoiceState] = None
  // Position of the current offset of the read data
  var currentOffset: Long = 0
  // Save flag that determines if we can save data or not.
  var canSaveData: Boolean = true

  /**
    * Reset the base parser state to it's initial state, e.g. like creating a new one with `new BaseParserState`.
    */
  def reset(): Unit = {
    elementStack.clear()
    sequenceStack.clear()
    sequenceRows.clear()
    lastNode = null
    dataStatus = BaseParserResponseStatus.OK
    choiceStatus = None
    currentOffset = 0
  }

  /**
    * Buffer the given structural node on the element stack. Sequences are buffered on the sequence stack and their
    * sequence row counter is initialized.
    *
    * @param node Currently added node.
    * @throws RuntimeException If an unknown element type is added or the element stacks are out of sync or broken.
    */
  def add(node: Node): Unit =
    getElementType(node.getNodeName) match {
      case ElementType.StructuralElement =>
        if (!isOnStructuralStack(node)) {
          // Push the structural node on the stack.
          elementStack.push(node)

          if (StructureElementType.isSequence(getStructureElementType(node.getNodeName))) {
            // If we have a sequence we need to push it onto the sequence stack and
            // initialize it's row counter.
            sequenceStack.push(node)
            sequenceRows.put(node.asInstanceOf[Element].getAttribute("id"), 0)
          }

          if (getStructureElementType(node.getNodeName) == StructureElementType.Choice) {
            // We need to initialize the choice state.
            // FIXME We overwrite the old choice state here which is okay as long as we don't allow nested choices.
            choiceStatus = Option(BaseParserChoiceState(currentOffset))
          }
        }
      case ElementType.UnknownElement =>
        throw new RuntimeException(s"Unknown element type: ${node.getNodeName}")
      case _ =>
    }

  /**
    * Remove elements from the stacks until a choice is reached.
    */
  def cleanUpTillLastChoice(): Unit =
    while (elementStack.head.getNodeName != ElementNames.CHOICE) {
      elementStack.pop()
    }

  /**
    * Get the current sequence and delete all elements from the structural and data element stacks
    * until the sequence is reached.
    */
  def cleanUpTillLastSequence(): Unit = {
    val s = sequenceStack.head.asInstanceOf[Element]
    while (elementStack.head.asInstanceOf[Element].getAttribute("id") != s.getAttribute("id")) {
      elementStack.pop()
    }
  }

  /**
    * Get the current running sequence from the sequence stack and check if it's count limit
    * has been reached.
    *
    * @return Either `true` or `false`.
    */
  def currentCountLimitReached: Boolean = {
    val s           = sequenceStack.head.asInstanceOf[Element]
    val count: Long = sequenceRows.get(s.getAttribute("id")).get
    getStructureElementType(s.getTagName) match {
      case StructureElementType.FixedSequence =>
        count >= s.getAttribute("count").toLong
      case StructureElementType.Sequence =>
        if (s.hasAttribute("max")) count >= s.getAttribute("max").toLong else false
      case _ =>
        false
    }
  }

  /**
    * Determine whether the current sequence has the `keepID` attribute set to true.
    *
    * @return `true` if the flag is set to `true` and `false` otherwise
    */
  def currentSequenceHasKeepId: Boolean =
    sequenceStack.nonEmpty && sequenceStack.head
      .asInstanceOf[Element]
      .getAttribute(AttributeNames.KEEP_ID) == "true"

  /**
    * Returns the actual sequence.
    *
    * @return The actual sequence.
    */
  def getCurrentSequence: Node = sequenceStack.head

  /**
    * Return the row count of the actual sequence.
    *
    * @return Number of rows parsed for the actual sequence.
    */
  def getCurrentSequenceRowCount: Long =
    if (sequenceStack.isEmpty)
      -1L
    else
      getSequenceRowCount(sequenceStack.head.asInstanceOf[Element].getAttribute("id"))

  /**
    * Return the row count of the given sequence id.
    *
    * @param id The ID of the sequence.
    * @return Number of rows parsed for the given sequence.
    */
  def getSequenceRowCount(id: String): Long = sequenceRows.getOrElse(id, -1L)

  /**
    * Returns the current structural element without deleting it from the stack.
    *
    * @return The current structural element.
    */
  def getCurrentStructuralElement: Node = elementStack.head

  /**
    * Increment the row count of the current sequence.
    *
    * @return Row count of the sequence.
    */
  def incrementCurrentSequenceRowCount: Long =
    incrementSequenceRowCount(sequenceStack.head.asInstanceOf[Element].getAttribute("id"))

  /**
    * Increment the row count of the given sequence.
    *
    * @param id ID of the sequence.
    * @return Row count of the sequence.
    */
  def incrementSequenceRowCount(id: String): Long = {
    val count = getSequenceRowCount(id) + 1
    sequenceRows.put(id, count)
    count
  }

  /**
    * Decrement the row count of the given sequence.
    *
    * @param id ID of the sequence.
    * @return Row count of the sequence.
    */
  def decrementSequenceRowCount(id: String): Long = {
    val count = getSequenceRowCount(id) - 1
    sequenceRows.put(id, count)
    count
  }

  /**
    * Reset the sequence row counter for the given sequence to zero and return
    * it's previous value.
    *
    * @param id The ID of the sequence.
    * @return The value of the sequence row counter before the reset.
    */
  def resetSequenceRowCount(id: String): Long = {
    val count = getSequenceRowCount(id)
    sequenceRows.put(id, 0)
    count
  }

  /**
    * Determine whether there is a choice on the element stack which means that we are within a choice element.
    *
    * @return `true` if there are choices on the element stack and `false` otherwise.
    */
  def isInChoice: Boolean =
    if (elementStack.isEmpty)
      false
    else
      elementStack.count(_.getNodeName == ElementNames.CHOICE) > 0

  /**
    * Determine whether the sequence stack has values and the current state is in sequence.
    *
    * @return `true` if the there are elements on the sequence stack and `false` if the stack is empty.
    */
  def isInSequence: Boolean = sequenceStack.nonEmpty

  /**
    * Determines if the current node of the tree walker is the last child of the current element
    * on the element stack. We check the next sibling and if it exists compare it's parent node
    * to the current node of the element stack.
    *
    * @param treeWalker A tree walker.
    * @return Returns `true` if it is the last child and `false` otherwise.
    */
  def isLastChildOfCurrentStructure(treeWalker: TreeWalker): Boolean = {
    val currentNode = treeWalker.getCurrentNode
    val status      = treeWalker.nextSibling() == null
    treeWalker.setCurrentNode(currentNode) // Important! Spool back the tree walker to the correct node!
    status
  }

  /**
    * Checks if a given node is already on the structural element stack.
    *
    * @param node The node to check.
    * @return `true` if the element is on the stack and `false` otherwise.
    */
  def isOnStructuralStack(node: Node): Boolean = {
    val elements = elementStack.filter(n => {
      n.asInstanceOf[Element].getAttribute("id") == node.asInstanceOf[Element].getAttribute("id")
    })
    elements.nonEmpty
  }

  /**
    * Remove the current data element from the internal stack and return it.
    * ATTENTION! To keep the stacks for structural and data elements in sync the current element from
    * the former is also deleted.
    *
    * @return The current data element.
    */
  def removeCurrentDataElement(): Node =
    elementStack.pop()

  /**
    * Remove the current sequence from the internal sequence stack and return it.
    *
    * @return The current sequence.
    */
  def removeCurrentSequence(): Node = {
    val s = sequenceStack.pop()
    s
  }

  /**
    * Remove the current structural element from the internal stack and return it.
    * ATTENTION! To keep the stacks for structural and data elements in sync the current element from
    * the latter is also deleted.
    *
    * @return The current structural element.
    */
  def removeCurrentStructuralElement(): Node =
    elementStack.pop()

  /**
    * Returns the size of the element stack.
    *
    * @return The number of elements on the element stack.
    */
  def size(): Int = elementStack.size

  override def toString: String =
    s"""
       |BaseParserState(
       |  elementStack: ${elementStack.mkString(", ")},
       |  sequenceStack: ${sequenceStack.mkString(", ")},
       |  sequenceRows: ${sequenceRows.mkString(", ")},
       |  lastNode: $lastNode,
       |  dataStatus: $dataStatus,
       |  choiceStatus: $choiceStatus,
       |  currentOffset: $currentOffset,
       |  canSaveData: $canSaveData
       |)
     """.stripMargin
}
