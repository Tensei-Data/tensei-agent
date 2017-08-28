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

import javax.xml.xpath.{ XPath, XPathConstants, XPathFactory }

import org.w3c.dom.{ Document, Element, Node, NodeList }
import javax.xml.parsers.DocumentBuilderFactory
import org.xml.sax.InputSource
import java.io.{ StringReader, StringWriter, Writer }
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.{ OutputKeys, TransformerFactory }
import javax.xml.transform.dom.DOMSource
import org.w3c.dom.traversal.{ DocumentTraversal, NodeFilter, TreeWalker }
import scala.collection.mutable.ListBuffer

/**
  * A trait providing some simple xml helper functions.
  */
trait XmlHelpers {

  /**
    * Compares the specified attribute of the given elements and returns `true` if they match.
    * This function will only return `true` if both elements have the specified attribute and
    * if it has the same value!
    *
    * @param e1 A DOM element.
    * @param e2 A DOM element.
    * @param attrName The name of the attribute.
    * @return Returns `true` if the ids match and `false` otherwise.
    */
  def compareAttribute(e1: Element, e2: Element, attrName: String): Boolean =
    e1.getAttribute(attrName) == e2.getAttribute(attrName)

  /**
    * Compares the specified attribute of the given nodes and returns `true` if they match.
    * This function will only return `true` if both nodes have the specified attribute and
    * if it has the same value!
    *
    * @param n1 A DOM node.
    * @param n2 A DOM node.
    * @param attrName The name of the attribute.
    * @return Returns `true` if the ids match and `false` otherwise.
    */
  def compareAttribute(n1: Node, n2: Node, attrName: String): Boolean =
    compareAttribute(n1.asInstanceOf[Element], n2.asInstanceOf[Element], attrName)

  /**
    * Copy the xml structure of the given element including all tags and attributes
    * but not(!) the data.
    *
    * @param e An xml element.
    * @param inSequence Indicate if the given element is part of a sequence.
    * @param keepId If set to `true` the id is converted to a class or the existing class is extended with the former id.
    * @return An exact copy of the element without data.
    */
  def copyElementStructure(e: Element,
                           inSequence: Boolean = false,
                           keepId: Boolean = true): Element = {
    val doc        = createNewDocument()
    val copy       = doc.createElement(e.getTagName)
    val attributes = e.getAttributes
    var i          = 0
    while (i < attributes.getLength) {
      val attrName = attributes.item(i).getNodeName
      if (inSequence) {
        if (keepId) {
          // Convert ID to class and copy all other attributes.
          attrName match {
            case "id" =>
              val classes = s"${e.getAttribute("class")} id:${e.getAttribute("id")}".trim
              copy.setAttribute("class", classes)
            case "class" =>
              if (copy.getAttribute("class").isEmpty && e.hasAttribute("class"))
                copy.setAttribute("class", e.getAttribute("class"))
            case _ => copy.setAttribute(attrName, e.getAttribute(attrName))
          }
        } else if (attrName != "id")
          copy.setAttribute(attrName, e.getAttribute(attrName)) // Copy everything except the ID.
      } else
        copy.setAttribute(attrName, e.getAttribute(attrName)) // Copy everything.
      i += 1
    }
    doc.appendChild(copy)
    copy
  }

  /**
    * Creates an empty xml document.
    *
    * @return An empty DOM tree document.
    */
  def createNewDocument(): Document = {
    val factory = DocumentBuilderFactory.newInstance()
    val loader  = factory.newDocumentBuilder()
    val doc     = loader.newDocument()
    doc
  }

  /**
    * Checks if an element with the given id exists within the given xml container.
    * The check for the id consists of checking the attribute "id" and the attribute
    * "class" for something like "id:GIVEN_ID".
    *
    * @param id        The id of the element.
    * @param container The xml container element.
    * @return `true` if the element exists or `false`.
    */
  def classOrIdExistsInElement(id: String, container: Element): Boolean =
    if (container.hasAttribute("id") && container.getAttribute("id") == id)
      true
    else {
      val xpath: XPath = XPathFactory.newInstance().newXPath()
      if (xpath
            .evaluate(s"""//*[@id="$id"]""", container, XPathConstants.NODE)
            .asInstanceOf[Element] != null)
        true
      else {
        val nodes = xpath
          .evaluate(s"""(//*[contains(concat(' ', @class, ' '), ' id:$id ')])""",
                    container,
                    XPathConstants.NODESET)
          .asInstanceOf[NodeList]
        if (nodes != null && nodes.getLength > 0)
          true
        else
          false
      }
    }

  /**
    * Return the given dom element as pretty printed string.
    *
    * @param e A DOM element.
    * @return A string.
    */
  def prettifyXml(e: Element): String = {
    val output = new StringBuilder
    output ++= s"<${e.getTagName}"
    var i = 0
    while (i < e.getAttributes.getLength) {
      val attrName = e.getAttributes.item(i).getNodeName
      output ++= s" $attrName="
      output ++= "\""
      output ++= e.getAttribute(attrName)
      output ++= "\""
      i += 1
    }
    output ++= ">"
    output.toString()
  }

  /**
    * Return the given XML DOM Document as pretty printed string.
    *
    * @param xml The DOM Document.
    * @return A string containing the xml.
    */
  def prettifyXml(xml: Document): String = {
    val tf = TransformerFactory.newInstance().newTransformer()
    tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8")
    tf.setOutputProperty(OutputKeys.INDENT, "yes")
    tf.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2")

    val out: Writer = new StringWriter()
    tf.transform(new DOMSource(xml), new StreamResult(out))
    out.toString
  }

  /**
    * Walk throug the given structure and remove all empty nodes from the xml tree.
    *
    * @param data The root/entry node for the treewalker.
    */
  def removeEmptyChildren(data: Element): Unit =
    if (data != null && data.getOwnerDocument != null) {
      val traversal  = data.getOwnerDocument.asInstanceOf[DocumentTraversal]
      val treeWalker = traversal.createTreeWalker(data, NodeFilter.SHOW_ELEMENT, null, true)

      var elements = getElementsToDelete(treeWalker)
      while (elements.size > 0) {
        elements.foreach(node => node.getParentNode.removeChild(node))
        elements = getElementsToDelete(treeWalker)
      }
    }

  /**
    * Traverse through the given tree and return all elements that are safe to delete.
    *
    * @param treeWalker A treewalker.
    * @return A list of nodes that can be deleted.
    */
  private def getElementsToDelete(treeWalker: TreeWalker): List[Node] = {
    val elements: ListBuffer[Node] = ListBuffer[Node]()

    val currentNode = treeWalker.getCurrentNode
    val hasParent   = currentNode.getParentNode != null
    var nextNode    = treeWalker.firstChild()

    // FIXME We have to adjust this to `allow_empty` and possible other data node value types.
    if (hasParent) {
      if (nextNode == null) {
        // A data element type has never children and can be removed if it has no content.
        if (currentNode.getTextContent.length == 0) elements += currentNode
      } else {
        // A structure element can be considered empty if the trimmed `getTextContent` is not empty.
        if (currentNode.getTextContent.trim.length == 0) elements += currentNode
      }
    }

    while (nextNode != null) {
      elements ++= getElementsToDelete(treeWalker)
      nextNode = treeWalker.nextSibling()
    }

    treeWalker.setCurrentNode(currentNode)

    elements.toList
  }

  /**
    * Create an xml tree document from a given xml string.
    * The created document is normalized!
    *
    * @param xmlSource The xml source as string.
    * @return An xml DOM tree.
    */
  def stringToXmlDocument(xmlSource: String): Document = {
    val factory = DocumentBuilderFactory.newInstance()
    val loader  = factory.newDocumentBuilder()
    val xml     = loader.parse(new InputSource(new StringReader(xmlSource)))
    xml.getDocumentElement.normalize()
    xml
  }
}
