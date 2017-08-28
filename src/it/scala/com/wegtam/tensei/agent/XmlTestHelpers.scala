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

package com.wegtam.tensei.agent

import java.io.{ InputStream, StringWriter, Writer }
import javax.xml.XMLConstants
import javax.xml.parsers.{ DocumentBuilder, DocumentBuilderFactory }
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.{ StreamResult, StreamSource }
import javax.xml.transform.{ OutputKeys, TransformerFactory }
import javax.xml.validation.SchemaFactory

import org.dfasdl.utils.{ DFASDLResourceResolver, XmlErrorHandler }
import org.w3c.dom.traversal.{ DocumentTraversal, NodeFilter }
import org.w3c.dom.{ Document, Element, Node }

import scala.collection.mutable.ListBuffer

/**
  * Test helpers for XML.
  */
trait XmlTestHelpers {

  /**
    * Creates a DOM document builder specific for our DFASDL schema.
    *
    * @return A document builder using the DFASDL schema.
    */
  def createTestDocumentBuilder(useSchema: Boolean = true): DocumentBuilder =
    if (useSchema) {
      val xsdMain: InputStream = getClass.getResourceAsStream("/org/dfasdl/dfasdl.xsd")

      val factory = DocumentBuilderFactory.newInstance()
      factory.setValidating(false)
      factory.setNamespaceAware(true)

      val schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
      schemaFactory.setResourceResolver(new DFASDLResourceResolver)
      factory.setSchema(schemaFactory.newSchema(new StreamSource(xsdMain)))

      val builder = factory.newDocumentBuilder()
      builder.setErrorHandler(new XmlErrorHandler)
      builder
    } else
      DocumentBuilderFactory.newInstance().newDocumentBuilder()

  /**
    * Import the given xml element into an empty dom tree and return it.
    *
    * @param element A xml element.
    * @return A dom tree containing only the element and it' structure.
    */
  def createTreeFromElement(element: Element): Document = {
    val builder        = DocumentBuilderFactory.newInstance().newDocumentBuilder()
    val resultTree     = builder.newDocument()
    val importedResult = resultTree.importNode(element, true)
    resultTree.appendChild(importedResult)
    resultTree.getDocumentElement.normalize()
    resultTree
  }

  /**
    * Traverse the tree upwards beginning with the given node and construct the absolute
    * xpath for the node.
    *
    * @param e A node within a tree.
    * @return The absolute xpath.
    */
  def getAbsoluteXPath(e: Node): String = {
    var position = 1
    var s        = e.getPreviousSibling
    while (s != null) {
      if (s.getNodeType == e.getNodeType)
        position += 1
      s = s.getPreviousSibling
    }
    val parent = e.getParentNode
    val path =
      if (parent.getNodeType != Node.DOCUMENT_NODE)
        getAbsoluteXPath(parent) + "/" + e.getNodeName + s"[$position]"
      else
        "/" + e.getNodeName + s"[$position]"
    path
  }

  /**
    * Iterates the xml tree and returns a node list.
    *
    * @param xml An XML tree.
    * @return A list of sequential ordered xml elements.
    */
  def getNodeList(xml: Document): List[Element] = {
    val expectedElements: ListBuffer[Element] = ListBuffer[Element]()
    val traversal                             = xml.asInstanceOf[DocumentTraversal]
    val iterator =
      traversal.createNodeIterator(xml.getDocumentElement, NodeFilter.SHOW_ELEMENT, null, true)
    var currentNode = iterator.nextNode()
    while (currentNode != null) {
      expectedElements += currentNode.asInstanceOf[Element]
      currentNode = iterator.nextNode()
    }
    expectedElements.toList
  }

  /**
    * Return the given dom element as pretty printed string.
    *
    * @param e A DOM element.
    * @return A string.
    */
  def xmlToPrettyString(e: Element): String = {
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
    * Return the given XML DOM as pretty printed string.
    *
    * @param xml The DOM.
    * @return A string containing the xml.
    */
  def xmlToPrettyString(xml: Document): String = {
    val tf = TransformerFactory.newInstance().newTransformer()
    tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8")
    tf.setOutputProperty(OutputKeys.INDENT, "yes")

    val out: Writer = new StringWriter()
    tf.transform(new DOMSource(xml), new StreamResult(out))
    out.toString
  }
}
