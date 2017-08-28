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

import org.dfasdl.utils.ElementNames
import java.io.{ InputStream, StringReader }
import javax.xml.parsers.DocumentBuilderFactory
import com.wegtam.tensei.agent.DefaultSpec
import org.xml.sax.InputSource

class XmlHelpersTest extends DefaultSpec with XmlHelpers {
  describe("XmlHelpers") {
    describe("classOrIdExistsInElement") {
      describe("when searching an existing ID") {
        it("should return true") {
          val d  = createNewDocument()
          val e1 = d.createElement("myElement")
          e1.setAttribute("id", "MY-ID")
          d.appendChild(e1)

          classOrIdExistsInElement("MY-ID", d.getDocumentElement) should be(true)
        }
      }

      describe("when searching an empty document") {
        it("should return false") {
          val d  = createNewDocument()
          val e1 = d.createElement("myElement")
          d.appendChild(e1)

          classOrIdExistsInElement("MY-ID", d.getDocumentElement) should be(false)
        }
      }

      describe("when searching an existing ID within class") {
        it("should return true") {
          val d  = createNewDocument()
          val e1 = d.createElement("myElement")
          e1.setAttribute("class", "foo id:MY-ID bar")
          d.appendChild(e1)

          classOrIdExistsInElement("MY-ID", d.getDocumentElement) should be(true)
        }
      }
    }

    describe("compareAttributes") {
      describe("when given two elements") {
        describe("with an equally set attribute named 'foo'") {
          it("should return true") {
            val attrName  = "foo"
            val attrValue = "bar"
            val d         = createNewDocument()
            val e1        = d.createElement("myElement")
            e1.setAttribute(attrName, attrValue)
            val e2 = d.createElement("myElement")
            e2.setAttribute(attrName, attrValue)

            compareAttribute(e1, e2, attrName) should be(true)
          }
        }

        describe("with different attributes named 'foo'") {
          it("should return false") {
            val attrName = "foo"
            val d        = createNewDocument()
            val e1       = d.createElement("myElement")
            e1.setAttribute(attrName, "some value")
            val e2 = d.createElement("myElement")
            e2.setAttribute(attrName, "another value")

            compareAttribute(e1, e2, attrName) should be(false)
          }
        }

        describe("with the first element missing the attribute") {
          it("should return false") {
            val attrName  = "foo"
            val attrValue = "bar"
            val d         = createNewDocument()
            val e1        = d.createElement("myElement")
            val e2        = d.createElement("myElement")
            e2.setAttribute(attrName, attrValue)

            compareAttribute(e1, e2, attrName) should be(false)
          }
        }

        describe("with the second element missing the attribute") {
          it("should return false") {
            val attrName  = "foo"
            val attrValue = "bar"
            val d         = createNewDocument()
            val e1        = d.createElement("myElement")
            e1.setAttribute(attrName, attrValue)
            val e2 = d.createElement("myElement")

            compareAttribute(e1, e2, attrName) should be(false)
          }
        }

        describe("with both elements missing the attribute") {
          it("should return false") {
            val attrName = "foo"
            val d        = createNewDocument()
            val e1       = d.createElement("myElement")
            val e2       = d.createElement("myElement")

            compareAttribute(e1, e2, attrName) should be(true)
          }
        }
      }
    }

    describe("removeEmptyChildren") {
      describe("when given an empty tree") {
        it("should do nothing") {
          val doc = createNewDocument()
          removeEmptyChildren(doc.getDocumentElement)
        }
      }

      describe("when given a tree without empty elements") {
        it("should do nothing") {
          val doc  = createNewDocument()
          val root = doc.createElement(ElementNames.ELEMENT)
          doc.appendChild(root)

          var e = doc.createElement(ElementNames.ELEMENT)
          var d = doc.createElement(ElementNames.STRING)
          d.setTextContent("TEST!")
          e.appendChild(d)
          root.appendChild(e)

          e = doc.createElement(ElementNames.ELEMENT)
          d = doc.createElement(ElementNames.STRING)
          d.setTextContent("TEST!")
          e.appendChild(d)
          root.appendChild(e)

          e = doc.createElement(ElementNames.ELEMENT)
          d = doc.createElement(ElementNames.STRING)
          d.setTextContent("TEST!")
          e.appendChild(d)
          root.appendChild(e)

          val expectedXmlString = prettifyXml(doc)

          removeEmptyChildren(root)

          prettifyXml(doc) should be(expectedXmlString)
        }
      }

      describe("when given a tree containing empty elements") {
        it("should remove the empty elements") {
          val doc         = createNewDocument()
          val expectedDoc = createNewDocument()
          val root        = doc.createElement(ElementNames.ELEMENT)
          doc.appendChild(root)
          val expectedRoot = expectedDoc.importNode(root, true)
          expectedDoc.appendChild(expectedRoot)

          var e = doc.createElement(ElementNames.ELEMENT)
          var d = doc.createElement(ElementNames.STRING)
          d.setTextContent("TEST!")
          e.appendChild(d)
          root.appendChild(e)
          expectedRoot.appendChild(expectedDoc.importNode(e, true))

          e = doc.createElement(ElementNames.ELEMENT)
          d = doc.createElement(ElementNames.STRING)
          e.appendChild(d)
          root.appendChild(e)

          e = doc.createElement(ElementNames.ELEMENT)
          d = doc.createElement(ElementNames.STRING)
          d.setTextContent("TEST!")
          e.appendChild(d)
          root.appendChild(e)
          expectedRoot.appendChild(expectedDoc.importNode(e, true))

          val expectedXmlString = prettifyXml(expectedDoc)

          removeEmptyChildren(root)

          prettifyXml(doc) should be(expectedXmlString)
        }
      }

      describe("when given a more complex tree containing empty elements") {
        it("should remove the empty elements") {
          val inExpectedXml: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/removeEmptyElementsExpectedOutput.xml"
          )
          val expectedXml     = scala.io.Source.fromInputStream(inExpectedXml).mkString
          val documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder()
          val expectedTree    = documentBuilder.parse(new InputSource(new StringReader(expectedXml)))
          expectedTree.getDocumentElement.normalize()
          val expectedOutput = prettifyXml(expectedTree).replaceAll("\\s+", "")

          val inXmlStream: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/removeEmptyElementsInput.xml"
          )
          val inXml     = scala.io.Source.fromInputStream(inXmlStream).mkString
          val inputTree = documentBuilder.parse(new InputSource(new StringReader(inXml)))
          inputTree.getDocumentElement.normalize()

          removeEmptyChildren(inputTree.getDocumentElement)

          prettifyXml(inputTree).replaceAll("\\s+", "") should be(expectedOutput)
        }
      }
    }
  }
}
