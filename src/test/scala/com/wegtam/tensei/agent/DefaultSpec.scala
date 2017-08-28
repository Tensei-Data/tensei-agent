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

import org.dfasdl.utils.AttributeNames
import org.scalatest.{ FunSpec, Matchers }
import org.w3c.dom.Element

abstract class DefaultSpec extends FunSpec with Matchers with XmlTestHelpers {

  /**
    * Helper function to compare xml structure trees.
    *
    * @param expectedNodes List of expected nodes.
    * @param actualNodes List of actual nodes.
    * @return Returns `true` if there are no differences.
    */
  def compareXmlStructureNodes(expectedNodes: List[Element], actualNodes: List[Element]): Boolean = {
    (expectedNodes, actualNodes).zipped.map {
      case (e, a: Element) =>
        withClue(s"Comparing ${xmlToPrettyString(e)} to ${xmlToPrettyString(a)}: ") {
          withClue(s"Number of attributes for ${a.getTagName} (${a.getAttribute("id")})") {
            e.getAttributes.getLength should be(a.getAttributes.getLength)
          }
          var i = 0
          while (i < e.getAttributes.getLength) {
            val attrName = e.getAttributes.item(i).getNodeName
            withClue(s"Attribute $attrName should be present") {
              a.hasAttribute(attrName) should be(true)
            }
            if (attrName != AttributeNames.STORAGE_PATH)
              withClue(s"Value of attribute $attrName should be equal") {
                a.getAttribute(attrName) should be(e.getAttribute(attrName))
              }
            i += 1
          }
        }
    }
    true // If there was no test error until here we should be able to safely return `true`.
  }
}
