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

import org.dfasdl.utils.ElementNames
import com.wegtam.tensei.agent.DefaultSpec
import com.wegtam.tensei.agent.helpers.XmlHelpers

class BaseParserStateTest extends DefaultSpec with XmlHelpers {
  describe("BaseParserState") {
    describe("isInChoice") {
      describe("when the stack is empty") {
        it("should return false") {
          val state = new BaseParserState
          state.isInChoice should be(false)
        }
      }

      describe("when the stack has no choice elements") {
        it("should return false") {
          val state = new BaseParserState

          val doc = createNewDocument()
          val a   = doc.createElement(ElementNames.ELEMENT)
          a.setAttribute("id", "1")
          state.add(a)

          val b = doc.createElement(ElementNames.ELEMENT)
          b.setAttribute("id", "2")
          state.add(b)

          val c = doc.createElement(ElementNames.ELEMENT)
          c.setAttribute("id", "3")
          state.add(c)

          state.isInChoice should be(false)
        }
      }

      describe("when the stack contains a choice element") {
        it("should return true") {
          val state = new BaseParserState

          val doc = createNewDocument()
          val a   = doc.createElement(ElementNames.ELEMENT)
          a.setAttribute("id", "1")
          state.add(a)

          val b = doc.createElement(ElementNames.CHOICE)
          b.setAttribute("id", "2")
          state.add(b)

          val c = doc.createElement(ElementNames.ELEMENT)
          c.setAttribute("id", "3")
          state.add(c)

          state.isInChoice should be(true)
        }
      }

      describe("when the stack contains multiple choice elements") {
        it("should return true") {
          val state = new BaseParserState

          val doc = createNewDocument()
          val a   = doc.createElement(ElementNames.ELEMENT)
          a.setAttribute("id", "1")
          state.add(a)

          val b = doc.createElement(ElementNames.CHOICE)
          b.setAttribute("id", "2")
          state.add(b)

          val c = doc.createElement(ElementNames.CHOICE)
          c.setAttribute("id", "3")
          state.add(c)

          state.isInChoice should be(true)
        }
      }
    }
  }
}
