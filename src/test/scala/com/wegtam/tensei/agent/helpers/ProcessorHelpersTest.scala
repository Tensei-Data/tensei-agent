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

import com.wegtam.tensei.agent.DefaultSpec

class ProcessorHelpersTest extends DefaultSpec with ProcessorHelpers {
  describe("ProcessorHelpers") {
    describe("convertContainerElementToVectorMap") {
      it("should create a proper map") {
        val sourceDataXml = scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/helpers/sequence-data.xml")
          )
          .mkString
        val doc       = createNormalizedDocument(xml = sourceDataXml)
        val container = doc.getElementById("rows")

        val result = convertContainerElementToVectorMap(containerElement = container)

        result.size should be(3)
        result.contains("firstname") should be(true)
        result("firstname").size should be(5)
        result("firstname")(0) should be(container)
        result("lastname").size should be(5)
        result("lastname")(0) should be(container)
        result("email").size should be(5)
        result("email")(0) should be(container)
      }
    }
  }
}
