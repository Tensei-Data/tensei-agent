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

import java.nio.file.{ Files, StandardCopyOption }
import java.io.InputStream
import org.dfasdl.utils.exceptions.{ XmlValidationErrorException, XmlValidationFatalException }
import com.wegtam.tensei.agent.DefaultSpec

class DFASDLValidator$Test extends DefaultSpec {
  describe("DFASDLValidator") {
    describe("validateString") {
      describe("when given an empty String") {
        it("should throw a SAXParseException") {
          intercept[XmlValidationFatalException] {
            DFASDLValidator.validateString("")
          }
        }
      }

      describe("when given a minimal valid dfasdl") {
        it("should be valid") {
          val xml =
            """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"></dfasdl>"""
          DFASDLValidator.validateString(xml)
        }
      }
    }

    describe("validate") {
      it("must validate the local file") {
        val filename = Files.createTempFile("test-", ".xml")
        val in: InputStream = getClass.getResourceAsStream(
          "/com/wegtam/tensei/agent/middleware/complex-definition.xml"
        )
        Files.copy(in, filename, StandardCopyOption.REPLACE_EXISTING)
        DFASDLValidator.validate(filename.toFile)
      }

      it("must throw an exception when it does not validate") {
        intercept[XmlValidationErrorException] {
          val filename = Files.createTempFile("test-", ".xml")
          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/middleware/complex-definition-wrong.xml"
          )
          Files.copy(in, filename, StandardCopyOption.REPLACE_EXISTING)
          DFASDLValidator.validate(filename.toFile)
        }
      }
    }

    describe("validateLocal") {
      it("must validate the local file") {
        DFASDLValidator.validateLocal("/com/wegtam/tensei/agent/middleware/complex-definition.xml")
      }

      it("must throw an exception when it does not validate") {
        intercept[XmlValidationErrorException] {
          DFASDLValidator.validateLocal(
            "/com/wegtam/tensei/agent/middleware/complex-definition-wrong.xml"
          )
        }
      }
    }
  }
}
