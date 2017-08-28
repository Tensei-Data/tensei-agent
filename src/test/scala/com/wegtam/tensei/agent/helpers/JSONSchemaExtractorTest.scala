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

import java.io.{ File, InputStream }

import com.wegtam.tensei.agent.SchemaExtractor.{ ExtractorMetaData, FormatsFormattime }
import com.wegtam.tensei.agent.XmlTestHelpers
import org.scalatest.{ FunSpec, Matchers }

class JSONSchemaExtractorTest
    extends FunSpec
    with Matchers
    with JSONSchemaExtractor
    with XmlHelpers
    with XmlTestHelpers {
  val formatsFormattime = FormatsFormattime(
    timestamp = List(
      "yyyy-MM-dd h:mm:ss a",
      "yyyy-MM-dd h:mm:ss a z",
      "EEE, dd LLL yyyy HH:mm:ss z"
    ),
    date = List(
      "yyyyMMdd",
      "dd.MM.yyyy",
      "dd MM yyyy",
      "dd.LLL.yyyy",
      "dd LLL yyyy",
      "dd/MM/yyyy",
      "dd/LLL/yyyy",
      "MM/dd/yyyy",
      "LLL/dd/yyyy"
    ),
    time = List(
      "h:mm a",
      "HH:mm"
    )
  )

  val extractorMetaData = ExtractorMetaData(
    dfasdlNamePart = "CHANGEME",
    formatsFormattime = Option(formatsFormattime)
  )

  describe("JSONSchemaExtractor") {
    describe("examples") {
      describe("a simple object") {
        it("should create a valid DFASDL") {
          val jsonFile       = "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-1.json"
          val sourceFilePath = getClass.getResource(jsonFile).toURI
          val file           = new File(sourceFilePath)

          val dfasdl = extractFromLocalJSON(file, extractorMetaData)

          DFASDLValidator.validateString(dfasdl.get.content)

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-1-dfasdl.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString
          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
        }
      }

      describe("a simple array") {
        it("should create a valid DFASDL") {
          val jsonFile       = "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-2.json"
          val sourceFilePath = getClass.getResource(jsonFile).toURI
          val file           = new File(sourceFilePath)

          val dfasdl = extractFromLocalJSON(file, extractorMetaData)

          DFASDLValidator.validateString(dfasdl.get.content)

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-2-dfasdl.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString
          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
        }
      }

      describe("a simple object and array") {
        it("should create a valid DFASDL") {
          val jsonFile       = "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-3.json"
          val sourceFilePath = getClass.getResource(jsonFile).toURI
          val file           = new File(sourceFilePath)

          val dfasdl = extractFromLocalJSON(file, extractorMetaData)

          DFASDLValidator.validateString(dfasdl.get.content)

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-3-dfasdl.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
        }
      }

      describe("JS definition of a widget") {
        it("should create a valid DFASDL") {
          val jsonFile       = "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-4.json"
          val sourceFilePath = getClass.getResource(jsonFile).toURI
          val file           = new File(sourceFilePath)

          val dfasdl = extractFromLocalJSON(file, extractorMetaData)

          DFASDLValidator.validateString(dfasdl.get.content)

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-4-dfasdl.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
        }
      }

      describe("A simple element after a sequence") {
        it("should create a valid DFASDL") {
          val jsonFile       = "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-5.json"
          val sourceFilePath = getClass.getResource(jsonFile).toURI
          val file           = new File(sourceFilePath)

          val dfasdl = extractFromLocalJSON(file, extractorMetaData)

          DFASDLValidator.validateString(dfasdl.get.content)

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-5-dfasdl.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
        }
      }

      describe("Arrays in an object") {
        it("should create a valid DFASDL") {
          val jsonFile       = "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-6.json"
          val sourceFilePath = getClass.getResource(jsonFile).toURI
          val file           = new File(sourceFilePath)

          val dfasdl = extractFromLocalJSON(file, extractorMetaData)

          DFASDLValidator.validateString(dfasdl.get.content)

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-6-dfasdl.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
        }
      }

      describe("Compressed JSON") {
        it("should create a valid DFASDL") {
          val jsonFile       = "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-7.json"
          val sourceFilePath = getClass.getResource(jsonFile).toURI
          val file           = new File(sourceFilePath)

          val dfasdl = extractFromLocalJSON(file, extractorMetaData)

          DFASDLValidator.validateString(dfasdl.get.content)

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-7-dfasdl.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
        }
      }

      describe("with `formattime` elements that must be detected") {
        it("should determine the correct `formattime` formats") {
          val jsonFile       = "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-3.json"
          val sourceFilePath = getClass.getResource(jsonFile).toURI
          val file           = new File(sourceFilePath)

          val dfasdl = extractFromLocalJSON(file, extractorMetaData)

          DFASDLValidator.validateString(dfasdl.get.content)

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/jsonSchemaExtractor/example-3-dfasdl.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
        }
      }
    }
  }
}
