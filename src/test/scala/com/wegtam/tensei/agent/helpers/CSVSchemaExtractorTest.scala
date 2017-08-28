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

import com.wegtam.tensei.adt.ExtractSchemaOptions
import com.wegtam.tensei.agent.SchemaExtractor.{ ExtractorMetaData, FormatsFormattime }
import com.wegtam.tensei.agent.XmlTestHelpers
import org.scalatest.{ FunSpec, Matchers }

class CSVSchemaExtractorTest
    extends FunSpec
    with Matchers
    with CSVSchemaExtractor
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

  describe("CSVSchemaExtractor") {
    describe("get schema") {
      describe("without header") {
        describe("with comma") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-comma.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, "", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-comma.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with an entry of string in numeric row") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-comma-and-error.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, "", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-comma-and-error.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with semicolon") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-semicolon.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, ";", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-semicolon.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with space") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-space.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, " ", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-space.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with tab") {
          it("should create the DFASDL") {
            val csvFile        = "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-tab.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false,
                                                                   "\\t",
                                                                   ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-tab.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with double quotes at the entries") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-comma-and-dquotes.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-comma-and-dquotes.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with simple quotes at the entries") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-comma-and-squotes.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-comma-and-squotes.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with double values") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-comma-and-double-values.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-comma-and-double-values.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with double values that have a minus sign") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-comma-and-double-values-and-minus.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-comma-and-double-values-and-minus.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }
      }
      describe("with header") {
        describe("with comma") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true, "", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with a string entry in a numeric row") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-error.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true, "", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-error.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with semicolon") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-semicolon.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true, ";", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-semicolon.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with semicolon 2") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-semicolon2.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true, ";", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-semicolon2.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with space") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-space.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true, " ", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-space.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with tab") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-tab.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl = extractFromCSV(file,
                                        ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true,
                                                                              "\\t",
                                                                              ""),
                                        extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-tab.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with double quotes at the entries") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-dquotes-entries.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-dquotes-entries.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with simple quotes at the entries") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-squotes-entries.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-squotes-entries.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with double quotes at the headers") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-dquotes-header.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-dquotes-header.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with simple quotes at the headers") {
          it("should create the DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-squotes-header.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-squotes-header.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }
      }

      describe("with Date / Time / Timestamp") {
        describe("with date values") {
          it("should create a correct DFSADL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-date.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, "", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-date.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with Time values") {
          it("should create a correct DFSADL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-time.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, "", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-time.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with Timestamp values") {
          describe("in ISO form") {
            it("should create a correct DFSADL") {
              val csvFile =
                "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-timestamp-iso.csv"
              val sourceFilePath = getClass.getResource(csvFile).toURI
              val file           = new File(sourceFilePath)

              val dfasdl = extractFromCSV(file,
                                          ExtractSchemaOptions.createCsvOptions(hasHeaderLine =
                                                                                  false,
                                                                                "",
                                                                                ""),
                                          extractorMetaData)

              val in: InputStream = getClass.getResourceAsStream(
                "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-timestamp-iso.xml"
              )
              val xmlExpected = scala.io.Source.fromInputStream(in).mkString

              dfasdl.get.content.replaceAll("\\s+", "") should be(
                xmlExpected.replaceAll("\\s+", "")
              )
            }
          }

          describe("not in ISO form") {
            it("should create a correct DFSADL") {
              val csvFile =
                "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-timestamp-noiso.csv"
              val sourceFilePath = getClass.getResource(csvFile).toURI
              val file           = new File(sourceFilePath)

              val dfasdl = extractFromCSV(file,
                                          ExtractSchemaOptions.createCsvOptions(hasHeaderLine =
                                                                                  false,
                                                                                "",
                                                                                ""),
                                          extractorMetaData)

              val in: InputStream = getClass.getResourceAsStream(
                "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-timestamp-noiso.xml"
              )
              val xmlExpected = scala.io.Source.fromInputStream(in).mkString

              dfasdl.get.content.replaceAll("\\s+", "") should be(
                xmlExpected.replaceAll("\\s+", "")
              )
            }
          }
        }

        describe("with Date and Timestamp values") {
          it("should create a correct DFSADL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-date-and-timestamp.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, "", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-date-and-timestamp.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with Date, Time and Timestamp values") {
          it("should create a correct DFSADL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-date-time-and-timestamp.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, "", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-date-time-and-timestamp.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }
      }

      describe("with numerical values that start with zeros") {
        it("should create string entries for these values") {
          val csvFile =
            "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-numerical-start-with-zero.csv"
          val sourceFilePath = getClass.getResource(csvFile).toURI
          val file           = new File(sourceFilePath)

          val dfasdl =
            extractFromCSV(file,
                           ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, "", ""),
                           extractorMetaData)

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-numerical-start-with-zero.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
        }
      }

      describe("with numerical values that start with zeros and are really small") {
        it("should create formatnum entries for these values") {
          val csvFile =
            "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-numerical-start-with-zero-and-are-small.csv"
          val sourceFilePath = getClass.getResource(csvFile).toURI
          val file           = new File(sourceFilePath)

          val dfasdl =
            extractFromCSV(file,
                           ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, ";", ""),
                           extractorMetaData)

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-numerical-start-with-zero-and-are-small.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
        }
      }

      describe(
        "with numerical values that start with zeros and are really small and have ⎖ as decimal separator"
      ) {
        it("should create formatnum entries for these values") {
          val csvFile =
            "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-numerical-start-with-zero-and-are-small-and-special-separator.csv"
          val sourceFilePath = getClass.getResource(csvFile).toURI
          val file           = new File(sourceFilePath)

          val dfasdl =
            extractFromCSV(file,
                           ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, ";", ""),
                           extractorMetaData)

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-numerical-start-with-zero-and-are-small-and-special-separator.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
        }
      }

      describe("with numerical values that have diverse decimal separators") {
        it("should create string entries for these values") {
          val csvFile =
            "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-numerical-and-diverse-separators.csv"
          val sourceFilePath = getClass.getResource(csvFile).toURI
          val file           = new File(sourceFilePath)

          val dfasdl =
            extractFromCSV(file,
                           ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, ";", ""),
                           extractorMetaData)

          val in: InputStream = getClass.getResourceAsStream(
            "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-numerical-and-diverse-separators.xml"
          )
          val xmlExpected = scala.io.Source.fromInputStream(in).mkString

          dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
        }
      }

      describe("with null values in specific columns") {
        describe("with null entries in the last column") {
          it("should create a correct DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-null-at-end.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-null-at-end.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with two null entry columns at the end") {
          it("should create a correct DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-two-null-at-end.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-two-null-at-end.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with two null entry value columns before the last column") {
          it("should create a correct DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-two-null-before-end.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-two-null-before-end.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }

        describe("with a null value entry column at the beginning") {
          it("should create a correct DFASDL") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-null-at-start-and-end.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = true, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/complex-with-comma-and-null-at-start-and-end.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }
      }

      describe("with timestamp, date and time values that are not ISO conform but can be detected") {
        describe("timestamp") {
          it("should create a DFASDL with specific `formattime` elements") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-timestamp-formats.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-timestamp-formats.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }
        describe("multiple timestamp") {
          it("should create a DFASDL with specific `formattime` elements") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-multiple-timestamp-formats.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, "", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-multiple-timestamp-formats.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }
        describe("multiple date") {
          it("should create a DFASDL with specific `formattime` elements") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-multiple-date-formats.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-multiple-date-formats.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }
        describe("multiple time") {
          it("should create a DFASDL with specific `formattime` elements") {
            val csvFile =
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-multiple-time-formats.csv"
            val sourceFilePath = getClass.getResource(csvFile).toURI
            val file           = new File(sourceFilePath)

            val dfasdl =
              extractFromCSV(file,
                             ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, ",", ""),
                             extractorMetaData)

            val in: InputStream = getClass.getResourceAsStream(
              "/com/wegtam/tensei/agent/helpers/csvSchemaExtractor/simple-with-multiple-time-formats.xml"
            )
            val xmlExpected = scala.io.Source.fromInputStream(in).mkString

            dfasdl.get.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
          }
        }
      }
    }
  }
}
