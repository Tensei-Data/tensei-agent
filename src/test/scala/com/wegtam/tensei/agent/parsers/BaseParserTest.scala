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

import java.io.{ InputStream, StringReader }

import com.wegtam.tensei.agent.DefaultSpec
import com.wegtam.tensei.agent.adt.{
  BaseParserResponse,
  BaseParserResponseStatus,
  ParserDataContainer
}
import org.dfasdl.utils.DataElementType
import org.scalatest.BeforeAndAfterEach
import org.w3c.dom.Element
import org.xml.sax.InputSource

class BaseParserTest extends DefaultSpec with BaseParser with BeforeAndAfterEach {
  var saveCalls = 0

  /**
    * Reset our save call counter and the parser state.
    */
  override def beforeEach(): Unit = {
    saveCalls = 0
    state.reset()
  }

  /**
    * We override the method for testing.
    *
    * @param structureElement The data description.
    * @param useOffset An optional offset to use.
    * @param isInChoice Determines if we are within a choice.
    * @return A base parser response holding the data.
    */
  override def readDataElement(structureElement: Element,
                               useOffset: Long,
                               isInChoice: Boolean): BaseParserResponse =
    getDataElementType(structureElement.getTagName) match {
      case DataElementType.BinaryDataElement =>
        BaseParserResponse(Option(new Array[Byte](0)), DataElementType.BinaryDataElement)
      case DataElementType.StringDataElement =>
        structureElement.getAttribute("id") match {
          case "t1-a-endless" =>
            if (saveCalls < 6)
              BaseParserResponse(Option("YEAH!"), DataElementType.StringDataElement)
            else
              BaseParserResponse(None,
                                 DataElementType.StringDataElement,
                                 0L,
                                 BaseParserResponseStatus.END_OF_SEQUENCE)
          case "t2-a-endless" =>
            BaseParserResponse(None,
                               DataElementType.StringDataElement,
                               0L,
                               BaseParserResponseStatus.END_OF_SEQUENCE)
          case "t3-a-endless" =>
            if (saveCalls < 13)
              BaseParserResponse(Option("YEAH!"), DataElementType.StringDataElement)
            else
              BaseParserResponse(None,
                                 DataElementType.StringDataElement,
                                 0L,
                                 BaseParserResponseStatus.END_OF_SEQUENCE)
          case _ => BaseParserResponse(Option("YEAH!"), DataElementType.StringDataElement)

        }
      case DataElementType.UnknownElement =>
        throw new RuntimeException(s"Unknown element type: ${structureElement.getTagName}")
    }

  def save(data: ParserDataContainer, dataHash: Long, referenceId: Option[String] = None): Unit =
    saveCalls += 1

  describe("BaseParser") {
    describe("extractDataUsingStopSign") {
      val source =
        """I am just a random collection of characters.
          |I will always support the test case of my developer!
          |
          |This is my signature...""".stripMargin

      describe("using no stop sign") {
        it("should return the source string") {
          extractDataUsingStopSign(source, "") should be(Option(source))
        }
      }

      describe("using a single stop sign") {
        describe("which is a 'normal' character") {
          describe("that matches") {
            it("should return the correct subset of the source") {
              extractDataUsingStopSign(source, "d") should be(Option("I am just a ran"))
            }
          }

          describe("that doesn't match") {
            it("should return None") {
              extractDataUsingStopSign(source, ":") should be(None)
            }
          }
        }

        describe("which is a 'special' character") {
          describe("that matches") {
            it("should return the correct subset of the source") {
              extractDataUsingStopSign(source, "\n") should be(
                Option("I am just a random collection of characters.")
              )
            }
          }

          describe("that doesn't match") {
            it("should return None") {
              extractDataUsingStopSign(source, "\t") should be(None)
            }
          }
        }
      }

      describe("usign a group stop sign") {
        describe("that matches") {
          it("should return the correct subset of the source") {
            extractDataUsingStopSign(source, "[\r\n]") should be(
              Option("I am just a random collection of characters.")
            )
          }
        }

        describe("that doesn't match") {
          it("should return None") {
            extractDataUsingStopSign(source, "[\t\r]") should be(None)
          }
        }
      }

      describe("usign a word stop sign") {
        describe("that matches") {
          it("should return the correct subset of the source") {
            extractDataUsingStopSign(source, "characters") should be(
              Option("I am just a random collection of ")
            )
          }
        }

        describe("that doesn't match") {
          it("should return None") {
            extractDataUsingStopSign(source, "fancyShit") should be(None)
          }
        }
      }
    }

    describe("when traversing") {
      describe("a simple structure") {
        it("should create the elements") {
          val in: InputStream =
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/BaseParserTest-01.xml")
          val xml = scala.io.Source.fromInputStream(in).mkString

          val builder  = createDocumentBuilder()
          val document = builder.parse(new InputSource(new StringReader(xml)))
          document.getDocumentElement.normalize()

          traverseTree(document, null)

          withClue("Function for saving the data should only be called six times!") {
            saveCalls should be(6)
          }
          withClue("The element stack should be empty.") {
            state.elementStack.isEmpty should be(true)
          }
          withClue("The sequence element stack should be empty.") {
            state.sequenceStack.isEmpty should be(true)
          }
        }
      }

      describe("a simple structure with surrounding element") {
        it("should create the elements with surrounding element") {
          val in: InputStream =
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/BaseParserTest-02.xml")
          val xml = scala.io.Source.fromInputStream(in).mkString

          val builder  = createDocumentBuilder()
          val document = builder.parse(new InputSource(new StringReader(xml)))
          document.getDocumentElement.normalize()

          traverseTree(document, null)

          withClue(
            "Function for saving the data should only be called once for each data element!"
          ) { saveCalls should be(6) }
          withClue("The element stack should be empty.") {
            state.elementStack.isEmpty should be(true)
          }
          withClue("The sequence element stack should be empty.") {
            state.sequenceStack.isEmpty should be(true)
          }
        }
      }

      describe("a simple structure with more surrounding elements") {
        it("should create the elements with surrounding elements") {
          val in: InputStream =
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/BaseParserTest-03.xml")
          val xml = scala.io.Source.fromInputStream(in).mkString

          val builder  = createDocumentBuilder()
          val document = builder.parse(new InputSource(new StringReader(xml)))
          document.getDocumentElement.normalize()

          traverseTree(document, null)

          withClue("Function for saving the data should only be called six times!") {
            saveCalls should be(6)
          }
          withClue("The element stack should be empty.") {
            state.elementStack.isEmpty should be(true)
          }
          withClue("The sequence element stack should be empty.") {
            state.sequenceStack.isEmpty should be(true)
          }
        }
      }

      describe("a simple tree containing a sequence") {
        it("should walk from the top to the bottom") {
          val in: InputStream =
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/BaseParserTest-04.xml")
          val xml = scala.io.Source.fromInputStream(in).mkString

          val builder  = createDocumentBuilder()
          val document = builder.parse(new InputSource(new StringReader(xml)))
          document.getDocumentElement.normalize()

          traverseTree(document, null)

          withClue("Function for saving the data should only be called for each data element!") {
            saveCalls should be(18)
          }
          withClue("The element stack should be empty.") {
            state.elementStack.isEmpty should be(true)
          }
          withClue("The sequence element stack should be empty.") {
            state.sequenceStack.isEmpty should be(true)
          }
        }
      }

      describe("a more complex tree containing a sequence") {
        it("should walk from the top to the bottom") {
          val in: InputStream =
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/BaseParserTest-05.xml")
          val xml = scala.io.Source.fromInputStream(in).mkString

          val builder  = createDocumentBuilder()
          val document = builder.parse(new InputSource(new StringReader(xml)))
          document.getDocumentElement.normalize()

          traverseTree(document, null)

          withClue("Function for saving the data should only be called for each data element!") {
            saveCalls should be(18)
          }
          withClue("The element stack should be empty.") {
            state.elementStack.isEmpty should be(true)
          }
          withClue("The sequence element stack should be empty.") {
            state.sequenceStack.isEmpty should be(true)
          }
        }
      }

      describe("a more complex tree containing a sequence within an element") {
        it("should walk from the top to the bottom") {
          val in: InputStream =
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/BaseParserTest-06.xml")
          val xml = scala.io.Source.fromInputStream(in).mkString

          val builder  = createDocumentBuilder()
          val document = builder.parse(new InputSource(new StringReader(xml)))
          document.getDocumentElement.normalize()

          traverseTree(document, null)

          withClue("Function for saving the data should only be called for each data element!") {
            saveCalls should be(18)
          }
          withClue("The element stack should be empty.") {
            state.elementStack.isEmpty should be(true)
          }
          withClue("The sequence element stack should be empty.") {
            state.sequenceStack.isEmpty should be(true)
          }
        }
      }

      describe("a more complex tree containing a sequence using stacked elements") {
        it("should walk from the top to the bottom") {
          val in: InputStream =
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/BaseParserTest-07.xml")
          val xml = scala.io.Source.fromInputStream(in).mkString

          val builder  = createDocumentBuilder()
          val document = builder.parse(new InputSource(new StringReader(xml)))
          document.getDocumentElement.normalize()

          traverseTree(document, null)

          withClue("Function for saving the data should only be called for each data element!") {
            saveCalls should be(18)
          }
          withClue("The element stack should be empty.") {
            state.elementStack.isEmpty should be(true)
          }
          withClue("The sequence element stack should be empty.") {
            state.sequenceStack.isEmpty should be(true)
          }
        }
      }

      describe(
        "a more complex tree containing a sequence with multiple children using stacked elements"
      ) {
        it("should walk from the top to the bottom") {
          val in: InputStream =
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/BaseParserTest-08.xml")
          val xml = scala.io.Source.fromInputStream(in).mkString

          val builder  = createDocumentBuilder()
          val document = builder.parse(new InputSource(new StringReader(xml)))
          document.getDocumentElement.normalize()

          traverseTree(document, null)

          withClue("Function for saving the data should only be called for each data element!") {
            saveCalls should be(27)
          }
          withClue("The element stack should be empty.") {
            state.elementStack.isEmpty should be(true)
          }
          withClue("The sequence element stack should be empty.") {
            state.sequenceStack.isEmpty should be(true)
          }
        }
      }

      describe(
        "a more complex tree containing a sequence with multiple children using stacked elements followed by simple element"
      ) {
        it("should walk from the top to the bottom") {
          val in: InputStream =
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/BaseParserTest-09.xml")
          val xml = scala.io.Source.fromInputStream(in).mkString

          val builder  = createDocumentBuilder()
          val document = builder.parse(new InputSource(new StringReader(xml)))
          document.getDocumentElement.normalize()

          traverseTree(document, null)

          withClue("Function for saving the data should only be called for each data element!") {
            saveCalls should be(28)
          }
          withClue("The element stack should be empty.") {
            state.elementStack.isEmpty should be(true)
          }
          withClue("The sequence element stack should be empty.") {
            state.sequenceStack.isEmpty should be(true)
          }
        }
      }

      describe("a tree containing three fixed sequences followed by a simple element") {
        it("should walk from the top to the bottom") {
          val in: InputStream =
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/BaseParserTest-10.xml")
          val xml = scala.io.Source.fromInputStream(in).mkString

          val builder  = createDocumentBuilder()
          val document = builder.parse(new InputSource(new StringReader(xml)))
          document.getDocumentElement.normalize()

          traverseTree(document, null)

          // FIXME Currently the save function is called for the first column of an empty sequence. Therefore the save calls should be 13 not 16!
          withClue("Function for saving the data should only be called for each data element!") {
            saveCalls should be(16)
          }
          withClue("The element stack should be empty.") {
            state.elementStack.isEmpty should be(true)
          }
          withClue("The sequence element stack should be empty.") {
            state.sequenceStack.isEmpty should be(true)
          }
        }
      }
    }
  }
}
