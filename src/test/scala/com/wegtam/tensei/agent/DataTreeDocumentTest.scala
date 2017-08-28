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

import java.io.{ InputStream, StringReader }
import javax.xml.parsers.DocumentBuilderFactory

import akka.testkit.{ TestActorRef, TestFSMRef }
import com.wegtam.tensei.adt.{ DFASDL, ElementReference, StatusMessage, StatusType }
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages._
import com.wegtam.tensei.agent.DataTreeDocument.{ DataTreeDocumentMessages, DataTreeDocumentState }
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages.FoundContent
import com.wegtam.tensei.agent.adt.ParserDataContainer
import com.wegtam.tensei.agent.helpers.GenericHelpers
import org.xml.sax.InputSource

class DataTreeDocumentTest extends XmlActorSpec with XmlTestHelpers with GenericHelpers {
  describe("DataTreeDocument") {
    describe("when initialized with the smallest possible dfasdl") {
      it("should create a DOMTree") {
        val xml =
          """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"></dfasdl>"""
        val dfasdl = DFASDL("ID", xml)

        val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))
        val documentBuilder  = DocumentBuilderFactory.newInstance().newDocumentBuilder()
        val expectedTree     = documentBuilder.parse(new InputSource(new StringReader(xml)))
        expectedTree.getDocumentElement.normalize()

        dataTreeDocument.underlyingActor.dfasdlTree should not be null
        dataTreeDocument.underlyingActor.dfasdlTree.getDocumentElement.getNodeName should be(
          expectedTree.getDocumentElement.getNodeName
        )
      }

      describe("when send ReturnXML") {
        it("should return the dfasdl tree of the document") {
          val xml =
            """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"></dfasdl>"""
          val dfasdl = DFASDL("ID", xml)

          val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))
          val documentBuilder  = DocumentBuilderFactory.newInstance().newDocumentBuilder()
          val expectedTree     = documentBuilder.parse(new InputSource(new StringReader(xml)))
          expectedTree.getDocumentElement.normalize()

          dataTreeDocument ! ReturnXmlStructure

          val msg = expectMsgType[XmlStructure]

          msg.dfasdlId should be(dfasdl.id)
          xmlToPrettyString(msg.document) should be(xmlToPrettyString(expectedTree))
        }
      }
    }

    describe("when initialized with a simple dfasdl") {
      it("should create the correct DOM tree") {
        val in: InputStream =
          getClass.getResourceAsStream("/com/wegtam/tensei/agent/DataTreeDocument-simple.xml")
        val xml    = scala.io.Source.fromInputStream(in).mkString
        val dfasdl = DFASDL("ID", xml)

        val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))

        val documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder()
        val expectedTree    = documentBuilder.parse(new InputSource(new StringReader(xml)))
        expectedTree.getDocumentElement.normalize()

        val expectedNodes = getNodeList(expectedTree)
        val actualNodes   = getNodeList(dataTreeDocument.underlyingActor.dfasdlTree)

        dataTreeDocument.underlyingActor.dfasdlTree should not be null
        compareXmlStructureNodes(expectedNodes, actualNodes)
      }
    }

    describe("when initialized with a basic complex dfasdl") {
      it("should create the correct DOM tree") {
        val in: InputStream = getClass.getResourceAsStream(
          "/com/wegtam/tensei/agent/DataTreeDocument-basic-complex.xml"
        )
        val xml    = scala.io.Source.fromInputStream(in).mkString
        val dfasdl = DFASDL("ID", xml)

        val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))

        val documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder()
        val inExpected: InputStream = getClass.getResourceAsStream(
          "/com/wegtam/tensei/agent/DataTreeDocument-basic-complex-expected.xml"
        )
        val xmlExpected  = scala.io.Source.fromInputStream(inExpected).mkString
        val expectedTree = documentBuilder.parse(new InputSource(new StringReader(xmlExpected)))
        expectedTree.getDocumentElement.normalize()

        val expectedNodes = getNodeList(expectedTree)
        val actualNodes   = getNodeList(dataTreeDocument.underlyingActor.dfasdlTree)

        dataTreeDocument.underlyingActor.dfasdlTree should not be null
        compareXmlStructureNodes(expectedNodes, actualNodes)
      }
    }

    describe("when receiving FindDataContainer") {
      describe("for an unknown element") {
        it("should return an error message") {
          val xml =
            """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"></dfasdl>"""
          val dfasdl = DFASDL("ID", xml)

          val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))

          dataTreeDocument ! FindDataContainer("some-ID", "Some Data...")

          val message = expectMsgType[StatusMessage]
          message.message should be("Element with id 'some-ID' not found!")
        }
      }

      describe("for a known element") {
        describe("without data") {
          it("should return an error message") {
            val xml =
              """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><str id="MY-DATA"/></dfasdl>"""
            val dfasdl = DFASDL("ID", xml)

            val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))

            dataTreeDocument ! FindDataContainer("MY-DATA", "Some Data...")

            val message = expectMsgType[StatusMessage]
            message.message should be("No data has been stored yet for 'MY-DATA'!")
          }
        }

        describe("with data") {
          describe("that matches") {
            it("should return the data container") {
              val xml =
                """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><str id="MY-DATA"/></dfasdl>"""
              val dfasdl = DFASDL("ID", xml)
              val data = ParserDataContainer(
                "Some Data...",
                "MY-DATA",
                Option("ID"),
                -1L,
                Option(calculateDataElementStorageHash("MY-DATA", List.empty[(String, Long)]))
              )

              val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))

              dataTreeDocument ! SaveData(
                data,
                calculateDataElementStorageHash(data.elementId, List.empty[(String, Long)])
              )

              dataTreeDocument ! FindDataContainer(data.elementId, data.data)

              val message = expectMsgType[FoundContent]
              message.container should be(data)
            }
          }

          describe("that doesn't match") {
            it("should not return the data container") {
              val xml =
                """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><str id="MY-DATA"/></dfasdl>"""
              val dfasdl = DFASDL("ID", xml)
              val data   = ParserDataContainer("Some Data...", "MY-DATA", Option("ID"))

              val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))

              dataTreeDocument ! SaveData(
                data,
                calculateDataElementStorageHash(data.elementId, List.empty[(String, Long)])
              )

              dataTreeDocument ! FindDataContainer(data.elementId, "The data should not match...")

              val thrown = the[java.lang.AssertionError] thrownBy expectMsgType[FoundContent]
              thrown.getMessage should include("timeout")
            }
          }
        }

        describe("which is the child of a sequence") {
          describe("that has no data") {
            it("should return an error message") {
              val xml =
                """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQUENCE"><elem id="row"><str id="MY-DATA"/></elem></seq></dfasdl>"""
              val dfasdl           = DFASDL("ID", xml)
              val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))

              dataTreeDocument ! FindDataContainer("MY-DATA", "My data row number #5")

              val message = expectMsgType[StatusMessage]
              message.message should be("No data has been stored yet for 'MY-DATA'!")
            }
          }

          describe("that matches") {
            it("should return the data container") {
              val xml =
                """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQUENCE"><elem id="row"><str id="MY-DATA"/></elem></seq></dfasdl>"""
              val dfasdl           = DFASDL("ID", xml)
              val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))

              for (i <- 0L to 10L) {
                val data =
                  ParserDataContainer(s"My data row number #$i", "MY-DATA", Option("ID"), i)
                dataTreeDocument ! SaveData(
                  data,
                  calculateDataElementStorageHash(data.elementId, List(("MY-SEQUENCE", i)))
                )
              }

              dataTreeDocument ! FindDataContainer("MY-DATA", "My data row number #5")

              val message = expectMsgType[FoundContent]
              message.container.data should be("My data row number #5")
              message.container.sequenceRowCounter should be(5)
            }
          }

          describe("that doesn't match") {
            it("should not return the data container") {
              val xml =
                """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQUENCE"><elem id="row"><str id="MY-DATA"/></elem></seq></dfasdl>"""
              val dfasdl           = DFASDL("ID", xml)
              val dataTreeDocument = TestActorRef(new DataTreeDocument(dfasdl))

              for (i <- 0L to 10L) {
                val data =
                  ParserDataContainer(s"My data row number #$i", "MY-DATA", Option("ID"), i)
                dataTreeDocument ! SaveData(
                  data,
                  calculateDataElementStorageHash(data.elementId, List(("MY-SEQUENCE", i)))
                )
              }

              dataTreeDocument ! FindDataContainer("MY-DATA", "My data row number #UNLIMITED")

              val thrown = the[java.lang.AssertionError] thrownBy expectMsgType[
                ParserDataContainer
              ]
              thrown.getMessage should include("timeout")
            }
          }
        }
      }
    }

    describe("when receiving SaveData") {
      describe("with an empty whitelist") {
        describe("for a simple element") {
          it("should create an actor for it") {
            val xml =
              """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><str id="MY-DATA"/></dfasdl>"""
            val dfasdl = DFASDL("ID", xml)

            val dataTreeDocument = TestFSMRef(new DataTreeDocument(dfasdl))
            dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
            dataTreeDocument.children.size should be(0)
            dataTreeDocument ! SaveData(
              ParserDataContainer("FOO", "MY-DATA"),
              calculateDataElementStorageHash("MY-DATA", List.empty[(String, Long)])
            )
            dataTreeDocument.stateName should be(DataTreeDocumentState.Working)
            dataTreeDocument.children.size should be(1)
          }
        }

        describe("for a sequence element") {
          describe("with more than maxSequenceRowsPerActor rows") {
            it("should drop the defined number of rows within one actor") {
              val maxSequenceRowsPerActor =
                system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

              val xml =
                """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="COLUMN-1" stop-sign=","/><str id="COLUMN-2" stop-sign=","/><str id="COLUMN-3"/></elem></seq></dfasdl>"""
              val dfasdl = DFASDL("ID", xml)

              val dataTreeDocument = TestFSMRef(new DataTreeDocument(dfasdl))
              dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
              dataTreeDocument.children.size should be(0)

              for (i <- 0L until maxSequenceRowsPerActor * 4L) {
                dataTreeDocument ! SaveData(
                  ParserDataContainer(s"Column 1, row #$i", "COLUMN-1", Option("ID"), i),
                  calculateDataElementStorageHash("COLUMN-1", List(("MY-SEQ", i)))
                )
                dataTreeDocument ! SaveData(
                  ParserDataContainer(s"Column 2, row #$i", "COLUMN-2", Option("ID"), i),
                  calculateDataElementStorageHash("COLUMN-2", List(("MY-SEQ", i)))
                )
                dataTreeDocument ! SaveData(
                  ParserDataContainer(s"Column 3, row #$i", "COLUMN-3", Option("ID"), i),
                  calculateDataElementStorageHash("COLUMN-3", List(("MY-SEQ", i)))
                )
              }

              dataTreeDocument.children.size should be(4)
            }
          }

          describe("with exactly maxSequenceRowsPerActor rows") {
            it("should drop the defined number of rows within one actor") {
              val maxSequenceRowsPerActor =
                system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

              val xml =
                """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="COLUMN-1" stop-sign=","/><str id="COLUMN-2" stop-sign=","/><str id="COLUMN-3"/></elem></seq></dfasdl>"""
              val dfasdl = DFASDL("ID", xml)

              val dataTreeDocument = TestFSMRef(new DataTreeDocument(dfasdl))
              dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
              dataTreeDocument.children.size should be(0)

              for (i <- 0L until maxSequenceRowsPerActor) {
                dataTreeDocument ! SaveData(
                  ParserDataContainer(s"Column 1, row #$i", "COLUMN-1", Option("ID"), i),
                  calculateDataElementStorageHash("COLUMN-1", List(("MY-SEQ", i)))
                )
                dataTreeDocument ! SaveData(
                  ParserDataContainer(s"Column 2, row #$i", "COLUMN-2", Option("ID"), i),
                  calculateDataElementStorageHash("COLUMN-2", List(("MY-SEQ", i)))
                )
                dataTreeDocument ! SaveData(
                  ParserDataContainer(s"Column 3, row #$i", "COLUMN-3", Option("ID"), i),
                  calculateDataElementStorageHash("COLUMN-3", List(("MY-SEQ", i)))
                )
              }

              dataTreeDocument.children.size should be(1)
            }
          }

          describe("with exactly maxSequenceRowsPerActor + 1 rows") {
            it("should drop the defined number of rows within one actor") {
              val maxSequenceRowsPerActor =
                system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

              val xml =
                """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="COLUMN-1" stop-sign=","/><str id="COLUMN-2" stop-sign=","/><str id="COLUMN-3"/></elem></seq></dfasdl>"""
              val dfasdl = DFASDL("ID", xml)

              val dataTreeDocument = TestFSMRef(new DataTreeDocument(dfasdl))
              dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
              dataTreeDocument.children.size should be(0)

              for (i <- 0L to maxSequenceRowsPerActor) {
                dataTreeDocument ! SaveData(
                  ParserDataContainer(s"Column 1, row #$i", "COLUMN-1", Option("ID"), i),
                  calculateDataElementStorageHash("COLUMN-1", List(("MY-SEQ", i)))
                )
                dataTreeDocument ! SaveData(
                  ParserDataContainer(s"Column 2, row #$i", "COLUMN-2", Option("ID"), i),
                  calculateDataElementStorageHash("COLUMN-2", List(("MY-SEQ", i)))
                )
                dataTreeDocument ! SaveData(
                  ParserDataContainer(s"Column 3, row #$i", "COLUMN-3", Option("ID"), i),
                  calculateDataElementStorageHash("COLUMN-3", List(("MY-SEQ", i)))
                )
              }

              dataTreeDocument.children.size should be(2)
            }
          }
        }
      }

      describe("with a whitelist") {
        describe("for a simple element") {
          describe("that is whitelisted") {
            it("should create an actor for it") {
              val xml =
                """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><str id="MY-DATA"/></dfasdl>"""
              val dfasdl = DFASDL("ID", xml)

              val dataTreeDocument = TestFSMRef(
                new DataTreeDocument(dfasdl, Option("DataTreeDocumentTest"), Set("MY-DATA"))
              )
              dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
              dataTreeDocument.children.size should be(0)
              dataTreeDocument ! SaveData(
                ParserDataContainer("FOO", "MY-DATA"),
                calculateDataElementStorageHash("MY-DATA", List.empty[(String, Long)])
              )
              dataTreeDocument.stateName should be(DataTreeDocumentState.Working)
              dataTreeDocument.children.size should be(1)
            }
          }

          describe("that is not whitelisted") {
            it("should not create an actor for it") {
              val xml =
                """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><str id="MY-DATA"/></dfasdl>"""
              val dfasdl = DFASDL("ID", xml)

              val dataTreeDocument = TestFSMRef(
                new DataTreeDocument(dfasdl, Option("DataTreeDocumentTest"), Set("ANOTHER-DATA"))
              )
              dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
              dataTreeDocument.children.size should be(0)
              dataTreeDocument ! SaveData(
                ParserDataContainer("FOO", "MY-DATA"),
                calculateDataElementStorageHash("MY-DATA", List.empty[(String, Long)])
              )
              dataTreeDocument.stateName should be(DataTreeDocumentState.Working)
              dataTreeDocument.children.size should be(0)
            }
          }
        }

        describe("for a sequence element") {
          describe("that is whitelisted") {
            describe("with more than maxSequenceRowsPerActor rows") {
              it("should drop the defined number of rows within one actor") {
                val maxSequenceRowsPerActor =
                  system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

                val xml =
                  """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="COLUMN-1" stop-sign=","/><str id="COLUMN-2" stop-sign=","/><str id="COLUMN-3"/></elem></seq></dfasdl>"""
                val dfasdl = DFASDL("ID", xml)

                val dataTreeDocument = TestFSMRef(new DataTreeDocument(dfasdl))
                dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
                dataTreeDocument.children.size should be(0)

                for (i <- 0L until maxSequenceRowsPerActor * 4L) {
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 1, row #$i", "COLUMN-1", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-1", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 2, row #$i", "COLUMN-2", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-2", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 3, row #$i", "COLUMN-3", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-3", List(("MY-SEQ", i)))
                  )
                }

                dataTreeDocument.children.size should be(4)

                val row = maxSequenceRowsPerActor * 4 - 1
                dataTreeDocument ! DataTreeDocumentMessages.ReturnData("COLUMN-2", Option(row))
                val rowData = expectMsgType[DataTreeNodeMessages.Content]
                rowData.data.size should be(1)
                rowData.data.head.data should be(s"Column 2, row #$row")
              }
            }

            describe("with exactly maxSequenceRowsPerActor rows") {
              it("should drop the defined number of rows within one actor") {
                val maxSequenceRowsPerActor =
                  system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

                val xml =
                  """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="COLUMN-1" stop-sign=","/><str id="COLUMN-2" stop-sign=","/><str id="COLUMN-3"/></elem></seq></dfasdl>"""
                val dfasdl = DFASDL("ID", xml)

                val dataTreeDocument = TestFSMRef(new DataTreeDocument(dfasdl))
                dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
                dataTreeDocument.children.size should be(0)

                for (i <- 0L until maxSequenceRowsPerActor) {
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 1, row #$i", "COLUMN-1", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-1", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 2, row #$i", "COLUMN-2", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-2", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 3, row #$i", "COLUMN-3", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-3", List(("MY-SEQ", i)))
                  )
                }

                dataTreeDocument.children.size should be(1)

                val row = maxSequenceRowsPerActor - 1
                dataTreeDocument ! DataTreeDocumentMessages.ReturnData("COLUMN-2", Option(row))
                val rowData = expectMsgType[DataTreeNodeMessages.Content]
                rowData.data.size should be(1)
                rowData.data.head.data should be(s"Column 2, row #$row")
              }
            }

            describe("with exactly maxSequenceRowsPerActor + 1 rows") {
              it("should drop the defined number of rows within one actor") {
                val maxSequenceRowsPerActor =
                  system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

                val xml =
                  """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="COLUMN-1" stop-sign=","/><str id="COLUMN-2" stop-sign=","/><str id="COLUMN-3"/></elem></seq></dfasdl>"""
                val dfasdl = DFASDL("ID", xml)

                val dataTreeDocument = TestFSMRef(new DataTreeDocument(dfasdl))
                dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
                dataTreeDocument.children.size should be(0)

                for (i <- 0L to maxSequenceRowsPerActor) {
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 1, row #$i", "COLUMN-1", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-1", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 2, row #$i", "COLUMN-2", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-2", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 3, row #$i", "COLUMN-3", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-3", List(("MY-SEQ", i)))
                  )
                }

                dataTreeDocument.children.size should be(2)

                val row = maxSequenceRowsPerActor
                dataTreeDocument ! DataTreeDocumentMessages.ReturnData("COLUMN-2", Option(row))
                val rowData = expectMsgType[DataTreeNodeMessages.Content]
                rowData.data.size should be(1)
                rowData.data.head.data should be(s"Column 2, row #$row")
              }
            }
          }

          describe("that is partially whitelisted") {
            describe("with more than maxSequenceRowsPerActor rows") {
              it("should drop the defined number of rows within one actor") {
                val maxSequenceRowsPerActor =
                  system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

                val xml =
                  """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="COLUMN-1" stop-sign=","/><str id="COLUMN-2" stop-sign=","/><str id="COLUMN-3"/></elem></seq></dfasdl>"""
                val dfasdl = DFASDL("ID", xml)

                val dataTreeDocument = TestFSMRef(
                  new DataTreeDocument(dfasdl,
                                       Option("DataTreeDocumentTest"),
                                       Set("COLUMN-1", "COLUMN-3"))
                )
                dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
                dataTreeDocument.children.size should be(0)

                for (i <- 0L until maxSequenceRowsPerActor * 4) {
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 1, row #$i", "COLUMN-1", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-1", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 2, row #$i", "COLUMN-2", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-2", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 3, row #$i", "COLUMN-3", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-3", List(("MY-SEQ", i)))
                  )
                }

                dataTreeDocument.children.size should be(4)

                val row = maxSequenceRowsPerActor * 4 - 1
                dataTreeDocument ! DataTreeDocumentMessages.ReturnData("COLUMN-1", Option(row))
                val rowData = expectMsgType[DataTreeNodeMessages.Content]
                rowData.data.size should be(1)
                rowData.data.head.data should be(s"Column 1, row #$row")

                dataTreeDocument ! DataTreeDocumentMessages.ReturnData("COLUMN-2")
                val response = expectMsgType[StatusMessage]
                response.statusType should be(StatusType.FatalError)
                response.message should include regex "No data has been stored"
              }
            }

            describe("with exactly maxSequenceRowsPerActor rows") {
              it("should drop the defined number of rows within one actor") {
                val maxSequenceRowsPerActor =
                  system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

                val xml =
                  """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="COLUMN-1" stop-sign=","/><str id="COLUMN-2" stop-sign=","/><str id="COLUMN-3"/></elem></seq></dfasdl>"""
                val dfasdl = DFASDL("ID", xml)

                val dataTreeDocument = TestFSMRef(
                  new DataTreeDocument(dfasdl,
                                       Option("DataTreeDocumentTest"),
                                       Set("COLUMN-1", "COLUMN-3"))
                )
                dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
                dataTreeDocument.children.size should be(0)

                for (i <- 0L until maxSequenceRowsPerActor) {
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 1, row #$i", "COLUMN-1", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-1", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 2, row #$i", "COLUMN-2", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-2", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 3, row #$i", "COLUMN-3", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-3", List(("MY-SEQ", i)))
                  )
                }

                dataTreeDocument.children.size should be(1)

                val row = maxSequenceRowsPerActor - 1
                dataTreeDocument ! DataTreeDocumentMessages.ReturnData("COLUMN-1", Option(row))
                val rowData = expectMsgType[DataTreeNodeMessages.Content]
                rowData.data.size should be(1)
                rowData.data.head.data should be(s"Column 1, row #$row")

                dataTreeDocument ! DataTreeDocumentMessages.ReturnData("COLUMN-2")
                val response = expectMsgType[StatusMessage]
                response.statusType should be(StatusType.FatalError)
                response.message should include regex "No data has been stored"
              }
            }

            describe("with exactly maxSequenceRowsPerActor + 1 rows") {
              it("should drop the defined number of rows within one actor") {
                val maxSequenceRowsPerActor =
                  system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

                val xml =
                  """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="COLUMN-1" stop-sign=","/><str id="COLUMN-2" stop-sign=","/><str id="COLUMN-3"/></elem></seq></dfasdl>"""
                val dfasdl = DFASDL("ID", xml)

                val dataTreeDocument = TestFSMRef(
                  new DataTreeDocument(dfasdl,
                                       Option("DataTreeDocumentTest"),
                                       Set("COLUMN-1", "COLUMN-3"))
                )
                dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
                dataTreeDocument.children.size should be(0)

                for (i <- 0L to maxSequenceRowsPerActor) {
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 1, row #$i", "COLUMN-1", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-1", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 2, row #$i", "COLUMN-2", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-2", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 3, row #$i", "COLUMN-3", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-3", List(("MY-SEQ", i)))
                  )
                }

                dataTreeDocument.children.size should be(2)

                val row = maxSequenceRowsPerActor
                dataTreeDocument ! DataTreeDocumentMessages.ReturnData("COLUMN-1", Option(row))
                val rowData = expectMsgType[DataTreeNodeMessages.Content]
                rowData.data.size should be(1)
                rowData.data.head.data should be(s"Column 1, row #$row")

                dataTreeDocument ! DataTreeDocumentMessages.ReturnData("COLUMN-2")
                val response = expectMsgType[StatusMessage]
                response.statusType should be(StatusType.FatalError)
                response.message should include regex "No data has been stored"
              }
            }
          }

          describe("that is not whitelisted") {
            describe("with more than maxSequenceRowsPerActor rows") {
              it("should drop the defined number of rows within one actor") {
                val maxSequenceRowsPerActor =
                  system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

                val xml =
                  """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="COLUMN-1" stop-sign=","/><str id="COLUMN-2" stop-sign=","/><str id="COLUMN-3"/></elem></seq></dfasdl>"""
                val dfasdl = DFASDL("ID", xml)

                val dataTreeDocument = TestFSMRef(
                  new DataTreeDocument(dfasdl,
                                       Option("DataTreeDocumentTest"),
                                       Set("NOT-YOUR-COLUMN"))
                )
                dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
                dataTreeDocument.children.size should be(0)

                for (i <- 0L until maxSequenceRowsPerActor * 4L) {
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 1, row #$i", "COLUMN-1", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-1", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 2, row #$i", "COLUMN-2", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-2", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 3, row #$i", "COLUMN-3", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-3", List(("MY-SEQ", i)))
                  )
                }

                dataTreeDocument.children.size should be(0)
              }
            }

            describe("with exactly maxSequenceRowsPerActor rows") {
              it("should drop the defined number of rows within one actor") {
                val maxSequenceRowsPerActor =
                  system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

                val xml =
                  """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="COLUMN-1" stop-sign=","/><str id="COLUMN-2" stop-sign=","/><str id="COLUMN-3"/></elem></seq></dfasdl>"""
                val dfasdl = DFASDL("ID", xml)

                val dataTreeDocument = TestFSMRef(
                  new DataTreeDocument(dfasdl,
                                       Option("DataTreeDocumentTest"),
                                       Set("NOT-YOUR-COLUMN"))
                )
                dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
                dataTreeDocument.children.size should be(0)

                for (i <- 0L until maxSequenceRowsPerActor) {
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 1, row #$i", "COLUMN-1", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-1", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 2, row #$i", "COLUMN-2", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-2", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 3, row #$i", "COLUMN-3", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-3", List(("MY-SEQ", i)))
                  )
                }

                dataTreeDocument.children.size should be(0)
              }
            }

            describe("with exactly maxSequenceRowsPerActor + 1 rows") {
              it("should drop the defined number of rows within one actor") {
                val maxSequenceRowsPerActor =
                  system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

                val xml =
                  """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="COLUMN-1" stop-sign=","/><str id="COLUMN-2" stop-sign=","/><str id="COLUMN-3"/></elem></seq></dfasdl>"""
                val dfasdl = DFASDL("ID", xml)

                val dataTreeDocument = TestFSMRef(
                  new DataTreeDocument(dfasdl,
                                       Option("DataTreeDocumentTest"),
                                       Set("NOT-YOUR-COLUMN"))
                )
                dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)
                dataTreeDocument.children.size should be(0)

                for (i <- 0L to maxSequenceRowsPerActor) {
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 1, row #$i", "COLUMN-1", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-1", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 2, row #$i", "COLUMN-2", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-2", List(("MY-SEQ", i)))
                  )
                  dataTreeDocument ! SaveData(
                    ParserDataContainer(s"Column 3, row #$i", "COLUMN-3", Option("ID"), i),
                    calculateDataElementStorageHash("COLUMN-3", List(("MY-SEQ", i)))
                  )
                }

                dataTreeDocument.children.size should be(0)
              }
            }
          }
        }
      }
    }

    describe("when receiving ReturnData") {
      describe("without data") {
        it("should timeout") {
          val xml =
            """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><str id="MY-DATA"/></dfasdl>"""
          val dfasdl = DFASDL("ID", xml)

          val dataTreeDocument = TestFSMRef(new DataTreeDocument(dfasdl))
          dataTreeDocument.stateName should be(DataTreeDocumentState.Clean)

          dataTreeDocument ! ReturnData("MY-DATA")

          val thrown = the[java.lang.AssertionError] thrownBy expectMsgType[StatusMessage]
          thrown.getMessage should include("timeout")
        }
      }

      describe("with an unknown id") {
        it("should return an error message") {
          val xml =
            """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><str id="MY-DATA"/></dfasdl>"""
          val dfasdl = DFASDL("ID", xml)

          val dataTreeDocument = TestFSMRef(new DataTreeDocument(dfasdl))
          dataTreeDocument ! SaveData(ParserDataContainer("FOO", "MY-DATA"),
                                      calculateDataElementStorageHash("MY-DATA",
                                                                      List.empty[(String, Long)]))
          dataTreeDocument.stateName should be(DataTreeDocumentState.Working)

          dataTreeDocument ! ReturnData("UNKNOWN-ID")

          val message = expectMsgType[StatusMessage]
          message.message should be("Element with id 'UNKNOWN-ID' not found!")
        }
      }

      describe("with matching id") {
        describe("for a simple element") {
          it("should return the data") {
            val xml =
              """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><str id="MY-DATA"/></dfasdl>"""
            val dfasdl = DFASDL("ID", xml)

            val dataTreeDocument = TestFSMRef(new DataTreeDocument(dfasdl))
            dataTreeDocument ! SaveData(
              ParserDataContainer("FOO", "MY-DATA"),
              calculateDataElementStorageHash("MY-DATA", List.empty[(String, Long)])
            )
            dataTreeDocument.stateName should be(DataTreeDocumentState.Working)

            dataTreeDocument ! ReturnData("MY-DATA")

            val response = expectMsgType[DataTreeNodeMessages.Content]
            response.data.size should be(1)
            response.data.head.data should be("FOO")
          }
        }

        describe("for a sequence element") {
          describe("with specified sequence row") {
            it("should return the correct element") {
              val maxSequenceRowsPerActor =
                system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

              val xml =
                """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="MY-DATA" stop-sign=","/><str id="MY-NEXT-DATA"/></elem></seq></dfasdl>"""
              val dfasdl = DFASDL("ID", xml)

              val dataTreeDocument = TestFSMRef(new DataTreeDocument(dfasdl))

              for (i <- 0L until maxSequenceRowsPerActor * 2L) {
                val data =
                  ParserDataContainer(s"My data row number #$i", "MY-DATA", Option("ID"), i)
                dataTreeDocument ! SaveData(
                  data,
                  calculateDataElementStorageHash(data.elementId, List(("MY-SEQ", i)))
                )
                dataTreeDocument ! SaveData(
                  ParserDataContainer(s"My next data row number #$i",
                                      "MY-NEXT-DATA",
                                      Option("ID"),
                                      i),
                  calculateDataElementStorageHash("MY-NEXT-DATA", List(("MY-SEQ", i)))
                )
              }

              dataTreeDocument.stateName should be(DataTreeDocumentState.Working)

              dataTreeDocument ! ReturnData("MY-DATA", Option(maxSequenceRowsPerActor + 3))

              val response = expectMsgType[DataTreeNodeMessages.Content]
              response.data.size should be(1)
              response.data.head.data should be(
                s"My data row number #${maxSequenceRowsPerActor + 3}"
              )
            }
          }

          describe("without specified sequence row") {
            it("should return the first stored element") {
              val maxSequenceRowsPerActor =
                system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

              val xml =
                """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="MY-DATA" stop-sign=","/><str id="MY-NEXT-DATA"/></elem></seq></dfasdl>"""
              val dfasdl = DFASDL("ID", xml)

              val dataTreeDocument = TestFSMRef(new DataTreeDocument(dfasdl))

              for (i <- 0L until maxSequenceRowsPerActor * 2L) {
                val data =
                  ParserDataContainer(s"My data row number #$i", "MY-DATA", Option("ID"), i)
                dataTreeDocument ! SaveData(
                  data,
                  calculateDataElementStorageHash(data.elementId, List(("MY-SEQ", i)))
                )
                dataTreeDocument ! SaveData(
                  ParserDataContainer(s"My next data row number #$i",
                                      "MY-NEXT-DATA",
                                      Option("ID"),
                                      i),
                  calculateDataElementStorageHash("MY-NEXT-DATA", List(("MY-SEQ", i)))
                )
              }

              dataTreeDocument.stateName should be(DataTreeDocumentState.Working)

              dataTreeDocument ! ReturnData("MY-DATA", None)

              val response = expectMsgType[DataTreeNodeMessages.Content]
              response.data.size should be(1)
              response.data.head.data should be("My data row number #0")
            }
          }
        }
      }
    }

    describe("when receiving GetSequenceRowCount") {
      it("should return number of stored rows") {
        val maxSequenceRowsPerActor =
          system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

        val xml =
          """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" default-encoding="utf-8" semantic="niem"><seq id="MY-SEQ"><elem id="rows"><str id="MY-DATA" stop-sign=","/><str id="MY-NEXT-DATA"/></elem></seq></dfasdl>"""
        val dfasdl = DFASDL("ID", xml)

        val dataTreeDocument = TestFSMRef(new DataTreeDocument(dfasdl))

        for (i <- 0L until maxSequenceRowsPerActor * 2L) {
          val data = ParserDataContainer(s"My data row number #$i", "MY-DATA", Option("ID"), i)
          dataTreeDocument ! SaveData(data,
                                      calculateDataElementStorageHash(data.elementId,
                                                                      List(("MY-SEQ", i))))
          dataTreeDocument ! SaveData(
            ParserDataContainer(s"My next data row number #$i", "MY-NEXT-DATA", Option("ID"), i),
            calculateDataElementStorageHash("MY-NEXT-DATA", List(("MY-SEQ", i)))
          )
        }

        dataTreeDocument.stateName should be(DataTreeDocumentState.Working)

        dataTreeDocument ! GetSequenceRowCount(ElementReference("ID", "MY-SEQ"))

        val response = expectMsgType[SequenceRowCount]
        withClue("No rows were found for the element!")(response.rows.isDefined should be(true))
        response.rows.get should be(maxSequenceRowsPerActor * 2)
      }
    }
  }
}
