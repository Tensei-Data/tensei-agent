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

package com.wegtam.tensei.agent.writers

import javax.xml.xpath.{ XPath, XPathConstants, XPathFactory }

import com.wegtam.tensei.adt.DFASDL
import com.wegtam.tensei.agent.DefaultSpec
import org.dfasdl.utils.ElementNames
import org.w3c.dom.{ Element, NodeList }

class DatabaseWriterFunctionsTest extends DefaultSpec with DatabaseWriterFunctions {
  describe("DatabaseWriterFunctions") {
    describe("createForeignKeyStatements") {
      describe("given a table without foreign keys") {
        it("should return an empty list") {
          val targetDfasdl =
            DFASDL(
              id = "T",
              content =
                """
                  |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                  |  <seq id="target1" keepID="true">
                  |    <elem id="target1-row">
                  |      <num id="A" db-column-name="id"/>
                  |      <str id="B" db-column-name="name"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target2" keepID="true">
                  |    <elem id="target2-row">
                  |      <num id="C" db-column-name="id" db-auto-inc="true"/>
                  |      <str id="D" db-column-name="firstname" db-foreign-key="I"/>
                  |      <num id="E" db-column-name="my_name" db-foreign-key="F"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target3" keepID="true">
                  |    <elem id="target3-row">
                  |      <num id="F" db-column-name="id"/>
                  |      <str id="G" db-column-name="name" db-foreign-key="L"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target4" keepID="true">
                  |    <elem id="target4-row">
                  |      <num id="H" db-column-name="id"/>
                  |      <str id="I" db-column-name="name"/>
                  |      <num id="J" db-column-name="another_id"/>
                  |      <str id="J2" db-column-name="yet_another_foreigner"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target5" keepID="true">
                  |    <elem id="target5-row">
                  |      <num id="K" db-column-name="id" db-foreign-key="A"/>
                  |      <str id="L" db-column-name="name"/>
                  |    </elem>
                  |  </seq>
                  |</dfasdl>
                  | """.stripMargin
            )
          val docWithSchema = createNormalizedDocument(targetDfasdl.content)
          val table         = docWithSchema.getElementById("target1")

          createForeignKeyStatements(table)(docWithSchema) should be(Seq.empty[String])
        }
      }

      describe("given a table with foreign keys") {
        it("should return the correct statements") {
          val targetDfasdl =
            DFASDL(
              id = "T",
              content =
                """
                  |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                  |  <seq id="target1" keepID="true">
                  |    <elem id="target1-row">
                  |      <num id="A" db-column-name="id"/>
                  |      <str id="B" db-column-name="name"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target2" keepID="true">
                  |    <elem id="target2-row">
                  |      <num id="C" db-column-name="id" db-auto-inc="true"/>
                  |      <str id="D" db-column-name="firstname" db-foreign-key="I"/>
                  |      <num id="E" db-column-name="my_name" db-foreign-key="F"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target3" keepID="true">
                  |    <elem id="target3-row">
                  |      <num id="F" db-column-name="id"/>
                  |      <str id="G" db-column-name="name" db-foreign-key="L"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target4" keepID="true">
                  |    <elem id="target4-row">
                  |      <num id="H" db-column-name="id"/>
                  |      <str id="I" db-column-name="name"/>
                  |      <num id="J" db-column-name="another_id"/>
                  |      <str id="J2" db-column-name="yet_another_foreigner"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target5" keepID="true">
                  |    <elem id="target5-row">
                  |      <num id="K" db-column-name="id" db-foreign-key="A"/>
                  |      <str id="L" db-column-name="name"/>
                  |    </elem>
                  |  </seq>
                  |</dfasdl>
                  | """.stripMargin
            )
          val docWithSchema = createNormalizedDocument(targetDfasdl.content)
          val table         = docWithSchema.getElementById("target2")
          val expectedStatements = List(
            "ALTER TABLE target2 ADD FOREIGN KEY (firstname) REFERENCES target4(name)",
            "ALTER TABLE target2 ADD FOREIGN KEY (my_name) REFERENCES target3(id)"
          )

          createForeignKeyStatements(table)(docWithSchema) should be(expectedStatements)
        }
      }
    }

    describe("sortTables") {
      describe("given no foreign keys") {
        it("should return the original list") {
          val targetDfasdl =
            DFASDL(
              id = "T",
              content =
                """
                  |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                  |  <seq id="target1" keepID="true">
                  |    <elem id="target1-row">
                  |      <num id="A" db-column-name="id"/>
                  |      <str id="B" db-column-name="name"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target2" keepID="true">
                  |    <elem id="target2-row">
                  |      <num id="C" db-column-name="id" db-auto-inc="true"/>
                  |      <str id="D" db-column-name="firstname"/>
                  |      <num id="E" db-column-name="my_name"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target3" keepID="true">
                  |    <elem id="target3-row">
                  |      <num id="F" db-column-name="id"/>
                  |      <str id="G" db-column-name="name"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target4" keepID="true">
                  |    <elem id="target4-row">
                  |      <num id="H" db-column-name="id"/>
                  |      <str id="I" db-column-name="name"/>
                  |      <num id="J" db-column-name="another_id"/>
                  |      <str id="J2" db-column-name="yet_another_foreigner"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target5" keepID="true">
                  |    <elem id="target5-row">
                  |      <num id="K" db-column-name="id"/>
                  |      <str id="L" db-column-name="name"/>
                  |    </elem>
                  |  </seq>
                  |</dfasdl>
                  | """.stripMargin
            )
          val doc           = createNormalizedDocument(targetDfasdl.content, useSchema = false)
          val docWithSchema = createNormalizedDocument(targetDfasdl.content)
          val xpath: XPath  = XPathFactory.newInstance().newXPath()
          val ts = xpath
            .evaluate(
              s"/${ElementNames.ROOT}/${ElementNames.SEQUENCE} | /${ElementNames.ROOT}/${ElementNames.FIXED_SEQUENCE}",
              doc.getDocumentElement,
              XPathConstants.NODESET
            )
            .asInstanceOf[NodeList]
          val tables = for (idx <- 0 until ts.getLength) yield ts.item(idx).asInstanceOf[Element]

          sortTables(tables)(docWithSchema) should be(tables)
        }
      }

      describe("given foreign keys") {
        it("should sort the tables correctly") {
          val targetDfasdl =
            DFASDL(
              id = "T",
              content =
                """
                  |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                  |  <seq id="target1" keepID="true">
                  |    <elem id="target1-row">
                  |      <num id="A" db-column-name="id"/>
                  |      <str id="B" db-column-name="name"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target2" keepID="true">
                  |    <elem id="target2-row">
                  |      <num id="C" db-column-name="id" db-auto-inc="true"/>
                  |      <str id="D" db-column-name="firstname" db-foreign-key="I"/>
                  |      <num id="E" db-column-name="my_name" db-foreign-key="F"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target3" keepID="true">
                  |    <elem id="target3-row">
                  |      <num id="F" db-column-name="id"/>
                  |      <str id="G" db-column-name="name" db-foreign-key="L"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target4" keepID="true">
                  |    <elem id="target4-row">
                  |      <num id="H" db-column-name="id"/>
                  |      <str id="I" db-column-name="name"/>
                  |      <num id="J" db-column-name="another_id"/>
                  |      <str id="J2" db-column-name="yet_another_foreigner"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target5" keepID="true">
                  |    <elem id="target5-row">
                  |      <num id="K" db-column-name="id" db-foreign-key="A"/>
                  |      <str id="L" db-column-name="name"/>
                  |    </elem>
                  |  </seq>
                  |</dfasdl>
                  | """.stripMargin
            )
          val doc          = createNormalizedDocument(targetDfasdl.content, useSchema = false)
          val xpath: XPath = XPathFactory.newInstance().newXPath()
          val ts = xpath
            .evaluate(
              s"/${ElementNames.ROOT}/${ElementNames.SEQUENCE} | /${ElementNames.ROOT}/${ElementNames.FIXED_SEQUENCE}",
              doc.getDocumentElement,
              XPathConstants.NODESET
            )
            .asInstanceOf[NodeList]
          val tables        = for (idx <- 0 until ts.getLength) yield ts.item(idx).asInstanceOf[Element]
          val docWithSchema = createNormalizedDocument(targetDfasdl.content)
          val sortedTables =
            for (id <- List("target1", "target4", "target5", "target3", "target2"))
              yield docWithSchema.getElementById(id)

          sortTables(tables)(docWithSchema).map(_.getAttribute("id")) should be(
            sortedTables.map(_.getAttribute("id"))
          )
        }
      }

      describe("given foreign keys with cross references") {
        it("should sort the tables correctly") {
          val targetDfasdl =
            DFASDL(
              id = "T",
              content =
                """
                  |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                  |  <seq id="target1" keepID="true">
                  |    <elem id="target1-row">
                  |      <num id="A" db-column-name="id"/>
                  |      <str id="B" db-column-name="name"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target2" keepID="true">
                  |    <elem id="target2-row">
                  |      <num id="C" db-column-name="id" db-auto-inc="true"/>
                  |      <str id="D" db-column-name="firstname" db-foreign-key="I"/>
                  |      <num id="E" db-column-name="my_name" db-foreign-key="F"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target3" keepID="true">
                  |    <elem id="target3-row">
                  |      <num id="F" db-column-name="id"/>
                  |      <str id="G" db-column-name="name" db-foreign-key="L"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target4" keepID="true">
                  |    <elem id="target4-row">
                  |      <num id="H" db-column-name="id"/>
                  |      <str id="I" db-column-name="name"/>
                  |      <num id="J" db-column-name="another_id" db-foreign-key="K"/>
                  |      <str id="J2" db-column-name="yet_another_foreigner" db-foreign-key="G"/>
                  |    </elem>
                  |  </seq>
                  |  <seq id="target5" keepID="true">
                  |    <elem id="target5-row">
                  |      <num id="K" db-column-name="id"/>
                  |      <str id="L" db-column-name="name"/>
                  |    </elem>
                  |  </seq>
                  |</dfasdl>
                  | """.stripMargin
            )
          val doc          = createNormalizedDocument(targetDfasdl.content, useSchema = false)
          val xpath: XPath = XPathFactory.newInstance().newXPath()
          val ts = xpath
            .evaluate(
              s"/${ElementNames.ROOT}/${ElementNames.SEQUENCE} | /${ElementNames.ROOT}/${ElementNames.FIXED_SEQUENCE}",
              doc.getDocumentElement,
              XPathConstants.NODESET
            )
            .asInstanceOf[NodeList]
          val tables        = for (idx <- 0 until ts.getLength) yield ts.item(idx).asInstanceOf[Element]
          val docWithSchema = createNormalizedDocument(targetDfasdl.content)
          val sortedTables =
            for (id <- List("target1", "target5", "target3", "target4", "target2"))
              yield docWithSchema.getElementById(id)

          sortTables(tables)(docWithSchema).map(_.getAttribute("id")) should be(
            sortedTables.map(_.getAttribute("id"))
          )
        }
      }
    }
  }
}
