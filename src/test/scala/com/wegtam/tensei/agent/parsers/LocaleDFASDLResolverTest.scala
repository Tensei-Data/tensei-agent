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

import akka.testkit.TestActorRef
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.{ ActorSpec, DataTreeDocument, XmlTestHelpers }
import com.wegtam.tensei.agent.helpers.ExcelToCSVConverter
import com.wegtam.tensei.agent.helpers.ExcelToCSVConverter.ExcelConverterMessages.{
  Convert,
  ConvertResult
}

class LocaleDFASDLResolverTest extends ActorSpec with XmlTestHelpers {

  // When we change the locale of the system, the resolver for the DFASDL
  // does not get the DFASDL correctly
  //  Locale.setDefault(new Locale("de", "DE"))

  // this is just the language for the Excel file
  val locale = "de_DE"

  describe("when setting different language") {
    it("should resolve the DFASDL correctly") {
      val fileUri =
        getClass.getResource("/com/wegtam/tensei/agent/parsers/Excel/divers.xlsx").toURI
      val dfasdl = DFASDL(
        "SIMPLE-DFASDL",
        scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream("/com/wegtam/tensei/agent/parsers/Excel/divers.xml")
          )
          .mkString
      )
      val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
      val source = ConnectionInformation(uri = fileUri,
                                         dfasdlRef =
                                           Option(DFASDLReference(cookbook.id, dfasdl.id)),
                                         languageTag = Option(locale))

      // Convert
      val converter = TestActorRef(ExcelToCSVConverter.props(source, None))
      converter ! Convert
      val response = expectMsgType[ConvertResult]

      // Compare
      val dataTree =
        TestActorRef(DataTreeDocument.props(dfasdl, Option("FileParserTest"), Set.empty[String]))
      val fileParser = TestActorRef(
        FileParser.props(response.source, cookbook, dataTree, Option("FileParserTest"))
      )

      fileParser ! BaseParserMessages.Start
      val fileContent = expectMsgType[ParserStatusMessage]
      fileContent.status should be(ParserStatus.COMPLETED)
    }
  }

}
