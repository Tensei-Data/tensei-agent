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

package usecases.copy

import java.io.File

import akka.testkit.{ TestActorRef, TestFSMRef }
import com.wegtam.tensei.adt.{ ElementReference, GlobalMessages, MappingTransformation, Recipe }
import com.wegtam.tensei.adt.Recipe.MapOneToOne
import com.wegtam.tensei.agent.{ DummyActor, TenseiAgent, XmlActorSpec, XmlTestHelpers }

import scala.concurrent.duration._

class EmailToCSV extends XmlActorSpec with XmlTestHelpers {
  describe("Use cases") {
    describe("EmailToCSV") {
      it("should migrate specific fields of the Email into the CSV") {
        val sourceDfasdl = loadDfasdl("SOURCE", "/usecases/copy/email-01.xml")
        val targetDfasdl = loadDfasdl("TARGET", "/usecases/copy/email-target-01.xml")

        val recipe = Recipe(
          id = "RECIPE-01",
          mode = MapOneToOne,
          mappings = List(
            MappingTransformation(
              List(
                ElementReference(sourceDfasdl.id, "subjectValue"),
                ElementReference(sourceDfasdl.id, "fromValue"),
                ElementReference(sourceDfasdl.id, "toValue")
              ),
              List(
                ElementReference(targetDfasdl.id, "subject"),
                ElementReference(targetDfasdl.id, "from"),
                ElementReference(targetDfasdl.id, "to")
              )
            )
          )
        )

        val targetFilePath =
          File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

        val msg = createStartTransformationMessage(
          sourceDataPath = "/usecases/copy/email-01.txt",
          targetFilePath = targetFilePath,
          sourceDFASDL = sourceDfasdl,
          targetDFASDL = Option(targetDfasdl),
          recipes = List(recipe)
        )

        val dummy  = TestActorRef(DummyActor.props())
        val client = system.actorSelection(dummy.path)
        val agent  = TestFSMRef(new TenseiAgent("TEST-AGENT", client))

        agent ! msg

        expectMsgType[GlobalMessages.TransformationStarted](FiniteDuration(5, SECONDS))

        expectMsgType[GlobalMessages.TransformationCompleted](FiniteDuration(7, SECONDS))

        val expectedData = scala.io.Source
          .fromInputStream(
            getClass.getResourceAsStream("/usecases/copy/email-target-01-expected-data.csv")
          )
          .mkString
        val actualData = scala.io.Source.fromFile(targetFilePath).mkString

        actualData should be(expectedData)
      }
    }
  }

}
