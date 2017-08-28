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
import com.wegtam.tensei.adt._
import com.wegtam.tensei.adt.Recipe.MapOneToOne
import com.wegtam.tensei.agent._

import scala.concurrent.duration._

class CopyCSVTest extends XmlActorSpec with XmlTestHelpers {
  describe("Use cases") {
    describe("Copy") {
      describe("CopyCSV") {
        describe("when given a simple csv file") {
          describe("and the same source and target dfasdl") {
            it("should copy the file, adding a line-feed at the end") {
              val sourceDfasdl = loadDfasdl("SOURCE", "/usecases/copy/source-01.xml")
              val targetDfasdl = loadDfasdl("TARGET", "/usecases/copy/source-01.xml")

              val recipe = Recipe(
                id = "RECIPE-01",
                mode = MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(
                      ElementReference(sourceDfasdl.id, "firstname"),
                      ElementReference(sourceDfasdl.id, "lastname"),
                      ElementReference(sourceDfasdl.id, "e-mail")
                    ),
                    List(
                      ElementReference(targetDfasdl.id, "firstname"),
                      ElementReference(targetDfasdl.id, "lastname"),
                      ElementReference(targetDfasdl.id, "e-mail")
                    )
                  )
                )
              )

              val targetFilePath =
                File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

              val msg = createStartTransformationMessage(
                sourceDataPath = "/usecases/copy/source-01.csv",
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
                .fromInputStream(getClass.getResourceAsStream("/usecases/copy/source-01.csv"))
                .mkString
              val actualData = scala.io.Source.fromFile(targetFilePath).mkString

              withClue(s"Content of file $targetFilePath doesn't match expected data")(
                actualData should be(expectedData)
              )
            }
          }

          describe("and different dfasdls, mapping some columns") {
            it("should create the correct target file") {
              val sourceDfasdl = loadDfasdl("SOURCE", "/usecases/copy/source-01.xml")
              val targetDfasdl = loadDfasdl("TARGET", "/usecases/copy/target-01.xml")

              val recipe = Recipe(
                id = "RECIPE-01",
                mode = MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(
                      ElementReference(sourceDfasdl.id, "firstname"),
                      ElementReference(sourceDfasdl.id, "lastname"),
                      ElementReference(sourceDfasdl.id, "e-mail")
                    ),
                    List(
                      ElementReference(targetDfasdl.id, "firstname"),
                      ElementReference(targetDfasdl.id, "lastname"),
                      ElementReference(targetDfasdl.id, "e-mail")
                    )
                  )
                )
              )

              val targetFilePath =
                File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

              val msg = createStartTransformationMessage(
                sourceDataPath = "/usecases/copy/source-01.csv",
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
                .fromInputStream(getClass.getResourceAsStream("/usecases/copy/target-01.csv"))
                .mkString
              val actualData = scala.io.Source.fromFile(targetFilePath).mkString

              withClue(s"Content of file $targetFilePath doesn't match expected data")(
                actualData should be(expectedData)
              )
            }
          }
        }

        describe("when given a simple csv file with a line-feed at the end") {
          describe("and the same source and target dfasdl") {
            it("should copy the file, adding a line-feed at the end") {
              val sourceDfasdl = loadDfasdl("SOURCE", "/usecases/copy/source-01.xml")
              val targetDfasdl = loadDfasdl("TARGET", "/usecases/copy/source-01.xml")

              val recipe = Recipe(
                id = "RECIPE-01",
                mode = MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(
                      ElementReference(sourceDfasdl.id, "firstname"),
                      ElementReference(sourceDfasdl.id, "lastname"),
                      ElementReference(sourceDfasdl.id, "e-mail")
                    ),
                    List(
                      ElementReference(targetDfasdl.id, "firstname"),
                      ElementReference(targetDfasdl.id, "lastname"),
                      ElementReference(targetDfasdl.id, "e-mail")
                    )
                  )
                )
              )

              val targetFilePath =
                File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

              val msg = createStartTransformationMessage(
                sourceDataPath = "/usecases/copy/source-01-with-line-feed-at-the-end.csv",
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
                .fromInputStream(getClass.getResourceAsStream("/usecases/copy/source-01.csv"))
                .mkString
              val actualData = scala.io.Source.fromFile(targetFilePath).mkString

              withClue(s"Content of file $targetFilePath doesn't match expected data")(
                actualData should be(expectedData)
              )
            }
          }

          describe("and different dfasdls, mapping some columns") {
            it("should create the correct target file") {
              val sourceDfasdl = loadDfasdl("SOURCE", "/usecases/copy/source-01.xml")
              val targetDfasdl = loadDfasdl("TARGET", "/usecases/copy/target-01.xml")

              val recipe = Recipe(
                id = "RECIPE-01",
                mode = MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(
                      ElementReference(sourceDfasdl.id, "firstname"),
                      ElementReference(sourceDfasdl.id, "lastname"),
                      ElementReference(sourceDfasdl.id, "e-mail")
                    ),
                    List(
                      ElementReference(targetDfasdl.id, "firstname"),
                      ElementReference(targetDfasdl.id, "lastname"),
                      ElementReference(targetDfasdl.id, "e-mail")
                    )
                  )
                )
              )

              val targetFilePath =
                File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

              val msg = createStartTransformationMessage(
                sourceDataPath = "/usecases/copy/source-01-with-line-feed-at-the-end.csv",
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
                .fromInputStream(getClass.getResourceAsStream("/usecases/copy/target-01.csv"))
                .mkString
              val actualData = scala.io.Source.fromFile(targetFilePath).mkString

              withClue(s"Content of file $targetFilePath doesn't match expected data")(
                actualData should be(expectedData)
              )
            }
          }
        }

        describe("when given a simple csv file with different source and target dfasdl") {
          describe("and two line feed at the end of the source file") {
            it("should create the correct target file") {
              val sourceDfasdl = loadDfasdl("SOURCE", "/usecases/copy/source-02.xml")
              val targetDfasdl = loadDfasdl("TARGET", "/usecases/copy/target-02.xml")

              val recipe = Recipe(
                id = "RECIPE-01",
                mode = MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(
                      ElementReference(sourceDfasdl.id, "lastname"),
                      ElementReference(sourceDfasdl.id, "firstname"),
                      ElementReference(sourceDfasdl.id, "email"),
                      ElementReference(sourceDfasdl.id, "birthday"),
                      ElementReference(sourceDfasdl.id, "phone"),
                      ElementReference(sourceDfasdl.id, "division")
                    ),
                    List(
                      ElementReference(targetDfasdl.id, "employee_row_lastname"),
                      ElementReference(targetDfasdl.id, "employee_row_firstname"),
                      ElementReference(targetDfasdl.id, "employee_row_email"),
                      ElementReference(targetDfasdl.id, "employee_row_birthday"),
                      ElementReference(targetDfasdl.id, "employee_row_phone"),
                      ElementReference(targetDfasdl.id, "employee_row_department")
                    )
                  ),
                  MappingTransformation(
                    List(
                      ElementReference(sourceDfasdl.id, "lastname")
                    ),
                    List(
                      ElementReference(targetDfasdl.id, "employee_row_id")
                    ),
                    List(
                      TransformationDescription(
                        transformerClassName = "com.wegtam.tensei.agent.transformers.Nullify",
                        options = TransformerOptions(classOf[String], classOf[String])
                      )
                    )
                  )
                )
              )

              val targetFilePath =
                File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

              val msg = createStartTransformationMessage(
                sourceDataPath = "/usecases/copy/source-02.csv",
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
                .fromInputStream(getClass.getResourceAsStream("/usecases/copy/target-02.csv"))
                .mkString
              val actualData = scala.io.Source.fromFile(targetFilePath).mkString

              withClue(s"Content of file $targetFilePath doesn't match expected data")(
                actualData should be(expectedData)
              )
            }
          }

          describe("and 10 line feed at the end of the source file") {
            it("should create the correct target file") {
              val sourceDfasdl = loadDfasdl("SOURCE", "/usecases/copy/source-02.xml")
              val targetDfasdl = loadDfasdl("TARGET", "/usecases/copy/target-02.xml")

              val recipe = Recipe(
                id = "RECIPE-01",
                mode = MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(
                      ElementReference(sourceDfasdl.id, "lastname"),
                      ElementReference(sourceDfasdl.id, "firstname"),
                      ElementReference(sourceDfasdl.id, "email"),
                      ElementReference(sourceDfasdl.id, "birthday"),
                      ElementReference(sourceDfasdl.id, "phone"),
                      ElementReference(sourceDfasdl.id, "division")
                    ),
                    List(
                      ElementReference(targetDfasdl.id, "employee_row_lastname"),
                      ElementReference(targetDfasdl.id, "employee_row_firstname"),
                      ElementReference(targetDfasdl.id, "employee_row_email"),
                      ElementReference(targetDfasdl.id, "employee_row_birthday"),
                      ElementReference(targetDfasdl.id, "employee_row_phone"),
                      ElementReference(targetDfasdl.id, "employee_row_department")
                    )
                  ),
                  MappingTransformation(
                    List(
                      ElementReference(sourceDfasdl.id, "lastname")
                    ),
                    List(
                      ElementReference(targetDfasdl.id, "employee_row_id")
                    ),
                    List(
                      TransformationDescription(
                        transformerClassName = "com.wegtam.tensei.agent.transformers.Nullify",
                        options = TransformerOptions(classOf[String], classOf[String])
                      )
                    )
                  )
                )
              )

              val targetFilePath =
                File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

              val msg = createStartTransformationMessage(
                sourceDataPath = "/usecases/copy/source-03.csv",
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
                .fromInputStream(getClass.getResourceAsStream("/usecases/copy/target-02.csv"))
                .mkString
              val actualData = scala.io.Source.fromFile(targetFilePath).mkString

              withClue(s"Content of file $targetFilePath doesn't match expected data")(
                actualData should be(expectedData)
              )
            }
          }

          describe(
            "and multiple line feed at the end of the source file that contain tabs and spaces"
          ) {
            it("should create the correct target file") {
              val sourceDfasdl = loadDfasdl("SOURCE", "/usecases/copy/source-02.xml")
              val targetDfasdl = loadDfasdl("TARGET", "/usecases/copy/target-02.xml")

              val recipe = Recipe(
                id = "RECIPE-01",
                mode = MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(
                      ElementReference(sourceDfasdl.id, "lastname"),
                      ElementReference(sourceDfasdl.id, "firstname"),
                      ElementReference(sourceDfasdl.id, "email"),
                      ElementReference(sourceDfasdl.id, "birthday"),
                      ElementReference(sourceDfasdl.id, "phone"),
                      ElementReference(sourceDfasdl.id, "division")
                    ),
                    List(
                      ElementReference(targetDfasdl.id, "employee_row_lastname"),
                      ElementReference(targetDfasdl.id, "employee_row_firstname"),
                      ElementReference(targetDfasdl.id, "employee_row_email"),
                      ElementReference(targetDfasdl.id, "employee_row_birthday"),
                      ElementReference(targetDfasdl.id, "employee_row_phone"),
                      ElementReference(targetDfasdl.id, "employee_row_department")
                    )
                  ),
                  MappingTransformation(
                    List(
                      ElementReference(sourceDfasdl.id, "lastname")
                    ),
                    List(
                      ElementReference(targetDfasdl.id, "employee_row_id")
                    ),
                    List(
                      TransformationDescription(
                        transformerClassName = "com.wegtam.tensei.agent.transformers.Nullify",
                        options = TransformerOptions(classOf[String], classOf[String])
                      )
                    )
                  )
                )
              )

              val targetFilePath =
                File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

              val msg = createStartTransformationMessage(
                sourceDataPath = "/usecases/copy/source-04.csv",
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
                .fromInputStream(getClass.getResourceAsStream("/usecases/copy/target-02.csv"))
                .mkString
              val actualData = scala.io.Source.fromFile(targetFilePath).mkString

              withClue(s"Content of file $targetFilePath doesn't match expected data")(
                actualData should be(expectedData)
              )
            }
          }

          describe("with newlines in one of the inner columns") {
            it("should create the correct target file") {
              val sourceDfasdl = loadDfasdl("SOURCE", "/usecases/copy/source-newline-01.xml")
              val targetDfasdl = loadDfasdl("TARGET", "/usecases/copy/target-newline-01.xml")

              val recipe = Recipe(
                id = "RECIPE-01",
                mode = MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(
                      ElementReference(sourceDfasdl.id, "lastname"),
                      ElementReference(sourceDfasdl.id, "firstname"),
                      ElementReference(sourceDfasdl.id, "e-mail"),
                      ElementReference(sourceDfasdl.id, "description")
                    ),
                    List(
                      ElementReference(targetDfasdl.id, "lastname"),
                      ElementReference(targetDfasdl.id, "firstname"),
                      ElementReference(targetDfasdl.id, "e-mail"),
                      ElementReference(targetDfasdl.id, "description")
                    )
                  )
                )
              )

              val targetFilePath =
                File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

              val msg = createStartTransformationMessage(
                sourceDataPath = "/usecases/copy/source-newline-01.csv",
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
                  getClass.getResourceAsStream("/usecases/copy/target-newline-01.csv")
                )
                .mkString
              val actualData = scala.io.Source.fromFile(targetFilePath).mkString

              withClue(s"Content of file $targetFilePath doesn't match expected data")(
                actualData should be(expectedData)
              )
            }
          }

          describe("with newlines in one of the inner columns and tabs") {
            it("should create the correct target file") {
              val sourceDfasdl = loadDfasdl("SOURCE", "/usecases/copy/source-newline-02.xml")
              val targetDfasdl = loadDfasdl("TARGET", "/usecases/copy/target-newline-02.xml")

              val recipe = Recipe(
                id = "RECIPE-01",
                mode = MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(
                      ElementReference(sourceDfasdl.id, "lastname"),
                      ElementReference(sourceDfasdl.id, "firstname"),
                      ElementReference(sourceDfasdl.id, "e-mail"),
                      ElementReference(sourceDfasdl.id, "description")
                    ),
                    List(
                      ElementReference(targetDfasdl.id, "lastname"),
                      ElementReference(targetDfasdl.id, "firstname"),
                      ElementReference(targetDfasdl.id, "e-mail"),
                      ElementReference(targetDfasdl.id, "description")
                    )
                  )
                )
              )

              val targetFilePath =
                File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

              val msg = createStartTransformationMessage(
                sourceDataPath = "/usecases/copy/source-newline-02.tsv",
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
                  getClass.getResourceAsStream("/usecases/copy/target-newline-02.tsv")
                )
                .mkString
              val actualData = scala.io.Source.fromFile(targetFilePath).mkString

              withClue(s"Content of file $targetFilePath doesn't match expected data")(
                actualData should be(expectedData)
              )
            }
          }

          describe("with diverse decimal separated numbers") {
            describe("and decimal-separator in source defined as comma") {
              it("should create the correct target file") {
                val sourceDfasdl = loadDfasdl("SOURCE", "/usecases/copy/source-05.xml")
                val targetDfasdl = loadDfasdl("TARGET", "/usecases/copy/target-05.xml")

                val recipe = Recipe(
                  id = "RECIPE-01",
                  mode = MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(
                        ElementReference(sourceDfasdl.id, "firstname"),
                        ElementReference(sourceDfasdl.id, "lastname"),
                        ElementReference(sourceDfasdl.id, "e-mail"),
                        ElementReference(sourceDfasdl.id, "value")
                      ),
                      List(
                        ElementReference(targetDfasdl.id, "firstname"),
                        ElementReference(targetDfasdl.id, "lastname"),
                        ElementReference(targetDfasdl.id, "e-mail"),
                        ElementReference(targetDfasdl.id, "value")
                      )
                    )
                  )
                )

                val targetFilePath =
                  File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

                val msg = createStartTransformationMessage(
                  sourceDataPath = "/usecases/copy/source-05.csv",
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
                  .fromInputStream(getClass.getResourceAsStream("/usecases/copy/target-05.csv"))
                  .mkString
                val actualData = scala.io.Source.fromFile(targetFilePath).mkString

                withClue(s"Content of file $targetFilePath doesn't match expected data")(
                  actualData should be(expectedData)
                )
              }
            }

            describe("and decimal-separator in source defined as point") {
              it("should create the correct target file") {
                val sourceDfasdl = loadDfasdl("SOURCE", "/usecases/copy/source-06.xml")
                val targetDfasdl = loadDfasdl("TARGET", "/usecases/copy/target-06.xml")

                val recipe = Recipe(
                  id = "RECIPE-01",
                  mode = MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(
                        ElementReference(sourceDfasdl.id, "firstname"),
                        ElementReference(sourceDfasdl.id, "lastname"),
                        ElementReference(sourceDfasdl.id, "e-mail"),
                        ElementReference(sourceDfasdl.id, "value")
                      ),
                      List(
                        ElementReference(targetDfasdl.id, "firstname"),
                        ElementReference(targetDfasdl.id, "lastname"),
                        ElementReference(targetDfasdl.id, "e-mail"),
                        ElementReference(targetDfasdl.id, "value")
                      )
                    )
                  )
                )

                val targetFilePath =
                  File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

                val msg = createStartTransformationMessage(
                  sourceDataPath = "/usecases/copy/source-06.csv",
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
                  .fromInputStream(getClass.getResourceAsStream("/usecases/copy/target-06.csv"))
                  .mkString
                val actualData = scala.io.Source.fromFile(targetFilePath).mkString

                withClue(s"Content of file $targetFilePath doesn't match expected data")(
                  actualData should be(expectedData)
                )
              }
            }

            describe("and decimal-separator in source defined as ⎖") {
              it("should create the correct target file") {
                val sourceDfasdl = loadDfasdl("SOURCE", "/usecases/copy/source-07.xml")
                val targetDfasdl = loadDfasdl("TARGET", "/usecases/copy/target-07.xml")

                val recipe = Recipe(
                  id = "RECIPE-01",
                  mode = MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(
                        ElementReference(sourceDfasdl.id, "firstname"),
                        ElementReference(sourceDfasdl.id, "lastname"),
                        ElementReference(sourceDfasdl.id, "e-mail"),
                        ElementReference(sourceDfasdl.id, "value")
                      ),
                      List(
                        ElementReference(targetDfasdl.id, "firstname"),
                        ElementReference(targetDfasdl.id, "lastname"),
                        ElementReference(targetDfasdl.id, "e-mail"),
                        ElementReference(targetDfasdl.id, "value")
                      )
                    )
                  )
                )

                val targetFilePath =
                  File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

                val msg = createStartTransformationMessage(
                  sourceDataPath = "/usecases/copy/source-07.csv",
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
                  .fromInputStream(getClass.getResourceAsStream("/usecases/copy/target-07.csv"))
                  .mkString
                val actualData = scala.io.Source.fromFile(targetFilePath).mkString

                withClue(s"Content of file $targetFilePath doesn't match expected data")(
                  actualData should be(expectedData)
                )
              }
            }
          }
        }
      }
    }
  }
}
