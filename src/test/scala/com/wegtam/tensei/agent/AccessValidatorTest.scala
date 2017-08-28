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

import java.io.File
import java.net.URI

import akka.testkit.TestActorRef
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.AccessValidator.AccessValidatorMessages
import org.eclipse.jetty.server.{ Handler, Server }
import org.eclipse.jetty.server.handler.{
  ContextHandler,
  ContextHandlerCollection,
  ResourceHandler
}
import org.eclipse.jetty.toolchain.test.MavenTestingUtils
import org.eclipse.jetty.util.resource.Resource

import scala.util.Random
import scalaz._
import Scalaz._

class AccessValidatorTest extends ActorSpec {
  val agentRunIdentifier = Option("AccessValidatorTest")

  val serverPort: Int        = ActorSpec.findAvailablePort()
  var server: Option[Server] = None

  override def beforeAll(): Unit = {
    val s                        = new Server(serverPort)
    val context0: ContextHandler = new ContextHandler()
    context0.setContextPath("/")
    val rh0 = new ResourceHandler()
    val dir0 =
      MavenTestingUtils.getTestResourceDir("com/wegtam/tensei/agent/parsers/NetworkFileParsers")
    rh0.setBaseResource(Resource.newResource(dir0))
    context0.setHandler(rh0)

    // Create a ContextHandlerCollection and set the context handlers to it.
    // This will let jetty process urls against the declared contexts in
    // order to match up content.
    val contexts          = new ContextHandlerCollection()
    val c: Array[Handler] = new Array[Handler](1)
    c.update(0, context0)
    contexts.setHandlers(c)

    s.setHandler(contexts)
    // Start things up!
    s.start()
    server = Option(s)
  }

  override protected def afterAll(): Unit = {
    server.foreach(s => s.stop())
    super.afterAll()
  }

  describe("AccessValidator") {
    describe("validation a single connection") {
      describe("for a file") {
        describe("without write access") {
          describe("for an existing file") {
            it("should return a success") {
              val sourceFilePath =
                File.createTempFile("tensei-agent", "test").getAbsolutePath.replace("\\", "/")
              val source = ConnectionInformation(new URI(s"file:$sourceFilePath"), None)

              val accessValidator = TestActorRef(new AccessValidator(agentRunIdentifier))

              accessValidator ! AccessValidatorMessages.ValidateConnection(con = source,
                                                                           writeable = false)

              val response = expectMsgType[AccessValidatorMessages.ValidateConnectionResult]

              response.result match {
                case Success(c) => c should be(source)
                case Failure(f) => fail(f.toList.mkString(", "))
              }
            }
          }

          describe("for a non existing file") {
            it("should return a failure") {
              val sourceFilePath =
                s"/${Random.alphanumeric.take(10).mkString}/some/non/existing/path/file.txt"
              val source = ConnectionInformation(new URI(s"file:$sourceFilePath"), None)

              val accessValidator = TestActorRef(new AccessValidator(agentRunIdentifier))

              accessValidator ! AccessValidatorMessages.ValidateConnection(con = source,
                                                                           writeable = false)

              val response = expectMsgType[AccessValidatorMessages.ValidateConnectionResult]

              response.result match {
                case Success(_) => fail("This test should return a failure!")
                case Failure(f) =>
                  f.toList.head shouldEqual s"File $sourceFilePath does not exist!"
              }
            }
          }
        }

        describe("with write access") {
          describe("for an existing file") {
            it("should return a success") {
              val targetFilePath =
                File.createTempFile("tensei-agent", "test").getAbsolutePath.replace("\\", "/")
              val target = ConnectionInformation(new URI(s"file:$targetFilePath"), None)

              val accessValidator = TestActorRef(new AccessValidator(agentRunIdentifier))

              accessValidator ! AccessValidatorMessages.ValidateConnection(con = target,
                                                                           writeable = true)

              val response = expectMsgType[AccessValidatorMessages.ValidateConnectionResult]

              response.result match {
                case Success(c) => c should be(target)
                case Failure(f) => fail(f.toList.mkString(", "))
              }
            }
          }

          describe("for a non existing file") {
            it("should return a failure") {
              val targetFilePath =
                s"/${Random.alphanumeric.take(10).mkString}/some/non/existing/path/file.txt"
              val target = ConnectionInformation(new URI(s"file:$targetFilePath"), None)

              val accessValidator = TestActorRef(new AccessValidator(agentRunIdentifier))

              accessValidator ! AccessValidatorMessages.ValidateConnection(con = target,
                                                                           writeable = true)

              val response = expectMsgType[AccessValidatorMessages.ValidateConnectionResult]

              response.result match {
                case Success(_) => fail("This test should return a failure!")
                case Failure(f) =>
                  val errorMessage =
                    if (f.toList.head == "No such file or directory")
                      "No such file or directory"
                    else
                      "Datei oder Verzeichnis nicht gefunden"
                  f.toList.head shouldEqual errorMessage
              }
            }
          }
        }
      }

      describe("for a network file") {
        describe("as HTTP connection") {
          describe("without username and password") {
            describe("for a non existing file") {
              it("should return a failure") {
                val localURI = s"http://localhost:$serverPort/non-existing-file.csv"
                val data     = new java.net.URI(localURI)
                val dfasdl = DFASDL(
                  "SIMPLE-DFASDL",
                  scala.io.Source
                    .fromInputStream(
                      getClass.getResourceAsStream(
                        "/com/wegtam/tensei/agent/parsers/NetworkFileParsers/simple-dfasdl.xml"
                      )
                    )
                    .mkString
                )
                val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
                val source =
                  ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

                val accessValidator = TestActorRef(new AccessValidator(agentRunIdentifier))

                accessValidator ! AccessValidatorMessages.ValidateConnection(con = source,
                                                                             writeable = false)

                val response = expectMsgType[AccessValidatorMessages.ValidateConnectionResult]

                response.result match {
                  case Success(_) => fail("This test should return a failure!")
                  case Failure(f) =>
                    f.toList.head shouldEqual s"Error during HTTP connection for network file `$localURI`: HTTP/1.1 404 Not Found"
                }
              }
            }

            describe("for an existing file") {
              it("should return a failure") {
                val localURI = s"http://localhost:$serverPort/contacts.csv"
                val data     = new java.net.URI(localURI)
                val dfasdl = DFASDL(
                  "SIMPLE-DFASDL",
                  scala.io.Source
                    .fromInputStream(
                      getClass.getResourceAsStream(
                        "/com/wegtam/tensei/agent/parsers/NetworkFileParsers/simple-dfasdl.xml"
                      )
                    )
                    .mkString
                )
                val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
                val source =
                  ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

                val accessValidator = TestActorRef(new AccessValidator(agentRunIdentifier))

                accessValidator ! AccessValidatorMessages.ValidateConnection(con = source,
                                                                             writeable = false)

                val response = expectMsgType[AccessValidatorMessages.ValidateConnectionResult]

                response.result match {
                  case Success(c) => c should be(source)
                  case Failure(f) => fail(f.toList.mkString(", "))
                }
              }
            }
          }
        }
      }

      describe("for a database") {
        describe("without write access") {
          describe("for a valid jdbc uri") {
            it("should return a success") {
              val source = ConnectionInformation(new URI(s"jdbc:h2:mem:test"), None)

              val accessValidator = TestActorRef(new AccessValidator(agentRunIdentifier))

              accessValidator ! AccessValidatorMessages.ValidateConnection(con = source,
                                                                           writeable = false)

              val response = expectMsgType[AccessValidatorMessages.ValidateConnectionResult]

              response.result match {
                case Success(c) => c should be(source)
                case Failure(f) => fail(f.toList.mkString(", "))
              }
            }
          }

          describe("for an invalid jdbc uri") {
            it("should return a failure") {
              val source = ConnectionInformation(new URI(s"jdbc:noSuchDriver:mem:test"), None)

              val accessValidator = TestActorRef(new AccessValidator(agentRunIdentifier))

              accessValidator ! AccessValidatorMessages.ValidateConnection(con = source,
                                                                           writeable = false)

              val response = expectMsgType[AccessValidatorMessages.ValidateConnectionResult]

              response.result match {
                case Success(_) => fail("This test should return a failure!")
                case Failure(f) =>
                  f.toList.head shouldEqual "No suitable driver found for jdbc:noSuchDriver:mem:test"
              }
            }
          }
        }

        describe("with write access") {
          describe("for a valid jdbc uri") {
            it("should return a success") {
              val target = ConnectionInformation(new URI(s"jdbc:h2:mem:test"), None)

              val accessValidator = TestActorRef(new AccessValidator(agentRunIdentifier))

              accessValidator ! AccessValidatorMessages.ValidateConnection(con = target,
                                                                           writeable = true)

              val response = expectMsgType[AccessValidatorMessages.ValidateConnectionResult]

              response.result match {
                case Success(c) => c should be(target)
                case Failure(f) => fail(f.toList.mkString(", "))
              }
            }
          }

          describe("for an invalid jdbc uri") {
            it("should return a failure") {
              val target = ConnectionInformation(new URI(s"jdbc:noSuchDriver:mem:test"), None)

              val accessValidator = TestActorRef(new AccessValidator(agentRunIdentifier))

              accessValidator ! AccessValidatorMessages.ValidateConnection(con = target,
                                                                           writeable = true)

              val response = expectMsgType[AccessValidatorMessages.ValidateConnectionResult]

              response.result match {
                case Success(_) => fail("This test should return a failure!")
                case Failure(f) =>
                  f.toList.head shouldEqual "No suitable driver found for jdbc:noSuchDriver:mem:test"
              }
            }
          }
        }

        describe("for a local SQLITE databasae") {
          describe("Connect to a database") {
            it("should work") {
              val tempFile   = File.createTempFile("tensei-agent", "testSqlite.db")
              val sqliteFile = tempFile.getAbsolutePath.replace("\\", "/")

              val target = ConnectionInformation(new URI(s"jdbc:sqlite:$sqliteFile"), None)

              val accessValidator = TestActorRef(new AccessValidator(agentRunIdentifier))

              accessValidator ! AccessValidatorMessages.ValidateConnection(con = target,
                                                                           writeable = true)

              val response = expectMsgType[AccessValidatorMessages.ValidateConnectionResult]

              response.result match {
                case Success(s) => s should be(target)
                case Failure(f) => fail(f.toList.mkString(", "))
              }

              tempFile.delete()
            }
          }
        }
      }
    }
  }
}
