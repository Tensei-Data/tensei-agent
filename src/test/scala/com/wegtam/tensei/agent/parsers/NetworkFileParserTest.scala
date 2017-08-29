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

import akka.actor.Terminated
import akka.testkit.{ TestActorRef, TestProbe }
import akka.util.{ ByteString, Timeout }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.{ ActorSpec, DataTreeDocument, XmlTestHelpers }
import org.eclipse.jetty.server.handler._
import org.eclipse.jetty.server.{ Handler, Server }
import org.eclipse.jetty.toolchain.test.MavenTestingUtils
import org.eclipse.jetty.util.resource.Resource

import scala.concurrent.Await
import scala.concurrent.duration._

class NetworkFileParserTest extends ActorSpec with XmlTestHelpers {
  val portNumber             = ActorSpec.findAvailablePort()
  var server: Option[Server] = None

  override def beforeAll(): Unit = {
    server = Option(new Server(portNumber))

    // Create a Context Handler and ResourceHandler. The ContextHandler is
    // getting set to "/" path but this could be anything you like for
    // builing out your url. Note how we are setting the ResourceBase using
    // our jetty maven testing utilities to get the proper resource
    // directory, you needn't use these, you simply need to supply the paths
    // you are looking to serve content from.
    val context0: ContextHandler = new ContextHandler()
    context0.setContextPath("/")
    val rh0 = new ResourceHandler()
    val dir0 =
      MavenTestingUtils.getTestResourceDir("com/wegtam/tensei/agent/parsers/NetworkFileParsers")
    rh0.setBaseResource(Resource.newResource(dir0))
    context0.setHandler(rh0)

    val context1: ContextHandler = new ContextHandler()
    context1.setContextPath("/secure/")
    val rh1 = new ResourceHandler()
    val dir1 = MavenTestingUtils.getTestResourceDir(
      "com/wegtam/tensei/agent/parsers/NetworkFileParsers/secured"
    )
    rh1.setBaseResource(Resource.newResource(dir1))
    context1.setHandler(rh1)

    //    val constraint = new Constraint(Constraint.ANY_AUTH, "PERSONWEB")
    //    constraint.setAuthenticate(true)
    //    val mapping=new ConstraintMapping()
    //    mapping.setPathSpec("/secure/*")
    //    mapping.setConstraint(constraint)
    //    val securityHandler=new ConstraintSecurityHandler()
    //    val service=new HashLoginService()
    //    val s : Array[String] = new Array[String](1)
    //    s.update(0, "PERSONWEB")
    //    service.putUser("user1",new Password("password"),s)
    //    service.putUser("user2",new Password("password"),s)
    //    securityHandler.setConstraintMappings(Collections.singletonList(mapping))
    //    securityHandler.setLoginService(service)
    //    securityHandler.setAuthenticator(new BasicAuthenticator())

    //    val loginService: LoginService = new HashLoginService("MyRealm",
    //      "src/test/resources/com/wegtam/tensei/agent/parsers/NetworkFileParsers/realm.properties")
    //    server.get.addBean(loginService)
    //    val securityHandler: ConstraintSecurityHandler = new ConstraintSecurityHandler()
    //    val constraint: Constraint = new Constraint()
    //    constraint.setName("auth")
    //    constraint.setAuthenticate(true)
    //    val con : Array[String] = new Array[String](2)
    //    constraint.setRoles(con)
    //    val mapping: ConstraintMapping = new ConstraintMapping()
    //    mapping.setPathSpec("/secure/*")
    //    mapping.setConstraint(constraint)
    //    securityHandler.setConstraintMappings(Collections.singletonList(mapping))
    //    securityHandler.setAuthenticator(new BasicAuthenticator())
    //    securityHandler.setLoginService(loginService)

    // Create a ContextHandlerCollection and set the context handlers to it.
    // This will let jetty process urls against the declared contexts in
    // order to match up content.
    val contexts          = new ContextHandlerCollection()
    val c: Array[Handler] = new Array[Handler](2)
    c.update(0, context0)
    c.update(1, context1)
    //    c.update(2, securityHandler)
    contexts.setHandlers(c)

    server.get.setHandler(contexts)
    // Start things up!
    server.get.start()
  }

  override def afterAll(): Unit = {
    if (server.isDefined)
      server.get.stop()
    val _ = Await.result(system.terminate(), Duration.Inf)
  }

  describe("NetworkFileParser") {
    val data = new java.net.URI(s"http://localhost:$portNumber/contacts.csv")
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
    val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

    it("should initialize itself upon request") {
      val dataTree = TestActorRef(
        DataTreeDocument.props(dfasdl, Option("NetworkFileParserTest"), Set.empty[String])
      )
      val networkFileParser = TestActorRef(
        NetworkFileParser.props(source, cookbook, dataTree, Option("NetworkFileParserTest"))
      )

      networkFileParser ! BaseParserMessages.SubParserInitialize

      expectMsg(BaseParserMessages.SubParserInitialized)
    }

    it("should stop itself upon request") {
      val dataTree = TestActorRef(
        DataTreeDocument.props(dfasdl, Option("NetworkFileParserTest"), Set.empty[String])
      )
      val networkFileParser =
        TestActorRef(FileParser.props(source, cookbook, dataTree, Option("NetworkFileParserTest")))

      val p = TestProbe()
      p.watch(networkFileParser)
      networkFileParser ! BaseParserMessages.Stop
      val t = p.expectMsgType[Terminated]
      t.actor shouldEqual networkFileParser
    }

    describe("with HTTP request") {
      describe("of a CSV file") {
        describe("with simple-dfasdl") {
          val data = new java.net.URI(s"http://localhost:$portNumber/contacts.csv")
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
          val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

          it("should parse upon request") {
            val dataTree = TestActorRef(
              DataTreeDocument.props(dfasdl, Option("NetworkFileParserTest"), Set.empty[String])
            )
            val networkFileParser = TestActorRef(
              NetworkFileParser.props(source, cookbook, dataTree, Option("NetworkFileParserTest"))
            )

            implicit val timeout = Timeout(5.seconds)

            networkFileParser ! BaseParserMessages.SubParserInitialize
            expectMsg(BaseParserMessages.SubParserInitialized)

            networkFileParser ! BaseParserMessages.Start
            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
              ElementReference(dfasdl.id, "rows")
            )
            val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
            dataRows.rows.getOrElse(0L) should be(15L)

            dataTree ! DataTreeDocumentMessages.ReturnData("lastname")
            val cell00 = expectMsgType[DataTreeNodeMessages.Content]
            cell00.data.size should be(1)
            cell00.data.head.data should be(ByteString("Kohan"))

            dataTree ! DataTreeDocumentMessages.ReturnData("firstname")
            val cell01 = expectMsgType[DataTreeNodeMessages.Content]
            cell01.data.size should be(1)
            cell01.data.head.data should be(ByteString("Ignacio"))

            dataTree ! DataTreeDocumentMessages.ReturnData("lastname", Option(14L))
            val cell10 = expectMsgType[DataTreeNodeMessages.Content]
            cell10.data.size should be(1)
            cell10.data.head.data should be(ByteString("Betty"))

            dataTree ! DataTreeDocumentMessages.ReturnData("firstname", Option(14L))
            val cell11 = expectMsgType[DataTreeNodeMessages.Content]
            cell11.data.size should be(1)
            cell11.data.head.data should be(ByteString("Rex"))
          }
        }

        describe("with choice") {
          val data = new java.net.URI(s"http://localhost:$portNumber/choice.csv")
          val dfasdl = DFASDL(
            "SIMPLE-DFASDL",
            scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/com/wegtam/tensei/agent/parsers/NetworkFileParsers/choice.xml"
                )
              )
              .mkString
          )
          val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
          val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

          it("should parse upon request") {
            val dataTree = TestActorRef(
              DataTreeDocument.props(dfasdl, Option("NetworkFileParserTest"), Set.empty[String])
            )
            val networkFileParser = TestActorRef(
              NetworkFileParser.props(source, cookbook, dataTree, Option("NetworkFileParserTest"))
            )

            implicit val timeout = Timeout(5.seconds)

            networkFileParser ! BaseParserMessages.SubParserInitialize
            expectMsg(BaseParserMessages.SubParserInitialized)

            networkFileParser ! BaseParserMessages.Start
            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
              ElementReference(dfasdl.id, "rows")
            )
            val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
            dataRows.rows.getOrElse(0L) should be(2L)

            dataTree ! DataTreeDocumentMessages.ReturnData("num-field1")
            val cell00 = expectMsgType[DataTreeNodeMessages.Content]
            cell00.data.size should be(1)
            cell00.data.head.data shouldBe a[java.lang.Long]
            cell00.data.head.data should be(1L)

            dataTree ! DataTreeDocumentMessages.ReturnData("str-field2")
            val cell01 = expectMsgType[DataTreeNodeMessages.Content]
            cell01.data.size should be(1)
            cell01.data.head.data should be(ByteString("booyah"))

            dataTree ! DataTreeDocumentMessages.ReturnData("str-field3")
            val cell02 = expectMsgType[DataTreeNodeMessages.Content]
            cell02.data.size should be(1)
            cell02.data.head.data should be(ByteString("test01"))

            dataTree ! DataTreeDocumentMessages.ReturnData("str-field1", Option(1L))
            val cell10 = expectMsgType[DataTreeNodeMessages.Content]
            cell10.data.size should be(1)
            cell10.data.head.data should be(ByteString("MaxMustermann"))

            dataTree ! DataTreeDocumentMessages.ReturnData("num-field2", Option(1L))
            val cell11 = expectMsgType[DataTreeNodeMessages.Content]
            cell11.data.size should be(1)
            cell11.data.head.data should be(2)

            dataTree ! DataTreeDocumentMessages.ReturnData("str-field3", Option(1L))
            val cell12 = expectMsgType[DataTreeNodeMessages.Content]
            cell12.data.size should be(1)
            cell12.data.head.data should be(ByteString("1234Super"))
          }
        }

        describe("with fixseq") {
          val data = new java.net.URI(s"http://localhost:$portNumber/complex-csv-with-fixseq.csv")
          val dfasdl = DFASDL(
            "SIMPLE-DFASDL",
            scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/com/wegtam/tensei/agent/parsers/NetworkFileParsers/complex-csv-with-fixseq.xml"
                )
              )
              .mkString
          )
          val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
          val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

          it("should parse upon request") {
            val dataTree = TestActorRef(
              DataTreeDocument.props(dfasdl, Option("NetworkFileParserTest"), Set.empty[String])
            )
            val networkFileParser = TestActorRef(
              NetworkFileParser.props(source, cookbook, dataTree, Option("NetworkFileParserTest"))
            )

            implicit val timeout = Timeout(5.seconds)

            networkFileParser ! BaseParserMessages.SubParserInitialize
            expectMsg(BaseParserMessages.SubParserInitialized)

            networkFileParser ! BaseParserMessages.Start
            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
              ElementReference(dfasdl.id, "account_list")
            )
            val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
            dataRows.rows.getOrElse(0L) should be(3L)

            dataTree ! DataTreeDocumentMessages.ReturnData("firstname")
            val cell00 = expectMsgType[DataTreeNodeMessages.Content]
            cell00.data.size should be(1)
            cell00.data.head.data should be(ByteString("John"))

            dataTree ! DataTreeDocumentMessages.ReturnData("lastname")
            val cell01 = expectMsgType[DataTreeNodeMessages.Content]
            cell01.data.size should be(1)
            cell01.data.head.data should be(ByteString("Doe"))

            dataTree ! DataTreeDocumentMessages.ReturnData("division")
            val cell02 = expectMsgType[DataTreeNodeMessages.Content]
            cell02.data.size should be(1)
            cell02.data.head.data should be(ByteString("Sales"))

            dataTree ! DataTreeDocumentMessages.ReturnData("firstname", Option(1L))
            val cell10 = expectMsgType[DataTreeNodeMessages.Content]
            cell10.data.size should be(1)
            cell10.data.head.data should be(ByteString("Jane"))

            dataTree ! DataTreeDocumentMessages.ReturnData("lastname", Option(1L))
            val cell11 = expectMsgType[DataTreeNodeMessages.Content]
            cell11.data.size should be(1)
            cell11.data.head.data should be(ByteString("Doe"))

            dataTree ! DataTreeDocumentMessages.ReturnData("division", Option(1L))
            val cell12 = expectMsgType[DataTreeNodeMessages.Content]
            cell12.data.size should be(1)
            cell12.data.head.data should be(ByteString("Marketing"))

            dataTree ! DataTreeDocumentMessages.ReturnData("firstname", Option(2L))
            val cell20 = expectMsgType[DataTreeNodeMessages.Content]
            cell20.data.size should be(1)
            cell20.data.head.data should be(ByteString("Jake"))

            dataTree ! DataTreeDocumentMessages.ReturnData("lastname", Option(2L))
            val cell21 = expectMsgType[DataTreeNodeMessages.Content]
            cell21.data.size should be(1)
            cell21.data.head.data should be(ByteString("Doe"))

            dataTree ! DataTreeDocumentMessages.ReturnData("division", Option(2L))
            val cell22 = expectMsgType[DataTreeNodeMessages.Content]
            cell22.data.size should be(1)
            cell22.data.head.data should be(ByteString("Development"))
          }
        }
      }

      describe("of a TEXT file") {
        describe("with an E-mail") {
          val data = new java.net.URI(s"http://localhost:$portNumber/email-01.txt")
          val dfasdl = DFASDL(
            "SIMPLE-DFASDL",
            scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/com/wegtam/tensei/agent/parsers/NetworkFileParsers/email-01.xml"
                )
              )
              .mkString
          )
          val cookbook = Cookbook("COOKBOOK", List(dfasdl), None, List.empty[Recipe])
          val source   = ConnectionInformation(data, Option(DFASDLReference(cookbook.id, dfasdl.id)))

          it("should parse upon request") {
            val dataTree = TestActorRef(
              DataTreeDocument.props(dfasdl, Option("NetworkFileParserTest"), Set.empty[String])
            )
            val networkFileParser = TestActorRef(
              NetworkFileParser.props(source, cookbook, dataTree, Option("NetworkFileParserTest"))
            )

            implicit val timeout = Timeout(5.seconds)

            networkFileParser ! BaseParserMessages.SubParserInitialize
            expectMsg(BaseParserMessages.SubParserInitialized)

            networkFileParser ! BaseParserMessages.Start
            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
              ElementReference(dfasdl.id, "headers")
            )
            val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
            dataRows.rows.getOrElse(0L) should be(23L)

            dataTree ! DataTreeDocumentMessages.ReturnData("genericLabel")
            val cell00 = expectMsgType[DataTreeNodeMessages.Content]
            cell00.data.size should be(1)
            cell00.data.head.data should be(ByteString("Return-Path"))

            dataTree ! DataTreeDocumentMessages.ReturnData("genericHeaderMultiLineValue")
            val cell01 = expectMsgType[DataTreeNodeMessages.Content]
            cell01.data.size should be(1)
            cell01.data.head.data should be(ByteString("<jens@wegtam.com>"))

            dataTree ! DataTreeDocumentMessages.ReturnData("genericLabel", Option(2L))
            val cell10 = expectMsgType[DataTreeNodeMessages.Content]
            cell10.data.size should be(1)
            cell10.data.head.data should be(ByteString("Received"))

            dataTree ! DataTreeDocumentMessages.ReturnData("genericHeaderMultiLineValue",
                                                           Option(2L))
            val cell11 = expectMsgType[DataTreeNodeMessages.Content]
            cell11.data.size should be(1)
            cell11.data.head.data should be(
              ByteString(
                "from smtp41.gate.dfw1a (smtp41.gate.dfw1a.rsapps.net [172.20.100.41])\n\tby store130a.mail.dfw1a (SMTP Server) with ESMTP id 581391D80A2\n\tfor <andre@wegtam.com>; Mon, 28 Apr 2014 04:27:08 -0400 (EDT)"
              )
            )

            dataTree ! DataTreeDocumentMessages.ReturnData("dateLabel", Option(12L))
            val cell20 = expectMsgType[DataTreeNodeMessages.Content]
            cell20.data.size should be(1)
            cell20.data.head.data should be(ByteString("Date"))

            dataTree ! DataTreeDocumentMessages.ReturnData("dateValue", Option(12L))
            val cell21 = expectMsgType[DataTreeNodeMessages.Content]
            cell21.data.size should be(1)
            cell21.data.head.data should be(ByteString("Mon, 28 Apr 2014 10:27:06 +0200"))

            dataTree ! DataTreeDocumentMessages.ReturnData("body", Option(24L))
            val cell30 = expectMsgType[DataTreeNodeMessages.Content]
            cell30.data.size should be(1)
            cell30.data.head.data should be(
              ByteString(
                "Hi there,\n\nlorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam\nnonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat,\nsed diam voluptua. At vero eos et accusam et justo duo dolores et ea\nrebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem\nipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing\nelitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna\naliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo\ndolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus\nest Lorem ipsum dolor sit amet.\n\nLorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam\nnonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat,\nsed diam voluptua. At vero eos et accusam et justo duo dolores et ea\nrebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem\nipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing\nelitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna\naliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo\ndolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus\nest Lorem ipsum dolor sit amet.\n\nRegards,\n\nCicero\n\n--\n28. Ostermond 2014, 10:26\nHomepage : http://www.wegtam.com\n\nThe telephone is a good way to talk to people without having to offer\nthem a drink.\n\t\t-- Fran Lebowitz, \"Interview\""
              )
            )
          }
        }
      }

      describe("with auth") {
        describe("with simple-dfasdl") {
          val data = new java.net.URI(s"http://localhost:$portNumber/secure/contactssec.csv")
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
          val source = ConnectionInformation(data,
                                             Option(DFASDLReference(cookbook.id, dfasdl.id)),
                                             Option("test"),
                                             Option("testtest"))

          it("should parse upon request") {
            val dataTree = TestActorRef(
              DataTreeDocument.props(dfasdl, Option("NetworkFileParserTest"), Set.empty[String])
            )
            val networkFileParser = TestActorRef(
              NetworkFileParser.props(source, cookbook, dataTree, Option("NetworkFileParserTest"))
            )

            implicit val timeout = Timeout(5.seconds)

            networkFileParser ! BaseParserMessages.SubParserInitialize
            expectMsg(BaseParserMessages.SubParserInitialized)

            networkFileParser ! BaseParserMessages.Start
            val response = expectMsgType[ParserStatusMessage]
            response.status should be(ParserStatus.COMPLETED)

            dataTree ! DataTreeDocumentMessages.GetSequenceRowCount(
              ElementReference(dfasdl.id, "rows")
            )
            val dataRows = expectMsgType[DataTreeDocumentMessages.SequenceRowCount]
            dataRows.rows.getOrElse(0L) should be(15L)

            dataTree ! DataTreeDocumentMessages.ReturnData("lastname")
            val cell00 = expectMsgType[DataTreeNodeMessages.Content]
            cell00.data.size should be(1)
            cell00.data.head.data should be(ByteString("Kohan"))

            dataTree ! DataTreeDocumentMessages.ReturnData("firstname")
            val cell01 = expectMsgType[DataTreeNodeMessages.Content]
            cell01.data.size should be(1)
            cell01.data.head.data should be(ByteString("Ignacio"))

            dataTree ! DataTreeDocumentMessages.ReturnData("lastname", Option(14L))
            val cell10 = expectMsgType[DataTreeNodeMessages.Content]
            cell10.data.size should be(1)
            cell10.data.head.data should be(ByteString("Betty"))

            dataTree ! DataTreeDocumentMessages.ReturnData("firstname", Option(14L))
            val cell11 = expectMsgType[DataTreeNodeMessages.Content]
            cell11.data.size should be(1)
            cell11.data.head.data should be(ByteString("Rex"))
          }
        }
      }
    }
  }
}
