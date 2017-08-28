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

import java.util

import akka.testkit.TestActorRef
import akka.util.{ ByteString, Timeout }
import org.apache.ftpserver.ftplet._
import org.apache.ftpserver.usermanager._
import org.apache.ftpserver.usermanager.impl.{ BaseUser, WritePermission }
import org.apache.ftpserver.{ ConnectionConfigFactory, FtpServer, FtpServerFactory }
import org.apache.ftpserver.listener.ListenerFactory

import scala.concurrent.duration._
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.{ ActorSpec, DataTreeDocument, XmlTestHelpers }

import scala.concurrent.Await

class NetworkFileParserFTPTest extends ActorSpec with XmlTestHelpers {
  val portNumber                = ActorSpec.findAvailablePort()
  var server: Option[FtpServer] = None

  override def beforeAll(): Unit = {
    val serverFactory = new FtpServerFactory()
    val factory       = new ListenerFactory()
    // set the port of the listener
    factory.setPort(portNumber)

    val connectionConfigFactory = new ConnectionConfigFactory()
    connectionConfigFactory.setAnonymousLoginEnabled(true)

    serverFactory.setConnectionConfig(connectionConfigFactory.createConnectionConfig())

    val userManagerFactory = new PropertiesUserManagerFactory()
    userManagerFactory.setPasswordEncryptor(new SaltedPasswordEncryptor())
    val userManager = userManagerFactory.createUserManager()

    val user = new BaseUser()
    user.setName("test")
    user.setPassword("test")
    val dir =
      getClass.getResource("/com/wegtam/tensei/agent/parsers/NetworkFileParsers/secured/").getPath
    user.setHomeDirectory(dir)

    val user2 = new BaseUser()
    user2.setName("anonymous")
    user2.setHomeDirectory(dir)

    val auths = new util.ArrayList[Authority]()
    val auth  = new WritePermission()
    auths.add(auth)
    user.setAuthorities(auths)
    try {
      userManager.save(user)
      userManager.save(user2)
    } catch {
      case e: FtpException =>
        e.printStackTrace()
    }
    serverFactory.setUserManager(userManager)

    // replace the default listener
    serverFactory.addListener("default", factory.createListener())

    // start the server
    server = Option(serverFactory.createServer())
    try {
      server.get.start()
    } catch {
      case e: FtpException =>
        e.printStackTrace()
    }
  }

  override def afterAll(): Unit = {
    if (server.isDefined)
      server.get.stop()
    val _ = Await.result(system.terminate(), Duration.Inf)
  }

  describe("NetworkFileParserFTPTest") {
    describe("with FTP request") {
      describe("with anonymous FTP") {
        val data = new java.net.URI(s"ftp://localhost:$portNumber/contactssec.csv")
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
                                           Option("anonymous"))

        it("should parse upon request") {
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("NetworkFileParserFTPTest"), Set.empty[String])
          )
          val networkFileParser = TestActorRef(
            NetworkFileParser.props(source, cookbook, dataTree, Option("NetworkFileParserFTPTest"))
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

      describe("and simple user authentication") {
        val data = new java.net.URI(s"ftp://localhost:$portNumber/contactssec.csv")
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
                                           Option("test"))

        it("should parse upon request") {
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("NetworkFileParserFTPTest"), Set.empty[String])
          )
          val networkFileParser = TestActorRef(
            NetworkFileParser.props(source, cookbook, dataTree, Option("NetworkFileParserFTPTest"))
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

      describe("and a file in a subfolder") {
        val data = new java.net.URI(s"ftp://localhost:$portNumber/subfolder/contacts.csv")
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
                                           Option("test"))

        it("should parse upon request") {
          val dataTree = TestActorRef(
            DataTreeDocument.props(dfasdl, Option("NetworkFileParserFTPTest"), Set.empty[String])
          )
          val networkFileParser = TestActorRef(
            NetworkFileParser.props(source, cookbook, dataTree, Option("NetworkFileParserFTPTest"))
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
