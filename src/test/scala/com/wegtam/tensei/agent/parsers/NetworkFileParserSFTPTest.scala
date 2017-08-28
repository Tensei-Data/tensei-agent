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

import java.nio.file.Paths
import java.util

import akka.testkit.TestActorRef
import akka.util.{ ByteString, Timeout }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.adt.{ ParserStatus, ParserStatusMessage }
import com.wegtam.tensei.agent.{ ActorSpec, DataTreeDocument, XmlTestHelpers }
import org.apache.sshd.common.NamedFactory
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory
import org.apache.sshd.server.auth.UserAuth
import org.apache.sshd.server.auth.password.PasswordAuthenticator
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider
import org.apache.sshd.server.scp.ScpCommandFactory
import org.apache.sshd.server.session.ServerSession
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory
import org.apache.sshd.server.{ Command, SshServer }

import scala.concurrent.Await
import scala.concurrent.duration._

class NetworkFileParserSFTPTest extends ActorSpec with XmlTestHelpers {
  val portNumber: Int         = ActorSpec.findAvailablePort()
  var sshd: Option[SshServer] = None
  val USERNAME: String        = "username"
  val PASSWORD: String        = "password"

  private def setupSSHServer(): Unit = {
    val temporaryHostKEy = java.io.File.createTempFile("tensei-agent-test", ".tmp")
    val sshd             = SshServer.setUpDefaultServer()
    sshd.setPort(portNumber)
    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(temporaryHostKEy))

    val userAuthFactories = new util.ArrayList[NamedFactory[UserAuth]]()
    sshd.setUserAuthFactories(userAuthFactories)

    sshd.setCommandFactory(new ScpCommandFactory())
    val namedFactoryList = new util.ArrayList[NamedFactory[Command]]()
    val sf               = new SftpSubsystemFactory.Builder().build()
    namedFactoryList.add(sf)
    sshd.setSubsystemFactories(namedFactoryList)

    sshd.setPasswordAuthenticator(new PasswordAuthenticator() {
      def authenticate(username: String, password: String, session: ServerSession): Boolean =
        USERNAME.equals(username) && PASSWORD.equals(password)
    })
    sshd.setFileSystemFactory(
      new VirtualFileSystemFactory(
        Paths.get(
          getClass
            .getResource("/com/wegtam/tensei/agent/parsers/NetworkFileParsers/secured/")
            .getPath
        )
      )
    )

    try {
      sshd.start()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  override def beforeAll(): Unit =
    setupSSHServer()

  override def afterAll(): Unit = {
    if (sshd.isDefined)
      sshd.get.stop()
    val _ = Await.result(system.terminate(), Duration.Inf)
  }

  describe("NetworkFileParserSFTPTest") {
    describe("with port number") {
      val data = new java.net.URI(s"sftp://localhost:$portNumber/contactssec.csv")
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
                                         Option(USERNAME),
                                         Option(PASSWORD))

      it("should parse upon request") {
        val dataTree = TestActorRef(
          DataTreeDocument.props(dfasdl, Option("NetworkFileParserSFTPTest"), Set.empty[String])
        )
        val networkFileParser = TestActorRef(
          NetworkFileParser.props(source, cookbook, dataTree, Option("NetworkFileParserSFTPTest"))
        )

        implicit val timeout: Timeout = Timeout(5.seconds)

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

    describe("with subfolder") {
      val data = new java.net.URI(s"sftp://localhost:$portNumber/subfolder/contacts.csv")
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
                                         Option(USERNAME),
                                         Option(PASSWORD))

      it("should parse upon request") {
        val dataTree = TestActorRef(
          DataTreeDocument.props(dfasdl, Option("NetworkFileParserSFTPTest"), Set.empty[String])
        )
        val networkFileParser = TestActorRef(
          NetworkFileParser.props(source, cookbook, dataTree, Option("NetworkFileParserSFTPTest"))
        )

        implicit val timeout: Timeout = Timeout(5.seconds)

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
