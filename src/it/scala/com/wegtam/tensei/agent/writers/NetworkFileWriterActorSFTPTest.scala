package com.wegtam.tensei.agent.writers

import java.io.{ File, FileNotFoundException }
import java.nio.file.{ Files, Path, Paths }
import java.util

import akka.testkit.TestFSMRef
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.helpers.NetworkFileWriterHelper
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages.{
  AreYouReady,
  ReadyToWork,
  WriteData
}
import com.wegtam.tensei.agent.writers.BaseWriter.{ BaseWriterMessages, WriterMessageMetaData }
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
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success, Try }
import scalaz.Scalaz._

class NetworkFileWriterActorSFTPTest extends ActorSpec with NetworkFileWriterHelper {
  val contacts = Seq(
    "Max Mustermann",
    "Fritz Mustermann",
    "Helmut Mustermann",
    "Brigitte Musterfrau"
  )
  val portNumber: Int         = ActorSpec.findAvailablePort()
  var sshd: Option[SshServer] = None
  val USERNAME: String        = "username"
  val PASSWORD: String        = "password"

  var serverDirectory: Option[Path] = None

  private def setupSSHServer(): Unit = {
    val temporaryHostKEy: File = java.io.File.createTempFile("tensei-agent-sftp-test", ".tmp")
    val sshd: SshServer        = SshServer.setUpDefaultServer()
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

    val dir = Files.createTempDirectory("nwfwasftptest")
    sshd.setFileSystemFactory(
      new VirtualFileSystemFactory(
        dir.toAbsolutePath
      )
    )
    serverDirectory = Option(dir)
    sshd.start()
    this.sshd = Option(sshd)
  }

  override def beforeAll(): Unit =
    setupSSHServer()

  override def afterAll(): Unit = {
    if (sshd.isDefined)
      sshd.get.stop()
    val _ = Await.result(system.terminate(), Duration.Inf)
  }

  describe("NetworkFileWriterActorSFTPTest") {
    describe("with SFTP request") {
      val uri = new java.net.URI(s"sftp://localhost:$portNumber/contacts-target.csv")

      it("should answer after it has initialized") {
        val dfasdl = DFASDL(
          "SIMPLE-DFASDL",
          scala.io.Source
            .fromInputStream(
              getClass.getResourceAsStream(
                "/com/wegtam/tensei/agent/writers/NetworkFileWriters/contacts-dfasdl.xml"
              )
            )
            .mkString
        )
        val cookbook = Cookbook("COOKBOOK", List(dfasdl), Option(dfasdl), List.empty[Recipe])
        val con = new ConnectionInformation(uri,
                                            Option(DFASDLReference(cookbook.id, dfasdl.id)),
                                            Option(USERNAME),
                                            Option(PASSWORD))

        val writer =
          TestFSMRef(new NetworkFileWriterActor(con, dfasdl, Option("NetworkFileWriterTest")))
        writer.stateName should be(BaseWriter.State.Initializing)
        writer ! BaseWriterMessages.InitializeTarget
        writer ! AreYouReady
        val expectedMsg = ReadyToWork
        expectMsg(expectedMsg)
        writer ! BaseWriterMessages.CloseWriter
        expectMsg(BaseWriterMessages.WriterClosed("".right[String]))
      }

      describe("and simple user authentication") {
        it("should create the file") {
          val dfasdl = DFASDL(
            "SIMPLE-DFASDL",
            scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream(
                  "/com/wegtam/tensei/agent/writers/NetworkFileWriters/contacts-dfasdl.xml"
                )
              )
              .mkString
          )
          val cookbook = Cookbook(
            "COOKBOOK",
            List(dfasdl),
            Option(dfasdl),
            List(
              Recipe(
                "foo",
                Recipe.MapOneToOne,
                List(
                  MappingTransformation(List(ElementReference(dfasdl.id, "name")),
                                        List(ElementReference(dfasdl.id, "name")),
                                        List(),
                                        List(),
                                        None)
                )
              )
            )
          )
          val con = new ConnectionInformation(uri,
                                              Option(DFASDLReference(cookbook.id, dfasdl.id)),
                                              Option(USERNAME),
                                              Option(PASSWORD))
          val writer =
            TestFSMRef(
              new NetworkFileWriterActor(con, dfasdl, Option("NetworkFileWriterActorSFTPTest"))
            )
          writer.stateName should be(BaseWriter.State.Initializing)
          writer ! BaseWriterMessages.InitializeTarget
          writer ! AreYouReady
          expectMsg(ReadyToWork)

          contacts.zipWithIndex.foreach {
            case (contact, index) =>
              writer ! WriteData(number = index.toLong,
                                 data = contact,
                                 metaData = Option(WriterMessageMetaData(id = "name")))
          }

          writer ! BaseWriterMessages.CloseWriter
          expectMsg(BaseWriterMessages.WriterClosed("".right[String]))
          testFile(uri.getPath, contacts) match {
            case Success(b) => b should be(true)
            case Failure(f) => fail(f)
          }
        }
      }
    }
  }

  def testFile(filepath: String, contacts: Seq[String]): Try[Boolean] = {
    Thread.sleep(1000)
    serverDirectory.fold[Try[Boolean]](Failure(new FileNotFoundException("No server directory!"))) {
      dir =>
        Try {
          val fp      = Paths.get(dir.toString, filepath)
          val content = scala.io.Source.fromFile(fp.toFile).getLines().mkString("\n")
          content should be(contacts.mkString("\n"))
          true
        }
    }
  }

}
