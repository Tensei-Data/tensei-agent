package com.wegtam.tensei.agent.writers

import java.io.FileNotFoundException
import java.nio.file.{Files, Path, Paths}
import java.util

import akka.testkit.TestFSMRef
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.helpers.NetworkFileWriterHelper
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages.{AreYouReady, ReadyToWork, WriteData}
import com.wegtam.tensei.agent.writers.BaseWriter.{BaseWriterMessages, WriterMessageMetaData}
import org.apache.ftpserver.ftplet.Authority
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.impl.{BaseUser, WritePermission}
import org.apache.ftpserver.usermanager.{PropertiesUserManagerFactory, SaltedPasswordEncryptor}
import org.apache.ftpserver.{ConnectionConfigFactory, FtpServer, FtpServerFactory}
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scalaz.Scalaz._

class NetworkFileWriterActorFTPTest extends ActorSpec with BeforeAndAfterEach with NetworkFileWriterHelper {

  val portNumber: Int = ActorSpec.findAvailablePort()
  var server: Option[FtpServer] = None
  var serverDirectory: Option[Path] = None

  val contacts = Seq(
    "Max Mustermann",
    "Fritz Mustermann",
    "Helmut Mustermann",
    "Brigitte Musterfrau"
  )

  override def beforeEach(): Unit = {
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

    val dir = Files.createTempDirectory("nwfwaftptest")
    user.setHomeDirectory(dir.toAbsolutePath.toString)

    val user2 = new BaseUser()
    user2.setName("anonymous")
    user2.setHomeDirectory(dir.toAbsolutePath.toString)

    val auths = new util.ArrayList[Authority]()
    val auth  = new WritePermission()
    auths.add(auth)
    user.setAuthorities(auths)
    user2.setAuthorities(auths)
      userManager.save(user)
      userManager.save(user2)
    serverFactory.setUserManager(userManager)

    // replace the default listener
    serverFactory.addListener("default", factory.createListener())

    // start the server
    server = Option(serverFactory.createServer())
    server.foreach(_.start())
    serverDirectory = Option(dir)
  }

  override def afterEach(): Unit = {
    server.foreach(_.stop())
  }

  override def afterAll(): Unit = {
    val _ = Await.result(system.terminate(), Duration.Inf)
  }

  describe("NetworkFileWriterActorFTPTest") {
    describe("with FTP request") {
      val uri = new java.net.URI(s"ftp://localhost:$portNumber/contacts-target.csv")

      it("should answer after it has initialized") {
        val con = new ConnectionInformation(uri, None)
        val writer =
          TestFSMRef(new NetworkFileWriterActor(con, DFASDL("TEST", ""), Option("NetworkFileWriterTest")))
        writer.stateName should be(BaseWriter.State.Initializing)
        writer ! AreYouReady
        writer ! BaseWriterMessages.InitializeTarget
        val expectedMsg = ReadyToWork
        expectMsg(expectedMsg)
        writer ! BaseWriterMessages.CloseWriter
      }

      describe("with anonymous FTP") {
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
          val cookbook = Cookbook("COOKBOOK", List(dfasdl), Option(dfasdl), List.empty[Recipe])
          val con = new ConnectionInformation(uri, Option(DFASDLReference(cookbook.id, dfasdl.id)))
          val writer =
            TestFSMRef(new NetworkFileWriterActor(con, dfasdl, Option("NetworkFileWriterTest")))
          writer.stateName should be(BaseWriter.State.Initializing)
          writer ! BaseWriterMessages.InitializeTarget
          writer ! AreYouReady
          val expectedMsg = ReadyToWork
          expectMsg(expectedMsg)

          contacts.zipWithIndex.foreach {case (contact, index) =>
            writer ! WriteData(number = index.toLong,
              data = contact,
              metaData = Option(WriterMessageMetaData(id = "name")))
          }

          writer ! BaseWriterMessages.CloseWriter
          expectMsg(BaseWriterMessages.WriterClosed("".right[String]))

          testFile(uri.getPath, contacts) match {
            case Success(b) => b should be(true)
            case Failure(f) =>
              f.printStackTrace()
              fail(f)
          }
        }
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
          val cookbook = Cookbook("COOKBOOK", List(dfasdl), Option(dfasdl), List.empty[Recipe])
          val con = new ConnectionInformation(uri, Option(DFASDLReference(cookbook.id, dfasdl.id)), Option("test"), Option("test"))
          val writer =
            TestFSMRef(new NetworkFileWriterActor(con, dfasdl, Option("NetworkFileWriterTest")))
          writer.stateName should be(BaseWriter.State.Initializing)
          writer ! BaseWriterMessages.InitializeTarget
          writer ! AreYouReady
          val expectedMsg = ReadyToWork
          expectMsg(expectedMsg)

          contacts.zipWithIndex.foreach {case (contact, index) =>
            writer ! WriteData(number = index.toLong,
              data = contact,
              metaData = Option(WriterMessageMetaData(id = "name")))
          }

          writer ! BaseWriterMessages.CloseWriter
          expectMsg(BaseWriterMessages.WriterClosed("".right[String]))

          testFile(uri.getPath, contacts) match {
            case Success(b) => b should be(true)
            case Failure(f) =>
              f.printStackTrace()
              fail(f)
          }
        }
      }
    }
  }

  def testFile(filepath: String, contacts: Seq[String]): Try[Boolean] = {
    Thread.sleep(5000)
    serverDirectory.fold[Try[Boolean]](Failure(new FileNotFoundException("No server directory!"))) { dir =>
      Try {
        val fp = Paths.get(dir.toAbsolutePath.toString, filepath)
        val content = scala.io.Source.fromFile(fp.toFile, "UTF-8").getLines().mkString("\n")
        content should be(contacts.mkString("\n"))
        true
      }
    }
  }

}
