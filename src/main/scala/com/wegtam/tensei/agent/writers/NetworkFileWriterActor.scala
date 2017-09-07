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

import java.io.File
import java.net.InetAddress

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, FSM, Props, Terminated }
import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import akka.stream.alpakka.ftp.RemoteFileSettings.{ FtpSettings, FtpsSettings }
import akka.stream.alpakka.ftp.scaladsl.{ Ftp, Ftps, Sftp }
import akka.stream.alpakka.ftp.{ FtpCredentials, SftpSettings }
import akka.stream.scaladsl.{ Flow, Sink, Source }
import akka.stream.{ ActorMaterializer, IOResult, OverflowStrategy }
import akka.util.ByteString
import com.wegtam.tensei.adt.{ ConnectionInformation, DFASDL }
import com.wegtam.tensei.agent.adt.ConnectionTypeFileFromNetwork
import com.wegtam.tensei.agent.helpers.{ LoggingHelpers, NetworkFileWriterHelper, URIHelpers }
import com.wegtam.tensei.agent.processor.UniqueValueBuffer
import com.wegtam.tensei.agent.processor.UniqueValueBuffer.UniqueValueBufferMessages
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages.{ AreYouReady, ReadyToWork }
import com.wegtam.tensei.agent.writers.BaseWriter.State.{ Closing, Initializing, Working }
import com.wegtam.tensei.agent.writers.BaseWriter.{
  BaseWriterMessages,
  DEFAULT_CHARSET,
  DEFAULT_STOP_SIGN,
  SKIP_STOP_SIGN_OPTION
}
import com.wegtam.tensei.agent.writers.FileWriterActor.FileWriterActorMessages.CloseResources
import com.wegtam.tensei.agent.writers.NetworkFileWriterActor.NetworkConnectionType.{
  FtpConnection,
  FtpsConnection,
  SftpConnection
}
import com.wegtam.tensei.agent.writers.NetworkFileWriterActor.{
  NetworkConnectionType,
  NetworkFileWriterData
}
import org.dfasdl.utils.{ AttributeNames, DocumentHelpers }
import org.w3c.dom.traversal.{ DocumentTraversal, NodeFilter }
import org.w3c.dom.{ Document, Element }

import scala.collection.SortedSet
import scala.concurrent.Future
import scala.concurrent.duration.{ FiniteDuration, MILLISECONDS }
import scalaz.Scalaz._

object NetworkFileWriterActor {

  /**
    * Helper method to create a network file writer actor.
    *
    * @param target              The connection information for the target data sink.
    * @param dfasdl              The dfasdl describing the target file. It is needed to write sequence columns in the correct order.
    * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
    * @return The props to generate the actor.
    */
  def props(target: ConnectionInformation,
            dfasdl: DFASDL,
            agentRunIdentifier: Option[String]): Props =
    Props(new NetworkFileWriterActor(target, dfasdl, agentRunIdentifier))

  sealed trait NetworkFileWriterActorMessages

  object NetworkFileWriterActorMessages {
    case object CloseResources extends NetworkFileWriterActorMessages
  }

  /**
    * A class that buffers the state of the file writer.
    *
    * @param closeRequester An option to the actor ref that requested the closing of the writer.
    * @param messages       The message buffer with the already received writer messages.
    * @param readyRequests  A list of actor refs that have asked if we are ready to work.
    * @param writer         An actor that writes the messages to the target.
    */
  final case class NetworkFileWriterData(
      closeRequester: Option[ActorRef],
      messages: SortedSet[BaseWriterMessages.WriteData],
      readyRequests: List[ActorRef],
      writer: Option[ActorRef]
  )

  sealed trait NetworkConnectionType

  object NetworkConnectionType {
    case object FtpConnection extends NetworkConnectionType

    case object FtpsConnection extends NetworkConnectionType

    case object SftpConnection extends NetworkConnectionType
  }
}

class NetworkFileWriterActor(target: ConnectionInformation,
                             dfasdl: DFASDL,
                             agentRunIdentifier: Option[String])
    extends BaseWriter(target = target)
    with Actor
    with FSM[BaseWriter.State, NetworkFileWriterData]
    with ActorLogging
    with BaseWriterFunctions
    with DocumentHelpers
    with NetworkFileWriterHelper {
  // Create a distributed pub sub mediator.
  import DistributedPubSubMediator.Publish
  val mediator: ActorRef = DistributedPubSub(context.system).mediator

  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  implicit val actorSystem: ActorSystem        = context.system
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  lazy val writeTriggerInterval = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.writers.network.write-interval", MILLISECONDS),
    MILLISECONDS
  )
  setTimer("writeTrigger",
           BaseWriterMessages.WriteBufferedData,
           writeTriggerInterval,
           repeat = true)

  lazy val dfasdlDocument: Document = createNormalizedDocument(dfasdl.content)
  lazy val orderedDataElementIds: Vector[String] =
    try {
      val traversal = dfasdlDocument.asInstanceOf[DocumentTraversal]
      val iterator = traversal.createNodeIterator(dfasdlDocument.getDocumentElement,
                                                  NodeFilter.SHOW_ELEMENT,
                                                  new DataElementFilter(),
                                                  true)
      val builder  = Vector.newBuilder[String]
      var nextNode = iterator.nextNode()
      while (nextNode != null) {
        builder += nextNode.asInstanceOf[Element].getAttribute("id")
        nextNode = iterator.nextNode()
      }
      builder.result()
    } catch {
      case e: Throwable =>
        log.error(
          e,
          "An error occurred while trying to calculate the ordered target data element ids!"
        )
        Vector.empty[String]
    }
  lazy val uniqueDataElementIds: Set[String] =
    getUniqueDataElements(dfasdlDocument).map(_.getAttribute("id"))

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.clearMDC()
    cancelTimer("writeTrigger")
    super.postStop()
  }

  startWith(
    Initializing,
    NetworkFileWriterData(closeRequester = None,
                          messages = SortedSet.empty[BaseWriterMessages.WriteData],
                          readyRequests = List.empty[ActorRef],
                          writer = None)
  )

  when(Initializing) {
    case Event(BaseWriterMessages.InitializeTarget, data) =>
      val ftpWriter: Option[ActorRef] =
        URIHelpers.connectionType(target.uri) match {
          case ConnectionTypeFileFromNetwork =>
            val connectionType = getConnectionType(target)
            val credentials    = getFtpCredentials(target)
            val host           = InetAddress.getByName(target.uri.getHost)
            val port           = target.uri.getPort
            val takePort =
              if (port > 0) port
              else 21

            val byteSource: Source[ByteString, ActorRef] =
              Source.actorRef[ByteString](Int.MaxValue, OverflowStrategy.fail)
            val ftpConnection: Sink[ByteString, Future[IOResult]] =
              defineFtpConnection(connectionType, target, host, takePort, credentials)

            Option(
              Flow[ByteString].to(ftpConnection).runWith(byteSource)
            )
          case _ =>
            log.error("NetworkFileWriter not implemented for connection: {}", target.uri)
            None
        }

      goto(Working) using data.copy(
        writer = ftpWriter
      )
    case Event(AreYouReady, data) =>
      stay() using data.copy(readyRequests = sender() :: data.readyRequests)
  }

  when(Working) {
    case Event(msg: BaseWriterMessages.WriteData, data) =>
      log.debug("Got write request.")
      stay() using data.copy(messages = data.messages + msg)
    case Event(msg: BaseWriterMessages.WriteBatchData, data) =>
      log.debug("Got bulk write request containing {} messages.", msg.batch.size)
      stay() using data.copy(messages = data.messages ++ msg.batch)
    case Event(BaseWriterMessages.WriteBufferedData, data) =>
      log.debug("Received write buffered data request.")
      data.writer.fold(log.error("No network file writer defined!"))(
        w => writeMessages(w, data.messages)
      )
      stay() using data.copy(messages = SortedSet.empty[BaseWriterMessages.WriteData])
    case Event(AreYouReady, data) =>
      sender() ! ReadyToWork
      stay() using data
    case Event(BaseWriterMessages.CloseWriter, data) =>
      log.debug("Got close request for NetworkFileWriter.")
      data.writer.fold(log.error("No network file writer defined!"))(
        w => writeMessages(w, data.messages)
      )
      self ! CloseResources
      goto(Closing) using data.copy(closeRequester = Option(sender()))
  }

  when(Closing) {
    case Event(CloseResources, data) =>
      data.writer.foreach { w =>
        w ! akka.actor.Status.Success("Success".getBytes)
        context.watch(w)
      }
      stay() using data
    case Event(Terminated(ref), data) =>
      if (data.closeRequester.isDefined)
        data.closeRequester.get ! BaseWriterMessages.WriterClosed("".right[String])
      stay() using data
  }

  onTransition {
    case _ -> Working => nextStateData.readyRequests foreach (a => a ! ReadyToWork)
  }

  whenUnhandled {
    case Event(msg: BaseWriterMessages.WriteData, data) =>
      log.warning("Got unhandled network writer message!")
      stay() using data
    case Event(msg: BaseWriterMessages.WriteBatchData, data) =>
      log.warning("Got unhandled bulk network writer message!")
      stay() using data
  }

  initialize()

  /**
    * Initialize the target.
    *
    * @return Returns `true` upon success and `false` if an error occurred.
    */
  override def initializeTarget: Boolean = {
    val file = new File(target.uri.getSchemeSpecificPart)
    file.createNewFile()
  }

  private def defineFtpConnection(
      connectionType: NetworkConnectionType,
      target: ConnectionInformation,
      host: InetAddress,
      port: Int,
      credentials: FtpCredentials
  ): Sink[ByteString, Future[IOResult]] = {
    val path = target.uri.getPath
    connectionType match {
      case FtpConnection =>
        val settings =
          FtpSettings(
            host,
            port,
            credentials,
            binary = true,
            passiveMode = false
          )
        Ftp.toPath(path, settings, append = true)
      case FtpsConnection =>
        val settings =
          FtpsSettings(
            host,
            port,
            credentials,
            binary = true,
            passiveMode = false
          )
        Ftps.toPath(path, settings, append = true)
      case SftpConnection =>
        val settings =
          SftpSettings(
            host,
            port,
            credentials,
            strictHostKeyChecking = false
          )
        Sftp.toPath(path, settings, append = true)
      case _ => throw new RuntimeException(s"Connection type not implemented $connectionType")
    }
  }

  /**
    * Analyze the stopSign and write specific characters not from the variable but as defined String.
    *
    * @param stopSign  The given stopSign of the data element.
    * @param writer    The network file writer.
    * @param charset   The name of the charset to use.
    */
  private def analyzeStopSign(stopSign: String, writer: ActorRef, charset: String): Unit = {
    var part = ""
    for (i <- 1 to stopSign.length) {
      part = part + stopSign.charAt(i - 1)
      if (part.startsWith("\\")) {
        if (part.length == 2) {
          // Tab
          if (part.equals("\\t"))
            writer ! ByteString("\t".getBytes(charset))
          else
            writer ! ByteString(part.getBytes(charset))
          part = ""
        }
      }
      // Stop sign that does not start with a backslash
      else {
        writer ! ByteString(part.getBytes(charset))
        part = ""
      }
    }
  }

  /**
    * Process a batch of `WriterMessage`s and write them to the target filewriter.
    *
    * @param target The filewriter that should consume the data.
    * @param messages A list of `WriterMessage`s.
    */
  private def writeMessages(target: ActorRef,
                            messages: SortedSet[BaseWriterMessages.WriteData]): Unit = {
    val uniqueValues = getUniqueMessageValues(dfasdl, messages, uniqueDataElementIds)
    messages.foreach(msg => writeMessage(target, msg))
    uniqueValues.foreach(
      p =>
        mediator ! Publish(UniqueValueBuffer.UNIQUE_VALUE_BUFFER_CHANNEL,
                           UniqueValueBufferMessages.StoreS(p._1, p._2))
    )
  }

  /**
    * Pass the given data into a filewrite.
    *
    * @param target The filewriter that should consume the data.
    * @param message The `WriterMessage` containing the data and possible options.
    */
  private def writeMessage(target: ActorRef, message: BaseWriterMessages.WriteData): Unit = {
    val stopSign: Option[String] = getOption(AttributeNames.STOP_SIGN, message.options)
    val charset: Option[String]  = getOption(AttributeNames.ENCODING, message.options)

    val bs: Array[Byte] =
      message.data match {
        case binary: Array[Byte] => binary
        case byteString: ByteString =>
          byteString.utf8String.getBytes(charset.getOrElse(DEFAULT_CHARSET))
        case date: java.sql.Date =>
          date.toString.getBytes(charset.getOrElse(DEFAULT_CHARSET))
        case date: java.time.LocalDate =>
          date.toString.getBytes(charset.getOrElse(DEFAULT_CHARSET))
        case decimal: java.math.BigDecimal =>
          decimal.toPlainString.getBytes(charset.getOrElse(DEFAULT_CHARSET))
        case number: Number =>
          number.toString.getBytes(charset.getOrElse(DEFAULT_CHARSET))
        case string: String => string.getBytes(charset.getOrElse(DEFAULT_CHARSET))
        case time: java.sql.Time =>
          time.toString.getBytes(charset.getOrElse(DEFAULT_CHARSET))
        case time: java.time.LocalTime =>
          time.toString.getBytes(charset.getOrElse(DEFAULT_CHARSET))
        case timestamp: java.sql.Timestamp =>
          timestamp.toString.getBytes(charset.getOrElse(DEFAULT_CHARSET))
        case timestamp: java.time.OffsetDateTime =>
          timestamp.toString.getBytes(charset.getOrElse(DEFAULT_CHARSET))
        case None => Array.empty[Byte]
        case _ =>
          log.warning("Using generic writer algorithm for unsupported data format {}!",
                      message.data.getClass)
          message.data.toString.getBytes(charset.getOrElse(DEFAULT_CHARSET))
      }
    target ! ByteString(bs)

    if (getOption(SKIP_STOP_SIGN_OPTION, message.options).isEmpty)
      analyzeStopSign(stopSign.getOrElse(DEFAULT_STOP_SIGN),
                      target,
                      charset.getOrElse(DEFAULT_CHARSET))
  }
}
