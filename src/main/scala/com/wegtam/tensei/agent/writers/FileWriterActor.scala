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

import java.io._
import java.nio.file.{ Files, StandardOpenOption }

import akka.actor._
import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import akka.util.ByteString
import com.wegtam.tensei.adt.{ ConnectionInformation, DFASDL }
import com.wegtam.tensei.agent.helpers.LoggingHelpers
import com.wegtam.tensei.agent.processor.UniqueValueBuffer
import com.wegtam.tensei.agent.processor.UniqueValueBuffer.UniqueValueBufferMessages
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages.{ AreYouReady, ReadyToWork }
import com.wegtam.tensei.agent.writers.BaseWriter.State.{ Closing, Initializing, Working }
import com.wegtam.tensei.agent.writers.BaseWriter._
import com.wegtam.tensei.agent.writers.FileWriterActor.FileWriterActorMessages.CloseResources
import com.wegtam.tensei.agent.writers.FileWriterActor.FileWriterData
import org.dfasdl.utils.{ AttributeNames, DocumentHelpers }
import org.w3c.dom.{ Document, Element }
import org.w3c.dom.traversal.{ DocumentTraversal, NodeFilter }

import scala.collection.SortedSet
import scala.concurrent.duration._
import scalaz.Scalaz._

object FileWriterActor {

  /**
    * Helper method to create a file writer actor.
    *
    * @param target              The connection information for the target data sink.
    * @param dfasdl              The dfasdl describing the target file. It is needed to write sequence columns in the correct order.
    * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
    * @return The props to generate the actor.
    */
  def props(target: ConnectionInformation,
            dfasdl: DFASDL,
            agentRunIdentifier: Option[String]): Props =
    Props(classOf[FileWriterActor], target, dfasdl, agentRunIdentifier)

  sealed trait FileWriterActorMessages

  object FileWriterActorMessages {
    case object CloseResources extends FileWriterActorMessages
  }

  /**
    * A class that buffers the state of the file writer.
    *
    * @param closeRequester An option to the actor ref that requested the closing of the writer.
    * @param messages       The message buffer with the already received writer messages.
    * @param readyRequests  A list of actor refs that have asked if we are ready to work.
    * @param writer         A java file writer.
    */
  final case class FileWriterData(
      closeRequester: Option[ActorRef],
      messages: SortedSet[BaseWriterMessages.WriteData],
      readyRequests: List[ActorRef],
      writer: Option[OutputStream]
  )
}

/**
  * An actor that writes given informations into a file.
  *
  * @param target              The connection information for the target data sink.
  * @param dfasdl              The dfasdl describing the target file. It is needed to write sequence columns in the correct order.
  * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
  */
class FileWriterActor(target: ConnectionInformation,
                      dfasdl: DFASDL,
                      agentRunIdentifier: Option[String])
    extends BaseWriter(target = target)
    with Actor
    with FSM[BaseWriter.State, FileWriterData]
    with ActorLogging
    with BaseWriterFunctions
    with DocumentHelpers {
  // Create a distributed pub sub mediator.
  import DistributedPubSubMediator.Publish
  val mediator = DistributedPubSub(context.system).mediator

  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  lazy val writeTriggerInterval = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.writers.file.write-interval", MILLISECONDS),
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
    FileWriterData(closeRequester = None,
                   messages = SortedSet.empty[BaseWriterMessages.WriteData],
                   readyRequests = List.empty[ActorRef],
                   writer = None)
  )

  when(Initializing) {
    case Event(BaseWriterMessages.InitializeTarget, data) =>
      initializeTarget
      goto(Working) using data.copy(
        writer = Option(
          Files.newOutputStream(new File(target.uri.getSchemeSpecificPart).toPath,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.APPEND)
        )
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
      data.writer.fold(log.error("No file writer defined!"))(w => writeMessages(w, data.messages))
      stay() using data.copy(messages = SortedSet.empty[BaseWriterMessages.WriteData])
    case Event(AreYouReady, data) =>
      sender() ! ReadyToWork
      stay() using data
    case Event(BaseWriterMessages.CloseWriter, data) =>
      log.debug("Got close request for FileWriter.")
      data.writer.fold(log.error("No file writer defined!"))(w => writeMessages(w, data.messages))
      self ! CloseResources
      goto(Closing) using data.copy(closeRequester = Option(sender()))
  }

  when(Closing) {
    case Event(CloseResources, data) =>
      data.writer.foreach(w => w.close())
      if (data.closeRequester.isDefined)
        data.closeRequester.get ! BaseWriterMessages.WriterClosed("".right[String])
      stay() using data
  }

  onTransition {
    case _ -> Working => nextStateData.readyRequests foreach (a => a ! ReadyToWork)
  }

  whenUnhandled {
    case Event(msg: BaseWriterMessages.WriteData, data) =>
      log.warning("Got unhandled writer message!")
      stay() using data
    case Event(msg: BaseWriterMessages.WriteBatchData, data) =>
      log.warning("Got unhandled bulk writer message!")
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

  /**
    * Analyze the stopSign and write specific characters not from the variable but as defined String.
    *
    * @param stopSign  The given stopSign of the data element.
    * @param writer    The file writer.
    * @param charset   The name of the charset to use.
    */
  private def analyzeStopSign(stopSign: String, writer: OutputStream, charset: String): Unit = {
    var part = ""
    for (i <- 1 to stopSign.length) {
      part = part + stopSign.charAt(i - 1)
      if (part.startsWith("\\")) {
        if (part.length == 2) {
          // Tab
          if (part.equals("\\t")) {
            writer.write("\t".getBytes(charset))
          } else
            writer.write(part.getBytes(charset))
          part = ""
        }
      }
      // Stop sign that does not start with a backslash
      else {
        writer.write(part.getBytes(charset))
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
  private def writeMessages(target: OutputStream,
                            messages: SortedSet[BaseWriterMessages.WriteData]): Unit = {
    val uniqueValues = getUniqueMessageValues(dfasdl, messages, uniqueDataElementIds)
    messages.foreach(msg => writeMessage(target, msg))
    target.flush()
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
  private def writeMessage(target: OutputStream, message: BaseWriterMessages.WriteData): Unit = {
    val stopSign = getOption(AttributeNames.STOP_SIGN, message.options)
    val charset  = getOption(AttributeNames.ENCODING, message.options)

    message.data match {
      case binary: Array[Byte] => target.write(binary)
      case byteString: ByteString =>
        target.write(byteString.utf8String.getBytes(charset.getOrElse(DEFAULT_CHARSET)))
      case date: java.sql.Date =>
        target.write(date.toString.getBytes(charset.getOrElse(DEFAULT_CHARSET)))
      case decimal: java.math.BigDecimal =>
        target.write(decimal.toPlainString.getBytes(charset.getOrElse(DEFAULT_CHARSET)))
      case number: Number =>
        target.write(number.toString.getBytes(charset.getOrElse(DEFAULT_CHARSET)))
      case string: String => target.write(string.getBytes(charset.getOrElse(DEFAULT_CHARSET)))
      case time: java.sql.Time =>
        target.write(time.toString.getBytes(charset.getOrElse(DEFAULT_CHARSET)))
      case timestamp: java.sql.Timestamp =>
        target.write(timestamp.toString.getBytes(charset.getOrElse(DEFAULT_CHARSET)))
      case None => target.write(Array.empty[Byte])
      case _ =>
        log.warning("Using generic writer algorithm for unsupported data format {}!",
                    message.data.getClass)
        target.write(message.data.toString.getBytes(charset.getOrElse(DEFAULT_CHARSET)))
    }

    if (getOption(SKIP_STOP_SIGN_OPTION, message.options).isEmpty)
      analyzeStopSign(stopSign.getOrElse(DEFAULT_STOP_SIGN),
                      target,
                      charset.getOrElse(DEFAULT_CHARSET))
  }
}
