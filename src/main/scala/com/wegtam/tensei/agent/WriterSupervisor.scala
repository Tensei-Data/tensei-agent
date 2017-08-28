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

import akka.actor.{ Actor, ActorLogging, ActorRef, Cancellable, Props, Terminated }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.wegtam.tensei.adt.{ ConnectionInformation, DFASDL }
import com.wegtam.tensei.agent.WriterSupervisor.PushFilter
import com.wegtam.tensei.agent.adt.{
  ConnectionType,
  ConnectionTypeDatabase,
  ConnectionTypeFile,
  ConnectionTypeFileFromNetwork
}
import com.wegtam.tensei.agent.helpers.{ LoggingHelpers, URIHelpers }
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages
import com.wegtam.tensei.agent.writers._

import scala.collection.{ immutable, SortedSet }
import scala.concurrent.duration._
import scalaz._

/**
  * The top actor for write operations.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  * @param dfasdl The dfasdl document describing the target data.
  * @param target The connection information for the target data sink.
  */
class WriterSupervisor(agentRunIdentifier: Option[String],
                       dfasdl: DFASDL,
                       target: ConnectionInformation)
    extends Actor
    with ActorLogging {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  val maxBufferEntries: Int =
    context.system.settings.config.getInt("tensei.agents.processor.filter-push-limits.max-entries")
  val pushInterval = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.processor.filter-push-limits.interval", MILLISECONDS),
    MILLISECONDS
  )
  import context.dispatcher
  val pushTimer: Cancellable =
    context.system.scheduler.schedule(pushInterval, pushInterval, self, PushFilter)

  // Buffer messages to avoid passing single messages through the filter pipe over and over.
  var messageBuffer: immutable.SortedSet[BaseWriterMessages.WriteData] =
    SortedSet.empty[BaseWriterMessages.WriteData]
  // An option to the actor ref of the filter that filters writer messages.
  var filter: Option[ActorRef] = None
  // An option to the actor ref of the actual writer that will write the messages to the target.
  var writer: Option[ActorRef] = None

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    val f = context.actorOf(BaseWriterFilter.props(agentRunIdentifier, dfasdl))
    context watch f
    filter = Option(f)
    super.preStart()
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    pushTimer.cancel()
    filter.foreach { f =>
      context unwatch f
      context stop f
    }
    writer.foreach { w =>
      context unwatch w
      context stop w
    }
    log.clearMDC()
    super.postStop()
  }

  override def receive: Receive = {
    case BaseWriterMessages.InitializeTarget =>
      log.debug("Received request to initialise target.")
      createWriterWorker(agentRunIdentifier, dfasdl, target) match {
        case -\/(e) => log.error(e, "An error occured while trying to create the writer!")
        case \/-(w) =>
          w ! BaseWriterMessages.InitializeTarget
          context watch w
          writer = Option(w)
      }
    case BaseWriterMessages.AreYouReady =>
      log.debug("Received AreYouReady request from {}.", sender().path)
      if (filter.isDefined)
        writer.foreach(w => w forward BaseWriterMessages.AreYouReady) // We need to have a filter and a writer ready...
    case Terminated(ref) =>
      filter.foreach(
        f =>
          if (f == ref) {
            log.error("Filter crashed! Restarting.")
            context stop self
        }
      )
      writer.foreach(
        w =>
          if (w == ref) {
            log.error("Writer crashed!")
            context stop self
        }
      )
    case BaseWriterMessages.WriteData(number, data, options, metaData) =>
      log.debug("Got write request.")
      messageBuffer = messageBuffer + BaseWriterMessages.WriteData(number, data, options, metaData)
      if (messageBuffer.size >= maxBufferEntries)
        pushAndRemoveMessages()
    case BaseWriterMessages.WriteBatchData(batch) =>
      log.debug("Got batch write request containing {} entries.", batch.size)
      messageBuffer = messageBuffer ++ batch
      if (messageBuffer.size >= maxBufferEntries)
        pushAndRemoveMessages()
    case PushFilter =>
      log.debug("Received push filter request.")
      pushAndRemoveMessages()
    case BaseWriterMessages.CloseWriter =>
      log.debug("Received close writer request from {}.", sender().path)
      filter.fold(log.error("No writer filter running!")) { f =>
        pushAndRemoveMessages()
        f forward BaseWriterMessages.CloseWriter
      }
      context become closing
  }

  def closing: Receive = {
    case Terminated(ref) =>
      log.debug("Received terminated message for {}.", ref.path)
      filter.foreach { f =>
        if (f == ref) {
          log.debug("Filter terminated at {}.", ref.path)
          writer.foreach { w =>
            context unwatch w
            w ! BaseWriterMessages.CloseWriter
          }
        }
      }
      writer.foreach(
        w =>
          if (w == ref) {
            log.error("Writer crashed!")
            context stop self
        }
      )
    case BaseWriterMessages.WriterClosed(status) =>
      log.debug("Received writer closed message from {}.", sender().path)
      context.parent forward BaseWriterMessages.WriterClosed(status)
      context stop self
  }

  /**
    * Tries to create a writer worker for the given target connection and dfasdl.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @param dfasdl The dfasdl document describing the target data.
    * @param target The connection information for the target data sink.
    * @return Either the [[akka.actor.ActorRef]] of the created writer or an error.
    */
  private def createWriterWorker(agentRunIdentifier: Option[String],
                                 dfasdl: DFASDL,
                                 target: ConnectionInformation): Throwable \/ ActorRef =
    \/.fromTryCatch(
      URIHelpers.connectionType(target.uri) match {
        case ConnectionTypeDatabase =>
          context.actorOf(DatabaseWriterActor.props(target, dfasdl, agentRunIdentifier))
        case ConnectionTypeFileFromNetwork =>
          context.actorOf(NetworkFileWriterActor.props(target, dfasdl, agentRunIdentifier))
        case ConnectionTypeFile =>
          if (target.uri.toString.endsWith(".json"))
            context.actorOf(JsonFileWriterActor.props(target, dfasdl, agentRunIdentifier))
          else
            context.actorOf(FileWriterActor.props(target, dfasdl, agentRunIdentifier))
        case t: ConnectionType =>
          throw new RuntimeException(s"No writer worker implemented for connection type $t!")
      }
    )

  /**
    * Push the messages from the buffer to the filter and reset the buffer.
    */
  private def pushAndRemoveMessages(): Unit =
    if (messageBuffer.nonEmpty)
      filter.fold(log.error("No writer filter running!")) { f =>
        log.debug("Pushing {} messages to filter.", messageBuffer.size) // DEBUG
        f ! BaseWriterFilter.Filter(messageBuffer, writer)
        messageBuffer = SortedSet.empty[BaseWriterMessages.WriteData]
      }

}

object WriterSupervisor {
  val name = "WriterSupervisor"

  /**
    * Create the ActorRef configuration object.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @param dfasdl The dfasdl document describing the target data.
    * @param target The connection information for the target data sink.
    * @return The props to create the actor.
    */
  def props(agentRunIdentifier: Option[String],
            dfasdl: DFASDL,
            target: ConnectionInformation): Props =
    Props(new WriterSupervisor(agentRunIdentifier, dfasdl, target))

  /**
    * Tell the actor to push the buffered messages into the filter queue.
    */
  case object PushFilter

}
