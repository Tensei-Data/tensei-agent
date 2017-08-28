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

import java.io.{ BufferedReader, InputStreamReader }
import java.nio.file.{ FileSystems, Files, Path }

import akka.actor._
import com.wegtam.tensei.adt.{ GlobalMessages, StatusMessage, StatusType }

import scalaz._

/**
  * A simple actor that reports log file meta data and starts workers
  * that pipe requested log data to the requester.
  *
  * It stops itself after processing or if an error occurred.
  */
class LogReporter extends Actor with ActorLogging {
  val logDirName = context.system.settings.config.getString("tensei.agent.logdir")
  val agentId    = TenseiAgentApp.checkAndCreateId()

  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  override def receive: Receive = {
    case GlobalMessages.RequestAgentRunLogsMetaData(uuid) =>
      log.debug("Got request to report meta data of run {}.", uuid)
      \/.fromTryCatch(FileSystems.getDefault.getPath(logDirName, s"$uuid.log").toAbsolutePath) match {
        case -\/(failure) =>
          log.error(failure, "Could not determine file system path!")
          sender() ! GlobalMessages.ErrorOccured(
            StatusMessage(
              reporter = Option(self.path.toString),
              message = "Could not determine file system path!",
              statusType = StatusType.FatalError,
              cause = None
            )
          )
        case \/-(path) =>
          if (Files.exists(path)) {
            \/.fromTryCatch(Files.size(path)) match {
              case -\/(failure) =>
                log.error(failure, "Could not determine file size!")
                sender() ! GlobalMessages.ErrorOccured(
                  StatusMessage(
                    reporter = Option(self.path.toString),
                    message = "Could not determine file size!",
                    statusType = StatusType.MajorError,
                    cause = None
                  )
                )
              case \/-(size) =>
                sender() ! GlobalMessages.ReportAgentRunLogsMetaData(agentId = agentId,
                                                                     uuid = uuid,
                                                                     size = size)
            }
          } else {
            log.error("Requested log file {} does not exist!", path)
            sender() ! GlobalMessages.ErrorOccured(
              StatusMessage(
                reporter = Option(self.path.toString),
                message = s"Requested log file $path does not exist!",
                statusType = StatusType.FatalError,
                cause = None
              )
            )
          }
      }
      context stop self

    case GlobalMessages.RequestAgentRunLogs(id, uuid, offset, maxSize) =>
      log.debug("Got request to report log data of run {}.", uuid)
      if (id != agentId) {
        log.warning("Ignoring log data request for agent {}.", id)
        context stop self
      } else {
        \/.fromTryCatch(FileSystems.getDefault.getPath(logDirName, s"$uuid.log").toAbsolutePath) match {
          case -\/(failure) =>
            log.error(failure, "Could not determine file system path!")
            sender() ! GlobalMessages.ErrorOccured(
              StatusMessage(
                reporter = Option(self.path.toString),
                message = "Could not determine file system path!",
                statusType = StatusType.FatalError,
                cause = None
              )
            )
            context stop self
          case \/-(path) =>
            if (Files.exists(path)) {
              log.debug("Delegating log data streaming to log streamer.")
              val worker = context.actorOf(LogStreamer.props(sender(), uuid))
              context watch worker
              worker ! LogStreamer.StreamLog(path = path, offset = offset, maxSize = maxSize)
            } else {
              log.error("Requested log file {} does not exist!", path)
              sender() ! GlobalMessages.ErrorOccured(
                StatusMessage(
                  reporter = Option(self.path.toString),
                  message = s"Requested log file $path does not exist!",
                  statusType = StatusType.FatalError,
                  cause = None
                )
              )
              context stop self
            }
        }
      }

    case Terminated(ref) =>
      log.debug("Got terminated message, terminating.")
      context stop self
  }

}

object LogReporter {

  def props: Props = Props[LogReporter]

}

/**
  * A worker actor that will stream log data to a defined receiver.
  * The worker will stop after it processed a request or if an error occurred.
  *
  * @param receiver An actor ref of the receiver of the log data.
  * @param uuid The agent run identifier which is usually uuid.
  */
class LogStreamer(receiver: ActorRef, uuid: String) extends Actor with ActorLogging {

  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  override def receive: Actor.Receive = {
    case LogStreamer.StreamLog(path, offset, maxSize) =>
      log.debug("Streaming log data from {} to {}.", path, receiver)
      val is = Files.newInputStream(path)
      val r  = new BufferedReader(new InputStreamReader(is, "UTF-8"))
      // Skip lines until we've reached the offset.
      // This method of skipping will most likely lead to the first line
      // send being one the requester already has. But it won't skip
      // non-send lines.
      offset.foreach { o =>
        var l       = r.readLine()
        var b: Long = if (l != null) l.getBytes("UTF-8").length.toLong else 0L
        while (l != null && b < o) {
          l = r.readLine()
          b = if (l != null) b + l.getBytes("UTF-8").length.toLong else b + b
        }
      }
      // Start streaming log data.
      var line        = r.readLine()
      var bytes: Long = if (line != null) line.getBytes("UTF-8").length.toLong else 0L
      while (line != null && maxSize.forall(s => bytes <= s)) {
        if (line.nonEmpty) {
          // Avoid sending empty lines.
          receiver ! GlobalMessages.ReportAgentRunLogLine(
            uuid = uuid,
            logLine = line,
            offet = offset.map(o => o + bytes).getOrElse(bytes)
          )
        }
        line = r.readLine()
        bytes = if (line != null) bytes + line.getBytes("UTF-8").length.toLong else bytes + bytes
      }
      log.debug("LogStreamer done, stopping.")
      context stop self
  }

}

object LogStreamer {

  /**
    * A factory method to create the properties for the `LogStreamer` actor.
    *
    * @param receiver The `ActorRef` of the receiver of the log data.
    * @param uuid The agent run identifier which is usually uuid.
    * @return The props to create the actor.
    */
  def props(receiver: ActorRef, uuid: String): Props = Props(classOf[LogStreamer], receiver, uuid)

  /**
    * Instruct the actor to stream data from the given file path.
    *
    * @param path The path to the logfile.
    * @param offset An optional offset of bytes that has to be skipped.
    * @param maxSize An optional maximum byte size of the data that should be send.
    */
  final case class StreamLog(path: Path, offset: Option[Long] = None, maxSize: Option[Long] = None)

}
