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

import akka.actor._
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.wegtam.tensei.adt.GlobalMessages.TransformationError
import com.wegtam.tensei.adt.{
  AgentStartTransformationMessage,
  ConnectionInformation,
  GlobalMessages,
  StatusMessage
}
import com.wegtam.tensei.agent.AccessValidator.AccessValidatorMessages
import com.wegtam.tensei.agent.ChecksumValidator.ChecksumValidatorMessages
import com.wegtam.tensei.agent.Parser._
import com.wegtam.tensei.agent.SyntaxValidator.SyntaxValidatorMessages
import com.wegtam.tensei.agent.adt.ParserStatus.ParserStatusType
import com.wegtam.tensei.agent.adt._
import com.wegtam.tensei.agent.exceptions.{ AccessValidationException, ChecksumValidationException }
import com.wegtam.tensei.agent.helpers.ExcelToCSVConverter.ExcelConverterMessages.{
  Convert,
  ConvertResult
}
import com.wegtam.tensei.agent.helpers.{ ExcelToCSVConverter, LoggingHelpers, URIHelpers }
import com.wegtam.tensei.agent.parsers._

import scala.concurrent.duration._
import scalaz._
import Scalaz._

object Parser {
  val name = "Parser"

  /**
    * Helper method to create the parser supervisor actor.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @return The props to generate the actor.
    */
  def props(agentRunIdentifier: Option[String]): Props = Props(classOf[Parser], agentRunIdentifier)

  sealed trait ParserMessages

  object ParserMessages {

    case class StartParsing(stm: AgentStartTransformationMessage,
                            dataTreeDocs: Map[Int, ActorRef],
                            caller: Option[ActorRef] = None)

  }

  case class ParserData(
      msg: Option[AgentStartTransformationMessage] = None,
      caller: Option[ActorRef] = None,
      subparsers: List[ActorRef] = List.empty[ActorRef],
      dataTrees: Map[Int, ActorRef] = Map.empty[Int, ActorRef],
      statusMessages: List[ParserStatusType] = List.empty[ParserStatusType],
      uninitializedSubParsers: List[ActorRef] = List.empty[ActorRef],
      numberOfPrepareWorkers: Int = 0,
      removeTemporarySources: List[URI] = List.empty[URI]
  )

  /**
    * Used to sent the final status report to the original caller of the parser.
    *
    * @param statusMessages A list of status messages from the sub parsers.
    * @param dataTreeDocs   A list of actor refs for the data tree documents holding the parsed data.
    */
  case class ParserCompletedStatus(statusMessages: List[ParserStatusType],
                                   dataTreeDocs: List[ActorRef])

  /**
    * Checks if the source is an Excel file and must be converted to a CSV.
    *
    * @param c  The connection information.
    * @return `true` if the source is an Excel file, `false` otherwise
    */
  def sourceMustBeExcelConverted(c: ConnectionInformation): Boolean =
    c.uri.toString.endsWith(".xls") || c.uri.toString.endsWith(".xlsx")
}

/**
  * Main entry point for parsing the source data.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  */
class Parser(agentRunIdentifier: Option[String])
    extends Actor
    with FSM[ParserState, Parser.ParserData]
    with ActorLogging {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  val syntaxValidationTimeout = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.parser.syntax-validation-timeout", MILLISECONDS),
    MILLISECONDS
  )
  val accessValidationTimeout = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.parser.access-validation-timeout", MILLISECONDS),
    MILLISECONDS
  )
  val checksumValidationTimeout = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.parser.checksum-validation-timeout", MILLISECONDS),
    MILLISECONDS
  )
  val prepareSourcesTimeout = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.parser.prepare-sources-timeout", MILLISECONDS),
    MILLISECONDS
  )
  val subParserInitializationTimeout = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.parser.subparsers-init-timeout", MILLISECONDS),
    MILLISECONDS
  )

  // TODO Maybe we should start these on demand and kill them right after use.
  val syntaxValidator =
    context.actorOf(SyntaxValidator.props(agentRunIdentifier), "SyntaxValidator")
  val accessValidator =
    context.actorOf(AccessValidator.props(agentRunIdentifier), "AccessValidator")
  val checksumValidator =
    context.actorOf(ChecksumValidator.props(agentRunIdentifier), "ChecksumValidator")

  override def postStop(): Unit = {
    log.clearMDC()
    super.postStop()
  }

  startWith(ParserState.Idle, ParserData())

  when(ParserState.Idle) {
    case Event(msg: ParserMessages.StartParsing, data) =>
      val dfasdls = msg.stm.cookbook.target.get :: msg.stm.cookbook.sources
      syntaxValidator ! SyntaxValidatorMessages.ValidateDFASDLs(dfasdls)
      goto(ParserState.ValidatingSyntax) using data.copy(msg = Option(msg.stm),
                                                         dataTrees = msg.dataTreeDocs,
                                                         caller = Option(sender()))
  }

  when(ParserState.ValidatingSyntax, stateTimeout = syntaxValidationTimeout) {
    case Event(SyntaxValidatorMessages.ValidateDFASDLsResults(results), data) =>
      val failures = results.filter(_.isFailure)
      if (failures.nonEmpty) {
        log.error("Syntax validation failed!")
        failures foreach {
          case Success(_) => // This should actually never happen, but the match pattern has to be complete.
          case Failure(f) =>
            if (data.caller.isDefined) {
              data.caller.get ! TransformationError(
                data.msg.get.uniqueIdentifier,
                StatusMessage(message = "Syntax validation failed!",
                              reporter = Option(self.toString()),
                              cause = None)
              )
            }
            log.error(f.toList.mkString)
        }
        goto(ParserState.Idle) using ParserData()
      } else {
        accessValidator ! AccessValidatorMessages.ValidateTransformationConnections(
          sources = data.msg.get.sources,
          targets = List(data.msg.get.target)
        )
        goto(ParserState.ValidatingAccess) using data
      }
    case Event(StateTimeout, data) =>
      log.error("Timeout while waiting for syntax validation results!")
      goto(ParserState.Idle) using ParserData()
  }

  when(ParserState.ValidatingAccess, stateTimeout = accessValidationTimeout) {
    case Event(AccessValidatorMessages.ValidateTransformationConnectionsResults(results), data) =>
      if (results.exists(_.isFailure)) {
        log.error("Access validation failed!")
        if (data.caller.isDefined) {
          val messageText = results.filter(_.isFailure).map {
            case Success(_) => ""
            case Failure(f) => f.toList.mkString(", ")
          }
          data.caller.get ! TransformationError(data.msg.get.uniqueIdentifier,
                                                StatusMessage(message = messageText.mkString(", "),
                                                              reporter = Option(self.toString()),
                                                              cause = None))
        }
        goto(ParserState.Idle) using ParserData()
      } else {
        if (data.msg.get.hasChecksums) {
          checksumValidator ! ChecksumValidatorMessages.ValidateChecksums(data.msg.get.sources)
          goto(ParserState.ValidatingChecksums) using data
        } else {
          // If we have to prepare the source data (e.g. an Excel file), we must change
          // into another step that starts the preparation workers
          if (sourcesMustBePrepared(data.msg)) {
            val numberOfStartedWorkers = prepareSources(data.msg)
            goto(ParserState.PreparingSourceData) using data.copy(
              numberOfPrepareWorkers = numberOfStartedWorkers
            )
          } else {
            val subParsers = createParsers(data.msg.get, data.dataTrees)
            goto(ParserState.InitializingSubParsers) using data.copy(subparsers = subParsers,
                                                                     uninitializedSubParsers =
                                                                       subParsers)
          }
        }
      }
    case Event(StateTimeout, data) =>
      log.error("Timeout while waiting for access validation results!")
      goto(ParserState.Idle) using ParserData()
  }

  when(ParserState.ValidatingChecksums, stateTimeout = checksumValidationTimeout) {
    case Event(ChecksumValidatorMessages.ValidateChecksumsResults(results), data) =>
      val failures = results filter (_.isFailure)
      if (failures.nonEmpty) {
        log.error("Some checksum validations failed!")
        failures foreach (f => log.error(f.toList.mkString))
        if (data.caller.isDefined) {
          data.caller.get ! TransformationError(data.msg.get.uniqueIdentifier,
                                                StatusMessage(message =
                                                                "Checksum validation failed!",
                                                              reporter = Option(self.toString()),
                                                              cause = None))
        }
        goto(ParserState.Idle) using ParserData()
      } else {
        log.debug("Checksum validations successful.")
        // If we have to prepare the source data (e.g. an Excel file), we must change
        // into another step that starts the preparation workers
        if (sourcesMustBePrepared(data.msg)) {
          val numberOfStartedWorkers = prepareSources(data.msg)
          goto(ParserState.PreparingSourceData) using data.copy(
            numberOfPrepareWorkers = numberOfStartedWorkers
          )
        } else {
          val subParsers = createParsers(data.msg.get, data.dataTrees)
          goto(ParserState.InitializingSubParsers) using data.copy(subparsers = subParsers,
                                                                   uninitializedSubParsers =
                                                                     subParsers)
        }
      }
    case Event(StateTimeout, data) =>
      log.error(
        "Timeout while waiting for checksum validation! Consider increasing the timeout value."
      )
      goto(ParserState.Idle) using ParserData()
  }

  // FIXME: Test mit einem kompletten Durchlauf
  when(ParserState.PreparingSourceData, stateTimeout = prepareSourcesTimeout) {
    case Event(ConvertResult(source), data) =>
      log.info(
        s"Receiving the connection information of the prepared source data for ${source.dfasdlRef}"
      )
      // Memorise the uri that must be removed after the parsing process
      // Decrease the worker from the state counter
      val formerWorkers = data.numberOfPrepareWorkers
      val removeSources = source.uri :: data.removeTemporarySources
      val newMsg =
        for {
          m <- data.msg
          s <- m.sources.find(_.dfasdlRef == source.dfasdlRef)
          newC = s.copy(uri = source.uri)
          ls   = newC :: m.sources.filterNot(e => e == s)
        } yield m.copy(sources = ls)

      val newData = data.copy(msg = newMsg,
                              numberOfPrepareWorkers = formerWorkers - 1,
                              removeTemporarySources = removeSources)

      // Move to the next step
      if (newData.numberOfPrepareWorkers == 0) {
        val subParsers = createParsers(newData.msg.get, newData.dataTrees)
        goto(ParserState.InitializingSubParsers) using newData.copy(subparsers = subParsers,
                                                                    uninitializedSubParsers =
                                                                      subParsers)
      } else
        stay using newData
    case Event(StateTimeout, data) =>
      log.warning("Timeout while waiting for preparation of sources.")
      if (data.numberOfPrepareWorkers == 0)
        goto(ParserState.InitializingSubParsers) using data
      else {
        removeTemporarySources(data.removeTemporarySources)
        goto(ParserState.Idle) using ParserData()
      }

  }

  when(ParserState.InitializingSubParsers, stateTimeout = subParserInitializationTimeout) {
    case Event(BaseParserMessages.SubParserInitialized, data) =>
      val ref = sender()
      val newStateData =
        if (data.uninitializedSubParsers.contains(ref)) {
          log.debug("Sub parser at {} initialized.", ref.path)
          val remainingUninitializedSubParsers =
            data.uninitializedSubParsers.filterNot(r => r == ref)
          data.copy(uninitializedSubParsers = remainingUninitializedSubParsers)
        } else
          data
      if (newStateData.uninitializedSubParsers.isEmpty) {
        log.debug("All sub parsers initialized. Going to parsing state.")
        goto(ParserState.Parsing) using newStateData
      } else {
        log.debug("{} sub parsers not initialized. Waiting for initialization.",
                  newStateData.uninitializedSubParsers.size)
        stay using newStateData
      }
    case Event(StateTimeout, data) =>
      log.warning("Timeout while waiting for sub parser pings. Going to parsing state anyway.")
      goto(ParserState.Parsing) using data
  }

  when(ParserState.Parsing) {
    case Event(ParserStatusMessage(status, subParser), data) =>
      log.debug("Received status message from sub parser.")
      // Try to get the origin of the message.
      val origin =
        if (subParser.isEmpty) {
          log.error("Received status message without sender information!")
          sender()
        } else
          subParser.get

      // Process status message.
      status match {
        case ParserStatus.ABORTED =>
          log.error("Parsing was aborted! {}", origin.path)
        case ParserStatus.COMPLETED =>
          log.debug("Parsing completed. {}", origin.path)
        case ParserStatus.COMPLETED_WITH_ERROR =>
          if (data.caller.isDefined) {
            data.caller.get ! TransformationError(data.msg.get.uniqueIdentifier,
                                                  StatusMessage(message =
                                                                  "Parsing completed with errors!",
                                                                reporter = Option(self.toString()),
                                                                cause = None))
          }
          log.warning("Parsing completed with errors! {}", origin.path)
        case ParserStatus.END_OF_DATA =>
          log.warning("No more data to parse! {}", origin.path)
      }

      // Get remaining sub parsers and switch state.
      val remainingParsers = data.subparsers.filterNot(_ == origin)
      if (remainingParsers.nonEmpty)
        goto(ParserState.Parsing) using data.copy(
          subparsers = remainingParsers,
          statusMessages = data.statusMessages ::: status :: List()
        ) // Keep the current state with updated data.
      else {
        // Send completed message to original caller.
        if (data.caller.isDefined)
          data.caller.get ! ParserCompletedStatus(data.statusMessages, data.dataTrees.values.toList)
        else
          log.error(
            "No caller defined in parser state data! We cannot inform anyone upon completing the parsing!"
          )

        stopChildActors(data.subparsers)
        removeTemporarySources(data.removeTemporarySources)
        goto(ParserState.Idle) using ParserData()
      }

  }

  onTransition {
    case _ -> ParserState.InitializingSubParsers =>
      log.debug("Initializing sub parsers.")
      initializeSubParsers(nextStateData.uninitializedSubParsers)
    case x -> ParserState.Parsing =>
      if (x != ParserState.Parsing) {
        log.debug("Starting parsers.")
        startParsers(nextStateData.subparsers)
      }
  }

  whenUnhandled {
    case Event(e: AccessValidationException, data) =>
      log.error(e, "An error occurred!")
      if (data.caller.isDefined) {
        data.caller.get ! TransformationError(data.msg.get.uniqueIdentifier,
                                              StatusMessage(message = "Access validation failed!",
                                                            reporter = Option(self.toString()),
                                                            cause = None))
      }
      goto(ParserState.Idle) using ParserData()
    case Event(e: ChecksumValidationException, data) =>
      log.error(e, "An error occurred!")
      if (data.caller.isDefined) {
        data.caller.get ! TransformationError(
          data.msg.get.uniqueIdentifier,
          StatusMessage(message = "Checksum validation failed!",
                        reporter = Option(self.toString()),
                        cause = None)
        )
      }
      goto(ParserState.Idle) using ParserData()
    case Event(e: GlobalMessages.AbortTransformation, data) =>
      log.debug("The parser will be aborted!")
      stopChildActors(data.dataTrees.values.toList)
      stopChildActors(data.subparsers)
      // Remove temporary files that have been created during preparation for the parsing process
      removeTemporarySources(data.removeTemporarySources)
      e.ref ! GlobalMessages.AbortTransformationResponse(self, Option("Transformation aborted."))
      goto(ParserState.Idle) using ParserData()
  }

  initialize()

  private def stopChildActors(parser: List[ActorRef] = List()) =
    if (parser.nonEmpty) {
      parser.foreach(p => {
        val actorSelection = context.actorSelection(p.path)
        actorSelection ! BaseParserMessages.Stop
      })
    }

  /**
    * Creates a number of sub parsers to handle the given data sources.
    * It returns a list containing the `ActorRef` information of the sub parsers.
    *
    * @param msg       The AgentStartTransformationMessage that is used to feed data to the sub parsers.
    * @param dataTrees A map of the data tree document actors mapped to their dfasdl's hashcode.
    * @return A list of actor refs of the started sub parsers.
    * @throws RuntimeException If no dfasdl reference is defined or the DFASDL is not found in the cookbook.
    */
  private def createParsers(msg: AgentStartTransformationMessage,
                            dataTrees: Map[Int, ActorRef]): List[ActorRef] = {
    log.debug("Preparing parsing.")
    val cookbook = msg.cookbook

    msg.sources map (source => {
      if (source.dfasdlRef.isEmpty)
        throw new RuntimeException(s"No DFASDLReference defined for '${source.uri}'!")

      val dfasdl = cookbook.findDFASDL(source.dfasdlRef.get)
      if (dfasdl.isEmpty)
        throw new RuntimeException(
          s"Could not resolve DFASDL reference: ${source.dfasdlRef.get} in cookbook '${cookbook.id}'!"
        )

      val dataTree = dataTrees(dfasdl.get.hashCode())

      URIHelpers.connectionType(source.uri) match {
        case ConnectionTypeFile =>
          val subParserName = s"FileParser-${dfasdl.get.id}"
          val parserWorker =
            if (source.uri.toString.endsWith(".xml"))
              context.actorOf(XmlFileParser.props(source, cookbook, dataTree, agentRunIdentifier),
                              subParserName)
            else if (source.uri.toString.endsWith(".json"))
              context.actorOf(JsonFileParser.props(source, cookbook, dataTree, agentRunIdentifier),
                              subParserName)
            else
              context.actorOf(FileParser.props(source, cookbook, dataTree, agentRunIdentifier),
                              subParserName)
          log.debug("Sub parser created {} at {}", subParserName, parserWorker.path)
          parserWorker
        case ConnectionTypeFileFromNetwork =>
          val subParserName = s"NetworkFileParser-${dfasdl.get.id}"
          if (source.uri.toString.endsWith(".xml")) {
            log.error("Parser for XML via network files not yet implemented!")
            ???
          } else {
            val parserWorker = context.actorOf(
              NetworkFileParser.props(source, cookbook, dataTree, agentRunIdentifier),
              subParserName
            )
            log.debug("Sub parser created {} at {}", subParserName, parserWorker.path)
            parserWorker
          }
        case ConnectionTypeDatabase =>
          val subParserName = s"DatabaseParser-${dfasdl.get.id}"
          val parserWorker =
            context.actorOf(DatabaseParser.props(source, cookbook, dataTree, agentRunIdentifier),
                            subParserName)
          log.debug("Sub parser created {} at {}", subParserName, parserWorker.path)
          parserWorker
        case ConnectionTypeAPI =>
          log.error("Parser for api connections is not yet implemented!")
          ???
        case ConnectionTypeStream =>
          log.error("Parser for streams is not yet implemented!")
          ???
      }
    })
  }

  /**
    * Remove existing temporary files that were created in the preparation phase
    *
    * @param files  List of temporary files
    * @return Number of removed files
    */
  private def removeTemporarySources(files: List[URI]): Int =
    files.map { uri =>
      val file = new File(uri)
      if (file.exists() && file.isFile) {
        if (file.delete()) {
          log.info(s"Removed temporary file ${uri.getRawSchemeSpecificPart}")
          1
        } else {
          log.warning(s"Could not remove temporary file ${uri.getSchemeSpecificPart}")
          0
        }
      } else
        0
    }.sum

  /**
    * Check the connection information whether they contain sources that must be prepared by
    * additional workers.
    *
    *
    * e.g. tansform an Excel file into a CSV file
    *
    * @param msg The AgentStartTransformationMessage that started this processing.
    * @return `true` if sources must be prepared, `false` otherwise
    */
  private def sourcesMustBePrepared(msg: Option[AgentStartTransformationMessage]): Boolean =
    msg.fold(false) { astm =>
      astm.sources.exists(s => sourceMustBePrepared(s))
    }

  /**
    * Check if the connection information must be prepared by an additional worker.
    *
    * @param c  The connection information.
    * @return `true` if the connection information needs an additional step, `false` otherwise
    */
  private def sourceMustBePrepared(c: ConnectionInformation): Boolean =
    sourceMustBeExcelConverted(c)

  /**
    * Create workers for all sources that must be prepared before parsed.
    *
    * @param msg  The AgentStartTransformationMessage
    * @return Number of started preparation workers.
    */
  private def prepareSources(msg: Option[AgentStartTransformationMessage]): Int =
    msg.fold(0) { astm =>
      val res = astm.sources.map { source =>
        if (sourceMustBePrepared(source)) {
          createPreparationWorker(source).fold(0) { w =>
            w ! Convert
            1
          }
        } else
          0
      }
      res.sum
    }

  /**
    * Create a worker for the given source depending on the case.
    *
    * @param c  The connection information
    * @return The actorRef of the created worker
    */
  private def createPreparationWorker(c: ConnectionInformation): Option[ActorRef] =
    URIHelpers.connectionType(c.uri) match {
      case ConnectionTypeFile =>
        if (sourceMustBeExcelConverted(c))
          Option(context.actorOf(ExcelToCSVConverter.props(c, agentRunIdentifier)))
        else
          None
      case _ => None
    }

  /**
    * Sends a `SubParserInitialize` message to all actor refs in the given list.
    *
    * @param parsers A list of actor refs for sub parsers.
    */
  private def initializeSubParsers(parsers: List[ActorRef]): Unit =
    parsers foreach (ref => ref ! BaseParserMessages.SubParserInitialize)

  /**
    * Simply send the start message to the given list of parsers.
    *
    * @param parsers A list of actor refs for parsers.
    */
  private def startParsers(parsers: List[ActorRef]): Unit =
    parsers.foreach(_ ! BaseParserMessages.Start)
}
