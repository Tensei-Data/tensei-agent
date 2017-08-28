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

import akka.actor.FSM.{ CurrentState, SubscribeTransitionCallBack, Transition }
import akka.actor._
import akka.cluster.{ Cluster, Member }
import akka.cluster.ClusterEvent.{ CurrentClusterState, ReachableMember, UnreachableMember }
import akka.cluster.client.ClusterClient
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import akka.pattern.ask
import akka.remote.AssociationErrorEvent
import akka.util.Timeout
import com.wegtam.tensei.adt.GlobalMessages.{ TransformationError, TransformationStarted }
import com.wegtam.tensei.adt.StatsMessages.{ CalculateStatistics, CalculateStatisticsResult }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.ClusterMetricsListener.ClusterMetricsListenerMessages
import com.wegtam.tensei.agent.DataTreeDocumentsManager.DataTreeDocumentsManagerMessages
import com.wegtam.tensei.agent.Parser.{ ParserCompletedStatus, ParserMessages }
import com.wegtam.tensei.agent.Processor.ProcessorMessages.StartProcessingMessage
import com.wegtam.tensei.agent.Processor.ProcessorMessages
import com.wegtam.tensei.agent.Stats.StatsMessages.FinishAnalysis
import com.wegtam.tensei.agent.TenseiAgent.{ TenseiAgentData, TenseiAgentMessages }
import com.wegtam.tensei.agent.helpers.LoggingHelpers

import scala.concurrent.duration._
import scala.collection.mutable

object TenseiAgent {

  /**
    * Create props for an actor of this type.
    *
    * @return A prop for creating this actor which can then be further configured (e.g. calling `.withDispatcher()` on it.
    */
  def props(id: String, clusterClient: ActorSelection): Props =
    Props(classOf[TenseiAgent], id, clusterClient)

  sealed trait TenseiAgentMessages

  object TenseiAgentMessages {

    case object UpdateAgentInformationToServer extends TenseiAgentMessages

  }

  /**
    * Holds the state data for the agent.
    *
    * @param chef                  An actor selection for the cluster client pointing to the server.
    * @param currentTransformation The current transformation e.g. an option to the `AgentStartTransformationMessage` that is in progress.
    * @param currentEmployer       An option to the actor ref of the sender of the current transformation e.g. `AgentStartTransformationMessage`.
    * @param dataTreeDocs          A map containing all the source dfasdl ids pointing to their data tree documents.
    * @param dataTreeManager       The actor ref for the data tree documents manager.
    * @param parser                The actor ref for the parser.
    * @param parserState           The current state of the parser.
    * @param processor             The actor ref for the processor.
    * @param processorState        The current state of the processor.
    * @param stats                 The actor ref for the Stats analyzer.
    */
  case class TenseiAgentData(
      chef: ActorSelection,
      currentTransformation: Option[AgentStartTransformationMessage] = None,
      currentEmployer: Option[ActorRef] = None,
      dataTreeDocs: Map[Int, ActorRef] = Map.empty[Int, ActorRef],
      dataTreeManager: Option[ActorRef],
      parser: Option[ActorRef],
      parserState: ParserState = ParserState.Idle,
      processor: Option[ActorRef],
      processorState: ProcessorState = ProcessorState.Idle,
      stats: Option[ActorRef] = None
  ) {

    /**
      * Return the agent state data and remove possibly data that would clash when starting a new
      * transformation.
      *
      * @return The cleaned state data.
      */
    def clean: TenseiAgentData =
      this.copy(
        currentTransformation = None,
        currentEmployer = None,
        dataTreeDocs = Map.empty[Int, ActorRef],
        parserState = ParserState.Idle,
        processorState = ProcessorState.Idle,
        stats = None
      )
  }
}

/**
  * The top level node for an agent system.
  *
  * @param id            The ID of the agent.
  * @param clusterClient An actor ref to a cluster client that is used to talk to the chef de cuisine.
  */
class TenseiAgent(id: String, clusterClient: ActorSelection)
    extends Actor
    with FSM[TenseiAgentState, TenseiAgentData]
    with ActorLogging {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.

  val UPDATE_STATUS_TIMER_NAME = "updateAgentToServer"

  val abortTimeout = FiniteDuration(
    context.system.settings.config.getDuration("tensei.agents.abort-timeout", MILLISECONDS),
    MILLISECONDS
  )
  val cleanupTimeout = FiniteDuration(
    context.system.settings.config.getDuration("tensei.agents.cleanup-timeout", MILLISECONDS),
    MILLISECONDS
  )

  val parserState = new mutable.HashMap[String, ParserState]

  val metrics = context.actorOf(ClusterMetricsListener.props, ClusterMetricsListener.name)

  override val supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  override def preStart(): Unit = {
    log.info("Starting tensei agent {}...", id)
    val interval = FiniteDuration(
      context.system.settings.config
        .getDuration("tensei.agents.report-to-server-interval", SECONDS),
      SECONDS
    )
    setTimer(UPDATE_STATUS_TIMER_NAME,
             TenseiAgentMessages.UpdateAgentInformationToServer,
             interval,
             repeat = true)
    val cluster = Cluster(context.system)
    cluster.subscribe(self, classOf[UnreachableMember], classOf[ReachableMember])
    val _ = context.system.eventStream.subscribe(self, classOf[AssociationErrorEvent])
  }

  override def postStop(): Unit = {
    log.clearMDC()
    cancelTimer(UPDATE_STATUS_TIMER_NAME)
    val cluster = Cluster(context.system)
    cluster.unsubscribe(self)
  }

  startWith(TenseiAgentState.Idle, initializeState())

  onTransition {
    case from -> to =>
      updateAgentInformationToChef(nextStateData, stateName, Option(nextStateData.chef)) // Update chef on every transition.
      if (to == TenseiAgentState.Aborting) {
        terminateWorkers(nextStateData) // We need to terminate our worker actors.
        if (stateData.currentEmployer.isDefined) {
          log.debug("Notifying current employer about abortion.")
          val currentId =
            if (stateData.currentTransformation.isDefined)
              stateData.currentTransformation.get.uniqueIdentifier
            else
              None
          stateData.currentEmployer.get ! GlobalMessages.TransformationAborted(uuid = currentId)
        }
      }
  }

  // Many events can be handled in the `whenUnhandled` block because they are treated the same way in each state.
  whenUnhandled {
    case Event(msg: TransformationError, data) =>
      log.debug("Send TransformationError message to caller")
      terminateWorkers(data)
      if (data.currentEmployer.isDefined)
        data.currentEmployer.get ! msg
      goto(TenseiAgentState.CleaningUp) using data
    case Event(TenseiAgentMessages.UpdateAgentInformationToServer, data) =>
      log.debug("Send status information to ChefDeCuisine")
      updateAgentInformationToChef(data, stateName, Option(data.chef))
      stay() using data
    case Event(GlobalMessages.ReportToCaller, data) =>
      log.debug("Got ReportToCaller signal from {}.", sender().path)
      sender() ! GlobalMessages.ReportingTo(self, Option(id))
      stay() using data
    case Event(GlobalMessages.ReportToRef(ref), data) =>
      log.debug("Got ReportToRef({}) signal from {}.", ref.path, sender().path)
      ref ! GlobalMessages.ReportingTo(self, Option(id))
      stay() using data
    case Event(GlobalMessages.Restart, data) =>
      log.info("Received RESTART signal.")
      context.system.registerOnTermination { TenseiAgentApp.main(Array.empty) }
      context.system.terminate()
      stop(reason = FSM.Shutdown)
    case Event(GlobalMessages.Shutdown, data) =>
      log.info("Received SHUTDOWN signal.")
      context.system.terminate()
      stop(reason = FSM.Shutdown)

    case Event(msg: Identify, data) =>
      log.debug("Got Identify message from {}.", sender().path)
      sender() ! ActorIdentity(msg.messageId, Option(self))
      stay() using data

    case Event(clusterState: CurrentClusterState, data) =>
      log.debug("We ignore the CurrentClusterState message on purpose.")
      stay() using data
    case Event(UnreachableMember(member), data) =>
      handleUnreachableMember(member)
      stay() using data
    case Event(ReachableMember(member), data) =>
      handleReachableMember(member)
      stay() using data
    case Event(AssociationErrorEvent(cause, localAddress, remoteAddress, inbound, logLevel),
               data) =>
      log.error("A remote association error occurred.")
      if (cause.getCause != null && cause.getCause.getMessage.contains(
            "The remote system has quarantined this system."
          )) {
        log.error(
          "This agent system got quarantined from the server cluster! Stopping agent system!"
        )
        context.system.terminate()
        stop()
      } else
        stay() using data

    case Event(CurrentState(ref: ActorRef, state: ParserState), data) =>
      log.debug("Got current state event from parser with state: {}", state)
      val newData = data.copy(parserState = state)
      updateAgentInformationToChef(newData, stateName, Option(newData.chef))
      stay() using newData
    case Event(CurrentState(ref: ActorRef, state: ProcessorState), data) =>
      log.debug("Got current state event from processor with state {}", state)
      val newData = data.copy(processorState = state)
      updateAgentInformationToChef(newData, stateName, Option(newData.chef))
      stay() using newData
    case Event(Transition(ref: ActorRef, from: ParserState, to: ParserState), data) =>
      log.debug("Got transition message from parser from '{}' to '{}'.", from, to)
      val newData = data.copy(parserState = to)
      updateAgentInformationToChef(newData, stateName, Option(newData.chef))
      stay() using newData
    case Event(Transition(ref: ActorRef, from: ProcessorState, to: ProcessorState), data) =>
      log.debug("Got transition message from processor from '{}' to '{}'.", from, to)
      val newData = data.copy(processorState = to)
      updateAgentInformationToChef(newData, stateName, Option(newData.chef))
      stay() using newData

    case Event(msg: GlobalMessages.RequestAgentRunLogsMetaData, data) =>
      log.debug("Got request for agent run logs meta data.")
      val worker = context.actorOf(LogReporter.props)
      worker.forward(msg)
      stay() using data

    case Event(msg: GlobalMessages.RequestAgentRunLogs, data) =>
      log.debug("Got request for agent run logs meta data.")
      val worker = context.actorOf(LogReporter.props)
      worker.forward(msg)
      stay() using data

    case Event(Terminated(ref), data) =>
      log.debug("Got terminated message.")
      if (ref.path.name == "clusterClient") {
        log.info("Cluster client terminated. Restarting it.")
        TenseiAgentApp.createClusterClient(context.system) match {
          case scala.util.Failure(t) =>
            log.error(t, "Could not re-create cluster singleton!")
            val _ = context.system.terminate()
            stop(FSM.Failure(t))
          case scala.util.Success(client) =>
            log.info("Created new cluster singleton.")
            stay() using data.copy(chef = client)
        }
      } else {
        val importantActors = List(data.dataTreeManager, data.parser, data.processor)
        if (importantActors.contains(Option(ref))) {
          log.error("One of our actors terminated unexpectedly at {}, aborting!", ref.path)
          val newData =
            if (Option(ref) == data.dataTreeManager)
              data.copy(dataTreeManager = None)
            else if (Option(ref) == data.parser)
              data.copy(parser = None)
            else if (Option(ref) == data.processor)
              data.copy(processor = None)
            else {
              log.error("Received terminated message that we don't handle!")
              data
            }
          goto(TenseiAgentState.Aborting) using newData
        } else {
          log.warning("Got unhandled actor termination message for '{}'!", ref.path)
          stay() using data
        }
      }

    case Event(message: Any, data) =>
      log.warning("Got unhandled message from {} in state {}: {}.",
                  sender().path,
                  stateName,
                  message)
      stay() using data
  }

  when(TenseiAgentState.Idle) {
    case Event(GlobalMessages.AbortTransformation(ref), data) =>
      log.info("Got abort transformation message but no transformation is currently running!")
      stay() using data
    case Event(msg: AgentStartTransformationMessage, data) =>
      log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(msg.uniqueIdentifier))
      log.info("Received AgentStartTransformationMessage.")
      val nextData = initializeActorsAndState(data.chef, msg.uniqueIdentifier)
      // Start creating data tree document containers.
      log.debug("Starting data tree document containers.")
      msg.sources foreach (source => {
        val dfasdl = msg.cookbook.findDFASDL(source.dfasdlRef.get)
        if (dfasdl.isDefined)
          nextData.dataTreeManager.get ! DataTreeDocumentsManagerMessages
            .CreateDataTreeDocumentWithWhiteList(
              dfasdl.get,
              msg.cookbook.usedSourceIds.map(_.elementId)
            ) // FIXME We should refactor to extract only the ids from the appropriate dfasdl here.
        else
          log.error("No DFASDL defined for source {} and ref {} in cookbook {}!",
                    source.uri,
                    source.dfasdlRef,
                    msg.cookbook.id) // FIXME We should abort here!
      })
      // Send start message back
      val receivedFrom = Option(sender())
      if (receivedFrom.isDefined)
        receivedFrom.get ! TransformationStarted(msg.uniqueIdentifier)
      log.debug("Switching to InitializingResources state.")
      goto(TenseiAgentState.InitializingResources) using nextData.copy(currentTransformation =
                                                                         Option(msg),
                                                                       currentEmployer =
                                                                         receivedFrom)
    case Event(msg: GlobalMessages.ExtractSchema, data) =>
      log.info("Start extracting schema from connection")
      val extractor = context.actorOf(SchemaExtractor.props())
      extractor.forward(msg)
      stay() using data
    // Start the calculation of statistical data
    case Event(msg: CalculateStatistics, data) =>
      log.info("Got request to start the calculation of statistics!")
      val nextData = initializeActorsAndState(data.chef)
      val statsActor =
        context.actorOf(Stats.props(msg.source, msg.cookbook, msg.sourceIds, msg.percent),
                        Stats.name)
      // FIXME : das funktioniert nur für eine Source
      val dfasdl = msg.cookbook.findDFASDL(msg.source.dfasdlRef.get)
      val newStateData =
        nextData.copy(dataTreeDocs = nextData.dataTreeDocs + (dfasdl.get.hashCode -> statsActor),
                      stats = Option(statsActor))
      // start the parser
      val agentStartMessage =
        new AgentStartTransformationMessage(List(msg.source), msg.source, msg.cookbook)
      newStateData.parser.get ! ParserMessages.StartParsing(stm = agentStartMessage,
                                                            dataTreeDocs =
                                                              newStateData.dataTreeDocs)
      goto(TenseiAgentState.Working) using newStateData.copy(currentEmployer = Option(sender()))
  }

  when(TenseiAgentState.InitializingResources) {
    case Event(DataTreeDocumentsManagerMessages.DataTreeDocumentCreated(dataTreeRef, dfasdl),
               data) =>
      log.debug("Got DataTreeDocumentCreated message.")
      val newStateData =
        data.copy(dataTreeDocs = data.dataTreeDocs + (dfasdl.hashCode -> dataTreeRef))
      if (newStateData.dataTreeDocs.size == data.currentTransformation.get.sources.size) {
        newStateData.parser.get ! ParserMessages.StartParsing(
          stm = newStateData.currentTransformation.get,
          dataTreeDocs = newStateData.dataTreeDocs
        )
        goto(TenseiAgentState.Working) using newStateData
      } else {
        log.debug("Not enough data tree documents yet, waiting for more.")
        stay() using newStateData
      }
    case Event(GlobalMessages.AbortTransformation(ref), data) =>
      log.info("Got abort transformation message from {}, terminating actors!", sender().path)
      terminateWorkers(data)
      sender() ! GlobalMessages.AbortTransformationResponse(self,
                                                            Option("Transformation aborted!"))
      goto(TenseiAgentState.Aborting) using data
    case Event(msg: TransformationError, data) =>
      log.debug("Send TransformationError message to caller")
      terminateWorkers(data)
      if (data.currentEmployer.isDefined)
        data.currentEmployer.get ! msg
      goto(TenseiAgentState.CleaningUp) using data
  }

  when(TenseiAgentState.Working) {
    case Event(msg: AgentStartTransformationMessage, data) =>
      log.warning("Received AgentStartTransformationMessage but we are already working.")
      stay() using data
    case Event(GlobalMessages.AbortTransformation(ref), data) =>
      log.info("Got abort transformation message from {}, terminating actors!", sender().path)
      terminateWorkers(data)
      sender() ! GlobalMessages.AbortTransformationResponse(self,
                                                            Option("Transformation aborted!"))
      goto(TenseiAgentState.Aborting) using data
    case Event(ParserCompletedStatus(messages, dataTreeDocs), data) =>
      log.debug("Parser completed, starting processor.")
      if (data.stats.isEmpty) {
        data.processor.get ! StartProcessingMessage(stm = data.currentTransformation.get,
                                                    dataTreeDocs = dataTreeDocs,
                                                    caller = Option(self))
      } else {
        // The statistics analysis is done.
        log.info("Send the finish signal to the statistics analyzer.")
        data.stats.get ! FinishAnalysis
      }
      stay() using data
    case Event(ProcessorMessages.Completed, data) =>
      log.debug("Processor completed, cleaning up.")
      if (data.currentEmployer.isDefined)
        data.currentEmployer.get ! GlobalMessages.TransformationCompleted(
          uuid = data.currentTransformation.get.uniqueIdentifier
        ) // Notify the original sender that we have completed the transformation.
      terminateWorkers(data)
      goto(TenseiAgentState.CleaningUp) using data
    case Event(msg: TransformationError, data) =>
      log.debug("Send TransformationError message to caller")
      terminateWorkers(data)
      if (data.currentEmployer.isDefined)
        data.currentEmployer.get ! msg
      goto(TenseiAgentState.CleaningUp) using data
    case Event(msg: CalculateStatisticsResult, data) =>
      log.info("Statistics analyzer send finish message!")
      data.currentEmployer.get ! msg
      terminateWorkers(data)
      goto(TenseiAgentState.CleaningUp) using data
  }

  /**
    * This is basically the duplication of aborting.
    *
    * @todo We should really clean that up.
    */
  when(TenseiAgentState.CleaningUp, stateTimeout = cleanupTimeout) {
    case Event(Terminated(ref), data) =>
      log.info("Got terminated message for '{}'.", ref.path)
      val newData =
        if (Option(ref) == data.parser) {
          log.debug("Parser terminated.")
          data.copy(parser = None)
        } else if (Option(ref) == data.processor) {
          log.debug("Processor terminated.")
          data.copy(processor = None)
        } else if (Option(ref) == data.dataTreeManager) {
          log.debug("DataTreeManager terminated.")
          data.copy(dataTreeManager = None)
        } else {
          log.error("Received terminated message that we don't handle!")
          data
        }
      if (newData.parser.isEmpty && newData.processor.isEmpty && newData.dataTreeManager.isEmpty) {
        log.info("All child actors terminated. Re-initializing actors and state.")
        goto(TenseiAgentState.Idle) using initializeState()
      } else {
        log.info("Not all child actors terminated. Waiting...")
        stay() using newData
      }
    case Event(StateTimeout, data) =>
      log.warning("Got timeout while waiting for child actors to stop. Restarting system!")
      context.system.registerOnTermination { TenseiAgentApp.main(Array.empty) }
      context.system.terminate()
      stop(reason = FSM.Shutdown)
  }

  /**
    * We wait for the `Terminated` message for each of our actors.
    * When we have "nulled" them all out, we go to the `Idle` state with fresh state.
    * If the timeout event is triggered then we reboot the whole actor system.
    */
  when(TenseiAgentState.Aborting, stateTimeout = abortTimeout) {
    case Event(Terminated(ref), data) =>
      log.info("Got terminated message for '{}'.", ref.path)
      val newData =
        if (Option(ref) == data.parser) {
          log.debug("Parser terminated.")
          data.copy(parser = None)
        } else if (Option(ref) == data.processor) {
          log.debug("Processor terminated.")
          data.copy(processor = None)
        } else if (Option(ref) == data.dataTreeManager) {
          log.debug("DataTreeManager terminated.")
          data.copy(dataTreeManager = None)
        } else {
          log.error("Received terminated message that we don't handle!")
          data
        }
      if (newData.parser.isEmpty && newData.processor.isEmpty && newData.dataTreeManager.isEmpty) {
        log.info("All child actors terminated. Re-initializing actors and state.")
        goto(TenseiAgentState.Idle) using initializeState()
      } else {
        log.info("Not all child actors terminated. Waiting...")
        stay() using newData
      }
    case Event(StateTimeout, data) =>
      log.warning("Got timeout while waiting for child actors to stop. Restarting system!")
      context.system.registerOnTermination { TenseiAgentApp.main(Array.empty) }
      context.system.terminate()
      stop(reason = FSM.Shutdown)
  }

  initialize()

  /**
    * Terminate the workers which's actor refs we stored in the given state.
    * This function should be called when aborting or cleaning up a transformation.
    *
    * @param data The current state's data.
    */
  private def terminateWorkers(data: TenseiAgentData): Unit = {
    if (data.parser.isDefined) context stop data.parser.get
    if (data.processor.isDefined) context stop data.processor.get
    if (data.dataTreeManager.isDefined) context stop data.dataTreeManager.get
    if (data.stats.isDefined) context stop data.stats.get
  }

  /**
    * Send a report message to the watchdog on the server via the
    * provided cluster client actor.
    *
    * @param client An actor selection of a cluster client.
    */
  private def pingWatchdog(client: ActorSelection): Unit =
    client ! ClusterClient.Send(
      path = s"/user/${ClusterConstants.topLevelActorNameOnServer}/WatchDog",
      msg = GlobalMessages.ReportingTo(self, Option(id)),
      localAffinity = false
    )

  /**
    * Returns an empty state which contains an actor selection pointing to the chef de cuisine.
    *
    * @return An empty state with an actor ref of the cluster client for talking to the chef de cuisine.
    */
  private def initializeState(): TenseiAgentData = {
    val chefService = clusterClient
    // Resolve the selection and watch the ref.
    import context.dispatcher
    chefService.resolveOne(FiniteDuration(5, SECONDS)).map(ref => context watch ref)
    // Report to watchdog
    pingWatchdog(chefService)
    // Return new state.
    TenseiAgentData(chef = chefService, dataTreeManager = None, parser = None, processor = None)
  }

  /**
    * Initialize the actors we need as workhorses and return the `TenseiAgentData` containing the actor refs.
    * The initialized actors are added to our death watch. We also subscribe to state changes of the parser
    * and the processor.
    *
    * @param chefService        An actor selection pointing to the cluster client that talks to the chef de cuisine.
    * @param agentRunIdentifier An optional identifier for the current agent run which is usually a server side generated uuid.
    * @return The updated state data holding the appropriate actor refs.
    */
  private def initializeActorsAndState(
      chefService: ActorSelection,
      agentRunIdentifier: Option[String] = None
  ): TenseiAgentData = {
    val dataTreeManagerRef = context.actorOf(DataTreeDocumentsManager.props(agentRunIdentifier),
                                             DataTreeDocumentsManager.name)
    context watch dataTreeManagerRef
    val parserRef = context.actorOf(Parser.props(agentRunIdentifier), Parser.name)
    context watch parserRef
    parserRef ! SubscribeTransitionCallBack(self) // We need to subscribe to the callback to register state changes in the parser.
    val processorRef = context.actorOf(Processor.props(agentRunIdentifier), Processor.name)
    context watch processorRef
    processorRef ! SubscribeTransitionCallBack(self) // We need to subscribe to the callback to register state changes in the processor.
    TenseiAgentData(chef = chefService,
                    dataTreeManager = Option(dataTreeManagerRef),
                    parser = Option(parserRef),
                    processor = Option(processorRef))
  }

  /**
    * Handle an `UnreachableMember` event.
    * If the server is unreachable then we schedule a restart event.
    *
    * @param member The cluster member that has been marked unreachable.
    */
  def handleUnreachableMember(member: Member): Unit =
    if (member.roles.contains(ClusterConstants.Roles.server)) {
      log.warning("Server unreachable! Scheduling agent system restart.")
      val restartTimeout = FiniteDuration(
        context.system.settings.config
          .getDuration("tensei.agents.restart-after-unreachable-server", MILLISECONDS),
        MILLISECONDS
      )
      setTimer("RestartAgent", GlobalMessages.Restart, restartTimeout, repeat = false)
    } else if (member.roles.contains(ClusterConstants.Roles.agent)) {
      log.warning("An agent node became unreachable.")
      if (member.address.host.isDefined) {
        metrics ! ClusterMetricsListenerMessages.RemoveMetrics(member.address.host.get) // Remove node from metrics.
      } else {
        log.warning("Unreachable agent node has no hostname set!")
      }
    }

  /**
    * Handle an `ReachableMember` event.
    * If the server is reachable again we try to stop the restart event timer if it is active.
    *
    * @param member The cluster member that has been marked reachable.
    */
  def handleReachableMember(member: Member): Unit =
    if (member.roles.contains(ClusterConstants.Roles.server)) {
      if (isTimerActive("RestartAgent")) {
        log.info("Server reachable again, trying to cancel system restart!")
        cancelTimer("RestartAgent")
      }
    }

  /**
    * Report status information to the chef de cuisine.
    *
    * @param currentData  The current state's data.
    * @param currentState The current state.
    * @param chef         An option to the actor selection pointing to the chef de cuisine.
    */
  def updateAgentInformationToChef(currentData: TenseiAgentData,
                                   currentState: TenseiAgentState,
                                   chef: Option[ActorSelection] = None): Unit =
    if (chef.isDefined) {
      val currentUniqueIdentifier =
        if (currentData.currentTransformation.isDefined)
          currentData.currentTransformation.get.uniqueIdentifier
        else
          None

      import context.dispatcher
      implicit val timeout = Timeout(
        FiniteDuration(context.system.settings.config
                         .getDuration("tensei.agents.metrics.ask-timeout", MILLISECONDS),
                       MILLISECONDS)
      )

      val askMetrics =
        ask(metrics, ClusterMetricsListenerMessages.ReportMetrics).mapTo[Map[String, RuntimeStats]]
      askMetrics foreach { runtimeMetrics =>
        val workingState = new AgentWorkingState(
          id = id,
          state = currentState,
          parser = currentData.parserState,
          processor = currentData.processorState,
          runtimeStats = runtimeMetrics,
          uniqueIdentifier = currentUniqueIdentifier
        )
        chef.get ! ClusterClient.Send(s"/user/${ClusterConstants.topLevelActorNameOnServer}",
                                      workingState,
                                      localAffinity = false)
      }
    }
}
