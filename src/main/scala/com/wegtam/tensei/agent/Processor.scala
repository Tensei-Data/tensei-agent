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

import akka.actor._
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.Processor.ProcessorData
import com.wegtam.tensei.agent.Processor.ProcessorMessages.{ Completed, StartProcessingMessage }
import com.wegtam.tensei.agent.SortTransformationMappings.SortTransformationMappingsMessages
import com.wegtam.tensei.agent.generators.BaseGenerator.BaseGeneratorMessages
import com.wegtam.tensei.agent.generators.BaseGenerator.BaseGeneratorMessages.PrepareToGenerate
import com.wegtam.tensei.agent.generators.{ DrupalVanCodeGenerator, IDGenerator }
import com.wegtam.tensei.agent.helpers.{ LoggingHelpers, ProcessorHelpers }
import com.wegtam.tensei.agent.processor.RecipesWorker.RecipesWorkerMessages
import com.wegtam.tensei.agent.processor.{
  AutoIncrementValueBuffer,
  RecipesWorker,
  UniqueValueBuffer
}
import com.wegtam.tensei.agent.writers.BaseWriter._

import scala.concurrent.duration._
import scalaz._

object Processor {
  val name = "Processor"

  /**
    * Helper method to create the processor supvervisor actor.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @return The props to generate the actor.
    */
  def props(agentRunIdentifier: Option[String]): Props =
    Props(classOf[Processor], agentRunIdentifier)

  sealed trait ProcessorMessages

  object ProcessorMessages {

    case object Completed extends ProcessorMessages

    case class StartProcessingMessage(stm: AgentStartTransformationMessage,
                                      dataTreeDocs: List[ActorRef],
                                      caller: Option[ActorRef] = None)

  }

  /**
    * The state for the processor.
    *
    * @param parent        An option to the `ActorRef` of the initiator of the processing.
    * @param writer        An option to the `ActorRef` of the writer.
    * @param recipesWorker An option to the actor ref of the recipes worker that processes the cookbook.
    * @param sources       A list of connection informations for the source data.
    * @param target        Connection information for the target data
    * @param cookbook      The cookbook containing the dfasdls and mapping and transformation recipes for the data.
    * @param dataTreeDocs  A list of `ActorRef` pointing to the `DataTreeDocument` data containers.
    * @param transformers  A map holding the started transformer actors for the processing.
    * @param generatorMessages Buffer for generator messages. TODO Evaluate if this is feasible.
    * @param autoIncBuffer An option to the actor ref of the actor that buffers the auto increment values.
    * @param uniqueValueBuffer An option to the actor ref of the actor that buffers the written values of unique target elements.
    * @param failed        A flag indicating that something went wrong.
    */
  case class ProcessorData(
      parent: Option[ActorRef] = None,
      writer: Option[ActorRef] = None,
      recipesWorker: Option[ActorRef] = None,
      sources: List[ConnectionInformation] = List.empty[ConnectionInformation],
      target: Option[ConnectionInformation] = None,
      cookbook: Option[Cookbook] = None,
      dataTreeDocs: List[ActorRef] = List.empty[ActorRef],
      transformers: Map[String, Option[ActorRef]] = Map.empty[String, Option[ActorRef]],
      generatorMessages: Int = 0,
      autoIncBuffer: Option[ActorRef] = None,
      uniqueValueBuffer: Option[ActorRef] = None,
      failed: Boolean = false
  )

}

/**
  * The processor does the process the data and pipes it further on to the writers.
  *
  * @todo We need to split it up some more to really use this as the supervisor and do the work in sub actors.
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  */
class Processor(agentRunIdentifier: Option[String])
    extends Actor
    with FSM[ProcessorState, Processor.ProcessorData]
    with ActorLogging
    with ProcessorHelpers {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  val sorter = context.actorOf(SortTransformationMappings.props, SortTransformationMappings.name)

  val askTimeout = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.processor.ask-timeout", MILLISECONDS),
    MILLISECONDS
  )
  val fetchDataTimeout = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.processor.fetch-data-timeout", MILLISECONDS),
    MILLISECONDS
  )
  val fetchDataStructureTimeout = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.processor.fetch-data-structure-timeout", MILLISECONDS),
    MILLISECONDS
  )
  val applyTransformationTimeout = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.processor.transformation-timeout", MILLISECONDS),
    MILLISECONDS
  )

  // If an error occurs within one of our child actors then we want the actor to be stopped.
  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  override def postStop(): Unit = {
    log.clearMDC()
    super.postStop()
  }

  startWith(ProcessorState.Idle, ProcessorData())

  when(ProcessorState.Idle) {
    case Event(msg: StartProcessingMessage, data) =>
      log.debug("Got start processing message.")
      val recipient =
        if (msg.caller.isDefined)
          msg.caller.get
        else
          sender()
      sorter ! SortTransformationMappingsMessages.SortMappings(msg.stm.cookbook)
      goto(ProcessorState.Sorting) using ProcessorData(
        parent = Option(recipient),
        sources = msg.stm.sources,
        target = Option(msg.stm.target),
        cookbook = Option(msg.stm.cookbook),
        dataTreeDocs = msg.dataTreeDocs
      )
  }

  when(ProcessorState.Sorting) {
    case Event(SortTransformationMappingsMessages.SortedMappings(sortedCookbook), data) =>
      log.debug("Got response from sorter.")
      val writerSupervisor: ActorRef = sortedCookbook.target.fold(
        throw new RuntimeException(s"No target dfasdl in cookbook ${sortedCookbook.id}!")
      )(
        dfasdl =>
          context.actorOf(
            WriterSupervisor.props(
              agentRunIdentifier,
              dfasdl,
              data.target.getOrElse(
                throw new RuntimeException(
                  s"No target connection information for cookbook ${sortedCookbook.id}!"
                )
              )
            ),
            WriterSupervisor.name
        )
      )
      log.debug("Initialise writer supervisor...")
      writerSupervisor ! BaseWriterMessages.InitializeTarget
      context watch writerSupervisor // Set a death watch on the created writer.
      // Create the buffer for auto increment values.
      val aiBuffer = context.actorOf(AutoIncrementValueBuffer.props(agentRunIdentifier),
                                     AutoIncrementValueBuffer.AUTO_INCREMENT_BUFFER_NAME)
      context watch aiBuffer // Set a death watch.
      val uniqueBuffer = context.actorOf(UniqueValueBuffer.props(agentRunIdentifier),
                                         UniqueValueBuffer.UNIQUE_VALUE_BUFFER_NAME)
      context watch uniqueBuffer // Set a death watch.
      goto(ProcessorState.Processing) using data.copy(cookbook = Option(sortedCookbook),
                                                      writer = Option(writerSupervisor),
                                                      autoIncBuffer = Option(aiBuffer),
                                                      uniqueValueBuffer = Option(uniqueBuffer))
  }

  when(ProcessorState.Processing) {
    case Event(RecipesWorkerMessages.FinishedProcessing(cookbookId, lastWriterMessageNo), data) =>
      log.debug("Received finished processing message for cookbook {}.", cookbookId)
      if (data.cookbook.get.id == cookbookId) {
        data.writer.get ! BaseWriterMessages.CloseWriter
        data.recipesWorker.foreach(r => context unwatch r)
        goto(ProcessorState.WaitingForWriterClosing) using data.copy(recipesWorker = None)
      } else {
        log.error("Got finished processing message for unknown cookbook {}!", cookbookId)
        stay() using data
      }

    case Event(BaseWriterMessages.ReadyToWork, data) =>
      log.debug("Got ready to work signal from writer.")
      val newGeneratorMessages = startGenerators(data.cookbook.get)
      if (newGeneratorMessages == 0) {
        log.debug("Starting processing.")
        val worker = context.actorOf(RecipesWorker.props(agentRunIdentifier))
        context watch worker
        worker ! RecipesWorkerMessages.StartProcessing(
          writer = data.writer.get,
          sources = data.sources,
          target = data.target.get,
          cookbook = data.cookbook.get,
          dataTreeDocs = data.dataTreeDocs
        )
        stay() using data.copy(recipesWorker = Option(worker))
      } else {
        stay() using data.copy(generatorMessages = newGeneratorMessages)
      }
    case Event(BaseGeneratorMessages.ReadyToGenerate, data) =>
      val remainingMessages =
        if (data.generatorMessages > 0)
          data.generatorMessages - 1
        else
          0
      if (remainingMessages == 0) {
        log.debug("Starting processing.")
        val worker = context.actorOf(RecipesWorker.props(agentRunIdentifier))
        context watch worker
        worker ! RecipesWorkerMessages.StartProcessing(
          writer = data.writer.get,
          sources = data.sources,
          target = data.target.get,
          cookbook = data.cookbook.get,
          dataTreeDocs = data.dataTreeDocs
        )
        stay() using data.copy(recipesWorker = Option(worker))
      } else
        stay() using data.copy(generatorMessages = remainingMessages)
  }

  when(ProcessorState.WaitingForWriterClosing) {
    case Event(BaseWriterMessages.WriterClosed(status), data) =>
      log.debug("Writer was closed.")
      status match {
        case -\/(error) =>
          log.error("Writer reported error: {}", error)
        case \/-(success) =>
          if (data.failed)
            log.info("Writer closed successfully after failure!")
          else
            log.info("Writer closed successfully.")
      }
      // Shutdown the writer actor.
      data.writer.foreach { writer =>
        context unwatch writer
        context stop writer
      }
      // Shutdown the auto-increment buffer
      data.autoIncBuffer.foreach { b =>
        context unwatch b
        context stop b
      }
      // Shutdown the unique-value buffer
      data.uniqueValueBuffer.foreach { b =>
        context unwatch b
        context stop b
      }
      if (data.failed) {
        log.warning("Stopping processor after failure.")
        stop()
      } else {
        // Inform our possibly existing parent.
        data.parent.foreach(parent => parent ! Completed)
        goto(ProcessorState.Idle) using ProcessorData()
      }
  }

  whenUnhandled {
    case Event(Terminated(ref), data) =>
      // TODO We shouldn't always break down. Maybe restarting some died actors would be enough to continue?
      log.error("One of our child actors terminated unexpectedly at {}!", ref.path)
      if (data.writer.contains(ref)) {
        log.error("The writer actor died!")
        stop()
      } else {
        log.warning("Trying to shutdown writer properly after failure.")
        data.writer.foreach(r => r ! BaseWriterMessages.CloseWriter)
        goto(ProcessorState.WaitingForWriterClosing) using data.copy(failed = true)
      }
  }

  onTransition {
    case ProcessorState.Sorting -> ProcessorState.Processing =>
      nextStateData.writer.get ! BaseWriterMessages.AreYouReady
  }

  initialize()

  /**
    * Look through the cookbook and start the generators if needed
    *
    * @param cookbook The cookbook that will be processed.
    * @return A list of BaseGeneratorMessages that should be waited for.
    */
  private def startGenerators(cookbook: Cookbook): Int = {
    var messages: Int = 0
    val classNames =
    cookbook.recipes flatMap (
        recipe =>
          recipe.mappings flatMap (
              mapping =>
                mapping.transformations map (
                    transformation => transformation.transformerClassName
                )
          )
    )
    classNames.distinct.foreach { name =>
      if (name == "com.wegtam.tensei.agent.transformers.IDTransformer") {
        val actor = context.actorOf(IDGenerator.props, IDGenerator.name)
        messages += 1
        actor ! PrepareToGenerate
      }
      if (name == "com.wegtam.tensei.agent.transformers.DrupalVanCodeTransformer") {
        val actor = context.actorOf(DrupalVanCodeGenerator.props, DrupalVanCodeGenerator.name)
        messages += 1
        actor ! PrepareToGenerate
      }
    }
    messages
  }
}
