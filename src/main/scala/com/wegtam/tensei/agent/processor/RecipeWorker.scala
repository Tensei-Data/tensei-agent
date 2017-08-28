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

package com.wegtam.tensei.agent.processor

import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.wegtam.tensei.adt.{ DFASDL, ElementReference, MappingTransformation, Recipe }
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.helpers.LoggingHelpers
import com.wegtam.tensei.agent.processor.Fetcher.FetcherMessages
import com.wegtam.tensei.agent.processor.RecipeWorker.{
  RecipeLimits,
  RecipeWorkerMessages,
  RecipeWorkerState,
  RecipeWorkerStateData
}
import com.wegtam.tensei.agent.processor.RecipesWorker.RecipesWorkerMessages
import org.dfasdl.utils.{ AttributeNames, ElementHelpers }
import org.w3c.dom.traversal.TreeWalker
import org.w3c.dom.Element

object RecipeWorker {

  /**
    * A sealed trait for the messages of this actor.
    */
  sealed trait RecipeWorkerMessages

  /**
    * A companion object for the trait to keep the namespace clean.
    */
  object RecipeWorkerMessages {

    /**
      * Prepare the execution of the recipe.
      */
    case object Prepare extends RecipeWorkerMessages

    /**
      * Process the next mapping.
      */
    case object ProcessNextMapping extends RecipeWorkerMessages

    /**
      * Process the recipe.
      */
    case object ProcessRecipe extends RecipeWorkerMessages

    /**
      * Instruct the actor to start processing the recipe.
      *
      * @param lastWriterMessageNumber The number of the last writer message that was sent out.
      * @param sourceDataTrees A list of source data trees paired with their dfasdl.
      * @param targetDfasdl The target DFASDL.
      * @param targetTreeWalker A tree walker used to traverse the target dfasdl tree.
      * @param writer An option to an actor ref of the data writer actor.
      */
    case class Start(
        lastWriterMessageNumber: Long,
        sourceDataTrees: List[SourceDataTreeListEntry],
        targetDfasdl: DFASDL,
        targetTreeWalker: Option[TreeWalker],
        writer: Option[ActorRef]
    ) extends RecipeWorkerMessages

    /**
      * Instruct the actor to stop itself.
      */
    case object Stop extends RecipeWorkerMessages

  }

  /**
    * A sealed trait for the state of the recipe worker actor.
    */
  sealed trait RecipeWorkerState

  /**
    * A companion object for the trait to keep the namespace clean.
    */
  object RecipeWorkerState {

    /**
      * The actor has been initialised but it is not doing anything.
      */
    case object Idle extends RecipeWorkerState

    /**
      * The actor is preparing the data fetcher.
      */
    case object PreparingFetcher extends RecipeWorkerState

    /**
      * The actor is preparing the processing of the recipe.
      */
    case object PreparingSelf extends RecipeWorkerState

    /**
      * The actor is preparing the base mapping worker.
      */
    case object PreparingWorker extends RecipeWorkerState

    /**
      * The actor is processing the recipe.
      */
    case object Processing extends RecipeWorkerState

  }

  /**
    * A simple helper class to wrap the calculated limits for the processing of a recipe into.
    *
    * @param sourceMaxRows The maximum number of rows in the data source.
    * @param targetMaxRows The maximum number of rows in the target.
    */
  case class RecipeLimits(
      sourceMaxRows: Option[Long],
      targetMaxRows: Option[Long]
  )

  /**
    * A wrapper for the state of the actor.
    *
    * @param currentLoopCounter A counter indicating how many times the recipe has currently being processed.
    * @param lastWriterMessageNumber The number of the last writer message that was sent out.
    * @param limits The calculated limits for recipe processing.
    * @param mappingQueue A list of mappings that must be processed.
    * @param maxLoops An option to the maximum number of times a recipe should be executed.
    * @param sourceDataTrees A list of source data trees paired with their dfasdl.
    * @param sourceSeqsRowCount A list which stores the answers from `GetSequenceRowCount` requests for source sequences.
    * @param targetDfasdl An option to the target DFASDL.
    * @param targetHasSequences An option to a flag that indicates if the target elements are children of a sequence.
    * @param targetTreeWalker A tree walker used to traverse the target dfasdl tree.
    * @param writer An option to an actor ref of the data writer actor.
    */
  case class RecipeWorkerStateData(
      currentLoopCounter: Long = 0L,
      lastWriterMessageNumber: Long = 0L,
      limits: RecipeLimits = RecipeLimits(None, None),
      mappingQueue: List[MappingTransformation] = List.empty[MappingTransformation],
      maxLoops: Option[Long] = None,
      sourceDataTrees: List[SourceDataTreeListEntry] = List.empty[SourceDataTreeListEntry],
      sourceSeqsRowCount: Map[ElementReference, Option[Long]] =
        Map.empty[ElementReference, Option[Long]],
      targetDfasdl: Option[DFASDL] = None,
      targetHasSequences: Option[Boolean] = None,
      targetTreeWalker: Option[TreeWalker] = None,
      writer: Option[ActorRef] = None
  )

  /**
    * A factory method to create a recipe worker actor.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @param recipe The recipe that should be processed by the worker.
    * @return The props to create the actor.
    */
  def props(agentRunIdentifier: Option[String], recipe: Recipe): Props =
    Props(classOf[RecipeWorker], agentRunIdentifier, recipe)

}

/**
  * An actor that processes a recipe.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  * @param recipe The recipe that should be processed by the worker.
  */
class RecipeWorker(agentRunIdentifier: Option[String], recipe: Recipe)
    extends Actor
    with FSM[RecipeWorkerState, RecipeWorkerStateData]
    with ActorLogging
    with ElementHelpers
    with WorkerHelpers {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  override val supervisorStrategy: SupervisorStrategy =
    AllForOneStrategy() {
      case _: Exception => Escalate
    }

  val sequenceIndicatorTrigger =
    context.system.settings.config.getLong("tensei.agents.sequence-indicator-trigger")

  val fetcher = context.actorOf(Fetcher.props(agentRunIdentifier))       // Start the data fetcher actor.
  val worker  = context.actorOf(MappingWorker.props(agentRunIdentifier)) // Start our base mapping worker.

  startWith(RecipeWorkerState.Idle, RecipeWorkerStateData())

  when(RecipeWorkerState.Idle) {
    case Event(msg: RecipeWorkerMessages.Start, data) =>
      log.debug("Received start processing message.")
      val newData = RecipeWorkerStateData(
        lastWriterMessageNumber = msg.lastWriterMessageNumber,
        sourceDataTrees = msg.sourceDataTrees,
        targetDfasdl = Option(msg.targetDfasdl),
        targetTreeWalker = msg.targetTreeWalker,
        writer = msg.writer
      )
      goto(RecipeWorkerState.PreparingFetcher) using newData
  }

  when(RecipeWorkerState.PreparingFetcher) {
    case Event(FetcherMessages.RoutersInitialised, data) =>
      log.debug("Fetcher is initialised.")
      goto(RecipeWorkerState.PreparingSelf) using data
    case Event(FetcherMessages.RoutersNotInitialised, data) =>
      log.debug("Fetcher needs to initialise.")
      fetcher ! FetcherMessages.InitialiseRouters(
        sourceRefs = data.sourceDataTrees.map(_.actorRef)
      )
      stay() using data
  }

  when(RecipeWorkerState.PreparingSelf) {
    case Event(RecipeWorkerMessages.Prepare, data) =>
      log.debug("Received prepare message.")

      // Get all source ids and their parent sequences.
      val sourceIds: List[ElementReference] = recipe.mappings.flatMap(_.sources)
      val sourceSeqs: Map[ElementReference, Element] =
        getParentSequences(sourceIds, data.sourceDataTrees.map(_.toDataTreeListEntry))
      // Get all target ids and their parent sequences.
      val targetIds: List[ElementReference] = recipe.mappings.flatMap(_.targets)
      val targetSeqs: Map[ElementReference, Element] =
        getParentSequences(targetIds,
                           List(
                             DataTreeListEntry(
                               dfasdlId = data.targetDfasdl.get.id,
                               document = data.targetTreeWalker.map(_.getRoot.getOwnerDocument)
                             )
                           ))

      // Calculate how many times we have to repeat the recipe.
      // We may have several cases here.
      // 1. Source(1x), Target(1x) => Write once
      // 2. Source(1x), Target(Nx) => Write once
      // 3. Source(Nx), Target(1x) => Write once
      // 4. Source(Nx), Target(Mx) => If M is defined and M <= N then Write M times else Write N times
      if (sourceSeqs.nonEmpty && targetSeqs.nonEmpty) {
        val targetSequenceCounts = (for (s <- targetSeqs.values)
          yield
            if (s.hasAttribute(AttributeNames.FIXED_SEQUENCE_COUNT))
              Option(s.getAttribute(AttributeNames.FIXED_SEQUENCE_COUNT).toLong)
            else if (s.hasAttribute(AttributeNames.SEQUENCE_MAX))
              Option(s.getAttribute(AttributeNames.SEQUENCE_MAX).toLong)
            else
              None // Potentially endless sequence (only limited by source data).
        ).toVector
        val targetMaxRows
          : Option[Long] = targetSequenceCounts.min // TODO If we ever support writing into multiple target sequences in one recipe, we'll need to change this behaviour!

        // Create the placeholder list and request sequence row counts in the process.
        val newSeqsRowCount: Map[ElementReference, Option[Long]] = sourceSeqs.keySet.map { ref =>
          log.debug("Requesting SeqRowCount for {}", ref)
          findElementAndDataTreeActorRef(ref, data.sourceDataTrees).foreach(
            t => t.dataTreeRef ! DataTreeDocumentMessages.GetSequenceRowCount(ref)
          )
          ref -> None
        }.toMap

        // Wait for the answers and initialise the source row counter with `Long.MaxValue`!
        stay() using data.copy(
          limits =
            RecipeLimits(sourceMaxRows = Option(Long.MaxValue), targetMaxRows = targetMaxRows),
          sourceSeqsRowCount = newSeqsRowCount,
          targetHasSequences = Option(targetSeqs.nonEmpty)
        )
      } else
        goto(RecipeWorkerState.PreparingWorker) using data.copy(
          limits = RecipeLimits(None, None),
          targetHasSequences = Option(targetSeqs.nonEmpty)
        ) // We can move to the next phase.

    case Event(msg: DataTreeDocumentMessages.SequenceRowCount, data) =>
      log.debug("Received sequence row count message for {}.", msg.ref)
      val newSeqsRowCount = data.sourceSeqsRowCount.filterNot(_._1 == msg.ref) // Remove sequence from queue.
      // The smaller number (or `None`) is preferred.
      val newMinSeqRowCount = List(data.limits.sourceMaxRows, msg.rows).min
      // Create the new state data.
      val newData: RecipeWorkerStateData = data.copy(
        limits = data.limits.copy(sourceMaxRows = newMinSeqRowCount),
        sourceSeqsRowCount = newSeqsRowCount
      )
      // Calculate the maximum number of loops if applicable.
      val maxLoops =
        if (newData.targetHasSequences.contains(true) && newData.limits.sourceMaxRows.isDefined)
          newData.limits.targetMaxRows
            .map(t => Option(t))
            .getOrElse(newData.limits.sourceMaxRows) // Write M times if M is defined and M <= N write M else Write N times (see comment above).
        else
          None

      if (newSeqsRowCount.isEmpty)
        goto(RecipeWorkerState.PreparingWorker) using newData.copy(maxLoops = maxLoops) // We have collected all row counts.
      else
        stay() using newData.copy(maxLoops = maxLoops) // We still need to wait for some row counts.
  }

  when(RecipeWorkerState.PreparingWorker) {
    case Event(MapperMessages.Ready, data) =>
      log.debug("Mapping worker is ready.")
      goto(RecipeWorkerState.Processing) using data
  }

  when(RecipeWorkerState.Processing) {
    case Event(RecipeWorkerMessages.ProcessRecipe, data) =>
      log.debug("Received process recipe message.")
      // We send ourselfs the message to start processing mappings.
      self ! RecipeWorkerMessages.ProcessNextMapping
      // Stay in processing mode with a freshly initialised mapping queue.
      stay() using data.copy(mappingQueue = recipe.mappings)

    case Event(RecipeWorkerMessages.ProcessNextMapping, data) =>
      log.debug("Received process mapping message.")
      if (data.mappingQueue.isEmpty) {
        // We have no more mappings in the queue.
        if (data.maxLoops.exists(m => data.currentLoopCounter < m - 1)) {
          // But we are within loop that is not finished yet.
          self ! RecipeWorkerMessages.ProcessRecipe // Initiate a new loop.
          if (data.currentLoopCounter > 0 && data.currentLoopCounter % sequenceIndicatorTrigger == 0) {
            log.info(
              "Processed {} rows of {}.",
              data.currentLoopCounter,
              data.maxLoops.getOrElse("∞")
            ) // TODO Replace this with a pubsub event that can be consumed by subscribers.
          }
          stay() using data.copy(currentLoopCounter = data.currentLoopCounter + 1) // Stay and increase the loop counter.
        } else {
          log.debug("Stopping recipe worker...")
          log.info("Processor finished recipe '{}'.", recipe.id) // FIXME Logging for internal information (Protokollierung)
          // This means we have finished processing the recipe.
          context.parent ! RecipesWorkerMessages.RecipeProcessed(lastWriterMessageNumber =
                                                                   data.lastWriterMessageNumber,
                                                                 currentLoopCounter =
                                                                   data.currentLoopCounter)
          stop() // Stop the actor.
        }
      } else {
        // We still have mappings to process.
        if (data.maxLoops.isDefined || (data.limits.sourceMaxRows.isDefined || (data.limits.sourceMaxRows.isEmpty && data.mappingQueue.nonEmpty && data.targetHasSequences
              .contains(false)))) {
          // We enter this branch if one of the following conditions is met:
          // 1 : The variable `maxLoops` is defined e.g. we have a source and a target sequence.
          // 2 : We have data from a source sequence.
          // 3 : We do not have data from a source sequence, but
          //     we have mappings and the target has no sequence.
          //     This allows the processing of e.g. CSVToJSON, the start and the end element or the header of a csv file.

          val seqRow = if (data.currentLoopCounter > 0) Option(data.currentLoopCounter) else None
          worker ! MapperMessages.ProcessMapping(
            mapping = data.mappingQueue.head,
            lastWriterMessageNumber = data.lastWriterMessageNumber,
            maxLoops = data.maxLoops.getOrElse(0L),
            recipeMode = recipe.mode,
            sequenceRow = seqRow
          )

          stay() using data.copy(mappingQueue = data.mappingQueue.tail) // Prepare for next `ProcessMapping` message.
        } else {
          log.warning("No source data was found for recipe {}!", recipe.id)
          log.debug("Stopping recipe worker...")
          // We inform our parent that we are finished.
          // TODO Check if this is okay or if we should produce a harder error here.
          context.parent ! RecipesWorkerMessages.RecipeProcessed(lastWriterMessageNumber =
                                                                   data.lastWriterMessageNumber,
                                                                 currentLoopCounter =
                                                                   data.currentLoopCounter)
          stop()
        }
      }

    case Event(msg: MapperMessages.MappingProcessed, data) =>
      self ! RecipeWorkerMessages.ProcessNextMapping
      stay() using data.copy(lastWriterMessageNumber = msg.lastWriterMessageNumber)
  }

  whenUnhandled {
    case Event(RecipeWorkerMessages.Stop, data) =>
      log.debug("Received stop message.")
      stop()
  }

  onTransition {
    case RecipeWorkerState.Idle -> RecipeWorkerState.PreparingFetcher =>
      fetcher ! FetcherMessages.AreRoutersInitialised
    case RecipeWorkerState.PreparingFetcher -> RecipeWorkerState.PreparingSelf =>
      self ! RecipeWorkerMessages.Prepare
    case RecipeWorkerState.PreparingSelf -> RecipeWorkerState.PreparingWorker =>
      worker ! MapperMessages.Initialise(
        fetcher = fetcher,
        sourceDataTrees = nextStateData.sourceDataTrees,
        targetDfasdl = nextStateData.targetDfasdl.get,
        targetTreeWalker = nextStateData.targetTreeWalker.get,
        writer = nextStateData.writer.get
      )
    case RecipeWorkerState.PreparingWorker -> RecipeWorkerState.Processing =>
      self ! RecipeWorkerMessages.ProcessRecipe
  }

  initialize()

  /**
    * Loops through all given element ids and tries to determine if they have a parent sequence.
    *
    * @param refs A list of element references.
    * @param dfasdlTrees A list of dfasdl document trees that should contain the given elements.
    * @return A map holding a sequence element using the element reference of the sequence as a key.
    */
  private def getParentSequences(
      refs: List[ElementReference],
      dfasdlTrees: List[DataTreeListEntry]
  ): Map[ElementReference, Element] = {
    val seqs = scala.collection.mutable.Map.empty[ElementReference, Element]
    refs.foreach(
      ref =>
        dfasdlTrees
          .find(_.dfasdlId == ref.dfasdlId)
          .flatMap(_.document.map(tree => tree.getElementById(ref.elementId)).filter(_ != null))
          .flatMap(e => getParentSequence(e))
          .foreach(
            e =>
              seqs.put(ElementReference(dfasdlId = ref.dfasdlId, elementId = e.getAttribute("id")),
                       e)
        )
    )
    seqs.toMap
  }
}
