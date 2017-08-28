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
import com.wegtam.tensei.adt.{ ConnectionInformation, Cookbook, DFASDL, Recipe }
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.helpers.LoggingHelpers
import com.wegtam.tensei.agent.processor.RecipeWorker.RecipeWorkerMessages
import com.wegtam.tensei.agent.processor.RecipesWorker.{
  RecipesWorkerMessages,
  RecipesWorkerState,
  RecipesWorkerStateData
}
import org.dfasdl.utils.DocumentHelpers
import org.w3c.dom.traversal.{ DocumentTraversal, NodeFilter, TreeWalker }

import scalaz._

object RecipesWorker {

  /**
    * A sealed trait for the messages that are send and received by this actor.
    */
  sealed trait RecipesWorkerMessages

  /**
    * A companion object for the trait to keep the namespace clean.
    */
  object RecipesWorkerMessages {

    /**
      * A message to report that a cookbook has been processed.
      *
      * @param cookbookId The ID of the cookbook.
      * @param lastWriterMessageNumber The number of the last writer message that was sent out.
      */
    case class FinishedProcessing(
        cookbookId: String,
        lastWriterMessageNumber: Long
    )

    /**
      * Start processing the next recipe in the pipe.
      */
    case object ProcessNextRecipe extends RecipesWorkerMessages

    /**
      * A message to report that a recipe has been processed.
      *
      * @param lastWriterMessageNumber The number of the last writer message that was sent out.
      * @param currentLoopCounter The number of times the recipe has been processed.
      */
    case class RecipeProcessed(
        lastWriterMessageNumber: Long,
        currentLoopCounter: Long = 0L
    ) extends RecipesWorkerMessages

    /**
      * Start processing the recipes within the given cookbook.
      *
      * @param writer       An actor ref to a writer.
      * @param sources      A list of connection informations for the data sources.
      * @param target       The connection information for the target.
      * @param cookbook     The actual cookbook that contains the recipes.
      * @param dataTreeDocs A list of actor refs to data trees that hold the source data.
      */
    case class StartProcessing(
        writer: ActorRef,
        sources: List[ConnectionInformation],
        target: ConnectionInformation,
        cookbook: Cookbook,
        dataTreeDocs: List[ActorRef]
    ) extends RecipesWorkerMessages

    /**
      * Instruct the actor to stop itself.
      */
    case object Stop extends RecipesWorkerMessages

  }

  /**
    * A sealed trait for the state of the recipes worker.
    */
  sealed trait RecipesWorkerState

  /**
    * A companion object for the trait to keep the namespace clean.
    */
  object RecipesWorkerState {

    /**
      * The actor is available for work.
      */
    case object Idle extends RecipesWorkerState

    /**
      * The actor is preparing to process the recipes.
      */
    case object Preparing extends RecipesWorkerState

    /**
      * The actor is currently processing recipes.
      */
    case object Processing extends RecipesWorkerState

  }

  /**
    * The state data for the process recipes actor.
    *
    * @param cookbook An option to the cookbook that includes the recipes to process.
    * @param lastWriterMessageNumber The number of the last writer message that was sent out.
    * @param recipeQueue A list of recipes that is bound to be processed.
    * @param sourceDataTrees A list of source data trees paired with their dfasdl.
    * @param targetDfasdl An option to the target DFASDL.
    * @param targetTreeWalker A tree walker used to traverse the target dfasdl tree.
    * @param writer   An option to an actor ref of the data writer actor.
    */
  case class RecipesWorkerStateData(
      cookbook: Option[Cookbook] = None,
      lastWriterMessageNumber: Long = 0L,
      recipeQueue: List[Recipe] = List.empty[Recipe],
      sourceDataTrees: List[SourceDataTreeListEntry] = List.empty[SourceDataTreeListEntry],
      targetDfasdl: Option[DFASDL] = None,
      targetTreeWalker: Option[TreeWalker] = None,
      writer: Option[ActorRef] = None
  )

  /**
    * A factory method to create the actor.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @return The props to create the actor.
    */
  def props(agentRunIdentifier: Option[String]): Props =
    Props(classOf[RecipesWorker], agentRunIdentifier)

}

/**
  * This actor processes a list of recipes.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  */
class RecipesWorker(agentRunIdentifier: Option[String])
    extends Actor
    with FSM[RecipesWorkerState, RecipesWorkerStateData]
    with ActorLogging
    with DocumentHelpers {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  override val supervisorStrategy: SupervisorStrategy =
    AllForOneStrategy() {
      case _: Exception => Escalate
    }

  startWith(RecipesWorkerState.Idle, RecipesWorkerStateData())

  when(RecipesWorkerState.Idle) {
    case Event(msg: RecipesWorkerMessages.StartProcessing, data) =>
      log.debug("Received start processing message.")
      if (msg.target.dfasdlRef.isEmpty) {
        log.error("No DFASDL-Reference defined in target connection!")
        stay() using data
      } else {
        val dfasdl = msg.cookbook.findDFASDL(msg.target.dfasdlRef.get)
        if (dfasdl.isEmpty) {
          log.error("DFASDL referenced by {} not found in cookbook {}!",
                    msg.target.dfasdlRef.get,
                    msg.cookbook.id)
          stay() using data
        } else {
          // Try to create a normalised xml document from the target DFASDL.
          \/.fromTryCatch(createNormalizedDocument(dfasdl.get.content)) match {
            case -\/(e) =>
              log.error(e, "An error occurred while trying to create the target document tree!")
              stay() using data
            case \/-(targetTree) =>
              // Create treewalker from a traversal for calculating some target element stuff later on.
              \/.fromTryCatch(targetTree.asInstanceOf[DocumentTraversal]) match {
                case -\/(e) =>
                  log.error(e, "Couldn't create traversal instance!")
                  stay() using data
                case \/-(traversal) =>
                  // We restrict the treewalker to data elements only!
                  \/.fromTryCatch(
                    traversal.createTreeWalker(targetTree.getDocumentElement,
                                               NodeFilter.SHOW_ELEMENT,
                                               new DataElementFilter(),
                                               true)
                  ) match {
                    case -\/(e) =>
                      log.error(e, "Couldn't create tree walker!")
                      stay() using data
                    case \/-(treeWalker) =>
                      val newState = RecipesWorkerStateData(
                        cookbook = Option(msg.cookbook),
                        sourceDataTrees = msg.dataTreeDocs.map(
                          ref =>
                            SourceDataTreeListEntry(dfasdlId = dfasdl.get.id,
                                                    document = None,
                                                    actorRef = ref)
                        ),
                        targetDfasdl = dfasdl,
                        targetTreeWalker = Option(treeWalker),
                        writer = Option(msg.writer)
                      )
                      goto(RecipesWorkerState.Preparing) using newState
                  }
              }
          }
        }
      }
  }

  when(RecipesWorkerState.Preparing) {
    case Event(msg: DataTreeDocumentMessages.XmlStructure, data) =>
      log.debug("Received xml structure.")
      val index = data.sourceDataTrees.indexWhere(_.actorRef.path == sender().path)
      if (index < 0) {
        log.warning("Received xml structure from unknown data tree document!")
        stay() using data
      } else {
        val e = data
          .sourceDataTrees(index)
          .copy(dfasdlId = msg.dfasdlId, document = Option(msg.document))
        val newData = data.copy(
          recipeQueue = data.cookbook.get.recipes,
          sourceDataTrees = data.sourceDataTrees.updated(index, e)
        )
        if (newData.sourceDataTrees.exists(_.document.isEmpty))
          stay() using newData // There are still document structures missing.
        else
          goto(RecipesWorkerState.Processing) using newData // We have all structures and can move on.
      }
  }

  when(RecipesWorkerState.Processing) {
    case Event(RecipesWorkerMessages.ProcessNextRecipe, data) =>
      log.debug("Received process next recipe message.")
      if (data.recipeQueue.isEmpty) {
        // Tell our parent that we're done and stop.
        context.parent ! RecipesWorkerMessages.FinishedProcessing(
          cookbookId = data.cookbook.get.id,
          lastWriterMessageNumber = data.lastWriterMessageNumber
        )
        stop()
      } else {
        // Create a worker for the recipe and instruct it to start processing.
        val worker = context.actorOf(
          RecipeWorker.props(agentRunIdentifier = agentRunIdentifier,
                             recipe = data.recipeQueue.head)
        )
        worker ! RecipeWorkerMessages.Start(
          lastWriterMessageNumber = data.lastWriterMessageNumber,
          sourceDataTrees = data.sourceDataTrees,
          targetDfasdl = data.targetDfasdl.get,
          targetTreeWalker = data.targetTreeWalker,
          writer = data.writer
        )
        stay() using data // Stay and wait for the `RecipeProcessed` message.
      }

    case Event(msg: RecipesWorkerMessages.RecipeProcessed, data) =>
      log.debug("Received recipe processed message.")
      // Trigger the next recipe.
      self ! RecipesWorkerMessages.ProcessNextRecipe
      // Stay in processing mode
      stay() using data.copy(
        lastWriterMessageNumber = msg.lastWriterMessageNumber,
        recipeQueue = data.recipeQueue.tail
      )
  }

  whenUnhandled {
    case Event(RecipesWorkerMessages.Stop, data) =>
      log.debug("Received stop message.")
      stop()
  }

  onTransition {
    case RecipesWorkerState.Idle -> RecipesWorkerState.Preparing =>
      // Instruct all actors without document to send us their xml structure.
      nextStateData.sourceDataTrees
        .filter(_.document.isEmpty)
        .foreach(e => e.actorRef ! DataTreeDocumentMessages.ReturnXmlStructure)
    case RecipesWorkerState.Preparing -> RecipesWorkerState.Processing =>
      // Send initial processing message.
      self ! RecipesWorkerMessages.ProcessNextRecipe
  }

  initialize()

}
