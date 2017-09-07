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

import java.util.UUID

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{ Actor, ActorLogging, AllForOneStrategy, FSM, Props, SupervisorStrategy }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.wegtam.tensei.adt.Recipe
import com.wegtam.tensei.agent.helpers.LoggingHelpers

import scalaz._

object MappingWorker {

  /**
    * A factory method to create the mapping worker actor.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @return The props to create the actor.
    */
  def props(agentRunIdentifier: Option[String]): Props =
    Props(new MappingWorker(agentRunIdentifier))

}

/**
  * An actor that orchestrates the processing of mappings.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  */
class MappingWorker(agentRunIdentifier: Option[String])
    extends Actor
    with FSM[MapperState, MapperStateData]
    with ActorLogging {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  override val supervisorStrategy: SupervisorStrategy =
    AllForOneStrategy() {
      case _: Exception => Escalate
    }

  startWith(MapperState.Empty, MapperStateData())

  when(MapperState.Empty) {
    case Event(msg: MapperMessages.Initialise, _) =>
      log.debug("Received initialisation message.")
      // Inform sender and switch state.
      sender() ! MapperMessages.Ready
      goto(MapperState.Ready) using MapperStateData(
        fetcher = Option(msg.fetcher),
        sourceDataTrees = msg.sourceDataTrees,
        targetDfasdl = Option(msg.targetDfasdl),
        targetTree = Option(msg.targetTreeWalker.getRoot.getOwnerDocument),
        targetTreeWalker = Option(msg.targetTreeWalker),
        writer = Option(msg.writer)
      )
  }

  when(MapperState.Ready) {
    case Event(msg: MapperMessages.ProcessMapping, data) =>
      log.debug("Received process mapping message.")
      val uuid = UUID.randomUUID().toString
      \/.fromTryCatch(msg.recipeMode match {
        case Recipe.MapAllToAll =>
          context.actorOf(
            MappingAllToAllWorker.props(
              agentRunIdentifier = agentRunIdentifier,
              fetcher = data.fetcher.get,
              maxLoops = msg.maxLoops,
              sequenceRow = msg.sequenceRow,
              sourceDataTrees = data.sourceDataTrees,
              targetDfasdl = data.targetDfasdl.get,
              targetTree = data.targetTree.get,
              targetTreeWalker = data.targetTreeWalker.get,
              writer = data.writer.get
            ),
            s"MappingAllToAllWorker-$uuid"
          )
        case Recipe.MapOneToOne =>
          context.actorOf(
            MappingOneToOneWorker.props(
              agentRunIdentifier = agentRunIdentifier,
              fetcher = data.fetcher.get,
              maxLoops = msg.maxLoops,
              sequenceRow = msg.sequenceRow,
              sourceDataTrees = data.sourceDataTrees,
              targetDfasdl = data.targetDfasdl.get,
              targetTree = data.targetTree.get,
              targetTreeWalker = data.targetTreeWalker.get,
              writer = data.writer.get
            ),
            s"MappingOneToOneWorker-$uuid"
          )
      }) match {
        case -\/(failure) =>
          // TODO Check if we should propagate the error via pubsub and if we really need to stop here.
          log.error(failure, "Couldn't create appropriate mapping workers!")
          stop()
        case \/-(worker) =>
          // Pipe the message through to the worker.
          worker ! msg
          stay() using data
      }

    case Event(msg: MapperMessages.MappingProcessed, data) =>
      // Propagate the message upwards and wait for further instructions.
      context.parent ! msg
      stay() using data
  }

  whenUnhandled {
    case Event(MapperMessages.Stop, _) =>
      log.debug("Received stop message.")
      stop()
  }

  initialize()

}
