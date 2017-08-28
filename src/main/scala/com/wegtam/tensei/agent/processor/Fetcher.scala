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

import akka.actor._
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import akka.routing.{ ActorRefRoutee, RoundRobinRoutingLogic, Router }
import com.wegtam.tensei.adt.AtomicTransformationDescription
import com.wegtam.tensei.agent.helpers.LoggingHelpers
import com.wegtam.tensei.agent.processor.Fetcher.{
  FetcherMessages,
  FetcherState,
  FetcherStateData
}
import com.wegtam.tensei.agent.processor.FetcherWorker.FetcherWorkerMessages
import org.w3c.dom.Element

object Fetcher {

  /**
    * A sealed trait for the messages of this actor.
    */
  sealed trait FetcherMessages

  /**
    * A companion object for the messages trait to keep the namespace clean.
    */
  object FetcherMessages {

    /**
      * Asks the actor to tell us if the routers have been initialised.
      */
    case object AreRoutersInitialised extends FetcherMessages

    /**
      * Instruct the fetcher to retrieve the data described by the parameters.
      * If a list of atomic transformations is provided then these transformations
      * are applied to the extracted data before it is returned.
      *
      * @param element         The element describing the data.
      * @param sourceRef       The actor ref of the data tree document holding the data.
      * @param locator         A helper class holding the parameters that specify the "location" of the data.
      * @param transformations The list of atomic transformations to apply.
      */
    case class FetchData(
        element: Element,
        sourceRef: ActorRef,
        locator: FetchDataLocator,
        transformations: List[AtomicTransformationDescription] =
          List.empty[AtomicTransformationDescription]
    ) extends FetcherMessages

    /**
      * Tell the actor to initialise the routers for the given source refs.
      *
      * @param sourceRefs A list of actor refs pointing to data tree documents.
      */
    case class InitialiseRouters(sourceRefs: List[ActorRef]) extends FetcherMessages

    /**
      * Tells that the routers have been initialised properly.
      */
    case object RoutersInitialised extends FetcherMessages

    /**
      * Tells that the routers have not been initialised properly.
      */
    case object RoutersNotInitialised extends FetcherMessages

    /**
      * Instruct the fetcher to stop itself.
      */
    case object Stop extends FetcherMessages

  }

  /**
    * A sealed trait for the state of a fetcher.
    */
  sealed trait FetcherState

  /**
    * A companion object to keep the namespace clean.
    */
  object FetcherState {

    case object Uninitialised extends FetcherState

    case object Ready extends FetcherState

  }

  /**
    * The data of a fetcher state.
    *
    * @param routers A map of source actor refs pointing to their routers.
    */
  case class FetcherStateData(routers: Map[ActorRef, Router] = Map.empty[ActorRef, Router])

  /**
    * A factory method to create the fetcher actor.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @return The props to create the actor.
    */
  def props(agentRunIdentifier: Option[String]): Props =
    Props(classOf[Fetcher], agentRunIdentifier)

}

/**
  * This actor is the main supervisor for fetching data from data tree documents and
  * applying atomic transformations if feasible.
  *
  * If a request if received a [[FetcherWorker]] child is spawned and instructed to
  * fullfill the request.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  */
class Fetcher(agentRunIdentifier: Option[String])
    extends Actor
    with FSM[FetcherState, FetcherStateData]
    with ActorLogging {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  startWith(FetcherState.Uninitialised, FetcherStateData())

  when(FetcherState.Uninitialised) {
    case Event(FetcherMessages.InitialiseRouters(sourceRefs), data) =>
      log.debug("Initialising routers")
      val routers = sourceRefs.map { ref =>
        val routees = Vector.fill(10) {
          val r = context.actorOf(
            FetcherWorker.props(agentRunIdentifier = agentRunIdentifier, source = ref)
          )
          context watch r
          ActorRefRoutee(r)
        }
        (ref, Router(RoundRobinRoutingLogic(), routees))
      }.toMap
      sender() ! FetcherMessages.RoutersInitialised
      goto(FetcherState.Ready) using FetcherStateData(routers = routers)

    case Event(FetcherMessages.AreRoutersInitialised, data) =>
      sender() ! FetcherMessages.RoutersNotInitialised
      stay() using data
  }

  when(FetcherState.Ready) {
    case Event(Terminated(ref), data) =>
      log.debug("One of our routers terminated. Restarting.")
      val modifiedRouter = data.routers.find(p => p._2.routees.contains(ActorRefRoutee(ref))).map {
        info =>
          val sourceRef = info._1
          val router    = info._2
          val removed   = router.removeRoutee(ref)
          val r = context.actorOf(
            FetcherWorker.props(agentRunIdentifier = agentRunIdentifier, source = sourceRef)
          )
          context watch r
          (sourceRef, removed.addRoutee(r))
      }
      val routers = modifiedRouter.map(r => data.routers + r).getOrElse(data.routers)
      stay() using data.copy(routers = routers)

    case Event(FetcherMessages.AreRoutersInitialised, data) =>
      sender() ! FetcherMessages.RoutersInitialised
      stay() using data

    case Event(FetcherMessages.FetchData(element, sourceRef, locator, transformations), data) =>
      log.debug("Received fetch data request.")
      data
        .routers(sourceRef)
        .route(FetcherWorkerMessages.Fetch(element, locator, sender(), transformations), sender())
      stay() using data
  }

  whenUnhandled {
    case Event(FetcherMessages.Stop, data) =>
      log.debug("Received stop request.")
      stop()
  }

  initialize()
}
