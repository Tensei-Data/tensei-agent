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
import com.wegtam.tensei.adt.TransformationDescription
import com.wegtam.tensei.agent.adt.{ ParserDataContainer, TransformerStatus }
import com.wegtam.tensei.agent.helpers.LoggingHelpers
import com.wegtam.tensei.agent.processor.TransformationWorker.{
  TransformationWorkerMessages,
  TransformationWorkerState,
  TransformationWorkerStateData
}
import com.wegtam.tensei.agent.transformers.BaseTransformer
import org.w3c.dom.Element

import scala.concurrent.duration._
import scalaz._

object TransformationWorker {

  /**
    * A sealed trait for the messages of the worker.
    */
  sealed trait TransformationWorkerMessages

  /**
    * A companion object for the messages trait to keep the namespace clean.
    */
  object TransformationWorkerMessages {

    /**
      * Instruct the actor to start the transformation sequence for the given data and
      * pipe the result to the specified target.
      *
      * @param container A parser data container holding the data.
      * @param element The data element description.
      * @param target An actor ref for the actor that should receive the end result.
      * @param transformations A list of transformations that must be applied.
      */
    case class Start(
        container: ParserDataContainer,
        element: Element,
        target: ActorRef,
        transformations: List[TransformationDescription]
    ) extends TransformationWorkerMessages

    /**
      * Instruct the actor to shutdown.
      */
    case object Stop extends TransformationWorkerMessages

    /**
      * A message that is send by a timer and indicates that the transformer
      * specified was unable to initialise itself within a given time frame.
      *
      * @param t The class name of the transformer.
      */
    case class PreparationTimeout(t: String) extends TransformationWorkerMessages

    /**
      * A message that is send by a timer and indicates that the transformer
      * specified was unable to execute the desired transformation within a
      * given time frame.
      *
      * @param t The class name of the transformer.
      */
    case class TransformationTimeout(t: String) extends TransformationWorkerMessages

  }

  /**
    * A sealed trait for the state of a worker.
    */
  sealed trait TransformationWorkerState

  /**
    * A companion object for the state trait to keep the namespace clean.
    */
  object TransformationWorkerState {

    /**
      * The actor is idle and waiting for a start message.
      */
    case object Idle extends TransformationWorkerState

    /**
      * The actor is in the state of transforming the given data.
      */
    case object Transforming extends TransformationWorkerState

  }

  /**
    * The state data for the worker.
    *
    * @param container A parser data container which holds the data.
    * @param target An actor ref that defines the target actor that shall receive the end result of the transformations.
    * @param transformations The list of transformations to apply.
    */
  case class TransformationWorkerStateData(
      container: Option[ParserDataContainer] = None,
      target: Option[ActorRef] = None,
      transformations: List[TransformationDescription] = List.empty[TransformationDescription]
  )

  /**
    * A factory method to create an actor.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @return The props to create an actor.
    */
  def props(agentRunIdentifier: Option[String]): Props =
    Props(classOf[TransformationWorker], agentRunIdentifier)

}

/**
  * This worker applies a list of transformations to a given data container.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  */
class TransformationWorker(agentRunIdentifier: Option[String])
    extends Actor
    with FSM[TransformationWorkerState, TransformationWorkerStateData]
    with ActorLogging {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  override val supervisorStrategy: SupervisorStrategy =
    AllForOneStrategy() {
      case _: Exception => Escalate
    }

  val prepareTransformationTimeout = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.processor.prepare-transformation-timeout", MILLISECONDS),
    MILLISECONDS
  )
  val prepareTimeoutTimerName = "PREPARE_TIMEOUT"
  val transformationTimeout = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.processor.transformation-timeout", MILLISECONDS),
    MILLISECONDS
  )
  val transformationTimeoutTimerName = "TRANSFORMATION_TIMEOUT"

  startWith(TransformationWorkerState.Idle, TransformationWorkerStateData())

  when(TransformationWorkerState.Idle) {
    case Event(msg: TransformationWorkerMessages.Start, data) =>
      log.debug("Received start message.")
      if (msg.transformations.isEmpty) {
        log.debug("No transformations defined. Relaying data to target.")
        msg.target ! msg.container
        stop()
      } else
        goto(TransformationWorkerState.Transforming) using TransformationWorkerStateData(
          container = Option(msg.container),
          target = Option(msg.target),
          transformations = msg.transformations
        )
    case Event(TransformationWorkerMessages.Stop, data) =>
      log.debug("Received stop message in idle state.")
      stop()
  }

  when(TransformationWorkerState.Transforming) {
    case Event(TransformationWorkerMessages.Stop, data) =>
      log.debug("Received stop message in transforming state.")
      stop()
    case Event(msg: TransformationWorkerMessages.PreparationTimeout, data) =>
      log.error("Transformer {} timed out while preparing for transformation!", msg.t)
      stop()
    case Event(msg: TransformationWorkerMessages.TransformationTimeout, data) =>
      log.error("Transformer {} timed out while transforming data!", msg.t)
      stop()
    case Event(BaseTransformer.ReadyToTransform, data) =>
      log.debug("Got ready message from transformer.")
      cancelTimer(prepareTimeoutTimerName)
      val dataToTransform: List[Any] = data.container.get.data match {
        case l: List[Any] => l
        case _            => List(data.container.get.data)
      }
      sender() ! BaseTransformer.StartTransformation(dataToTransform,
                                                     data.transformations.head.options)
      setTimer(
        transformationTimeoutTimerName,
        TransformationWorkerMessages.TransformationTimeout(
          data.transformations.head.transformerClassName
        ),
        transformationTimeout
      )
      stay() using data.copy(transformations = data.transformations.tail)
    case Event(msg: BaseTransformer.TransformerResponse, data) =>
      log.debug("Got transformer response.")
      cancelTimer(transformationTimeoutTimerName)
      if (msg.status != TransformerStatus.OK) {
        log.error("Transformer returned an error status!")
        stop()
      } else {
        val transformedData = if (msg.data.size > 1) msg.data else msg.data.head
        if (data.transformations.isEmpty) {
          log.debug("No transformations left.")
          data.target.get ! data.container.get.copy(data = transformedData)
          stop()
        } else {
          prepareTransformer(data.transformations.head) match {
            case -\/(failure) =>
              log.error(failure, "An error occurred while trying to initialise a transformer!")
              stop()
            case \/-(success) =>
              log.debug("Transformer initialised successfully.")
              val newContainer = data.container.get.copy(data = transformedData)
              stay() using data.copy(container = Option(newContainer))
          }
        }
      }
  }

  onTransition {
    case TransformationWorkerState.Idle -> TransformationWorkerState.Transforming =>
      prepareTransformer(nextStateData.transformations.head) match {
        case -\/(failure) =>
          log.error(failure, "An error occurred while trying to initialise a transformer!")
          self ! TransformationWorkerMessages.Stop
        case \/-(success) =>
          log.debug("Transformer initialised successfully.")
      }
  }

  initialize()

  /**
    * This function initialises a transformer actor, sends the `PrepareForTransformation` message to it
    * and starts a timer that fires after the preparation timeout.
    *
    * @param a A transformation description.
    * @return Either the actor ref of the transformer or an error.
    */
  private def prepareTransformer(a: TransformationDescription): Throwable \/ ActorRef =
    \/.fromTryCatch {
      val clazz       = Class.forName(a.transformerClassName)
      val transformer = context.actorOf(Props(clazz))
      transformer ! BaseTransformer.PrepareForTransformation
      setTimer(prepareTimeoutTimerName,
               TransformationWorkerMessages.PreparationTimeout(a.transformerClassName),
               prepareTransformationTimeout)
      transformer
    }
}
