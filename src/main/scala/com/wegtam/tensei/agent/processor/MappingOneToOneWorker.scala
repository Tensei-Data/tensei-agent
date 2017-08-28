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

import argonaut._
import Argonaut._
import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import akka.util.ByteString
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.adt.ParserDataContainer
import com.wegtam.tensei.agent.helpers.{ LoggingHelpers, ProcessorHelpers }
import com.wegtam.tensei.agent.processor.Fetcher.FetcherMessages
import com.wegtam.tensei.agent.processor.MappingOneToOneWorker.{
  MappingElementPair,
  MappingOneToOneState,
  MappingOneToOneStateData
}
import com.wegtam.tensei.agent.processor.TransformationWorker.TransformationWorkerMessages
import com.wegtam.tensei.agent.writers.BaseWriter
import com.wegtam.tensei.agent.writers.BaseWriter.{ BaseWriterMessages, WriterMessageMetaData }
import org.dfasdl.utils.AttributeNames
import org.w3c.dom.traversal.TreeWalker
import org.w3c.dom.{ Document, Element }

import scalaz._

object MappingOneToOneWorker {

  /**
    * A wrapper class for an element pair that is bound by mapping.
    *
    * @param source The source data element and the actor ref to its data tree document.
    * @param target The target data element description.
    */
  case class MappingElementPair(
      source: SourceElementAndDataTree,
      target: Element
  )

  /**
    * A sealed trait for the state of the one to one mapping actor.
    */
  sealed trait MappingOneToOneState

  /**
    * A companion object for the trait to keep the namespace clean.
    */
  object MappingOneToOneState {

    /**
      * The actor is available for work.
      */
    case object Idle extends MappingOneToOneState

    /**
      * The actor is processing a mapping.
      */
    case object Processing extends MappingOneToOneState

  }

  /**
    * The state data for the one to one mapping actor.
    *
    * @param atomics A list of atomic transformation descriptions.
    * @param currentWriterMessageNumber The current writer message number.
    * @param elementPairQueue A list of element pairs (source and target element).
    * @param elementTransformationQueue A list of transformation descriptions that have to be executed on the current element.
    * @param lastMappingKeyFieldValue An option to the last value of a mapping key field.
    * @param lastWriterMessageNumber The number of the last sent writer message.
    * @param mappingKeyField An option to the mapping key field if defined.
    * @param transformations A list of transformations descriptions.
    */
  case class MappingOneToOneStateData(
      atomics: List[AtomicTransformationDescription] = List.empty[AtomicTransformationDescription],
      currentWriterMessageNumber: Long = 0L,
      elementPairQueue: List[MappingElementPair] = List.empty[MappingElementPair],
      elementTransformationQueue: List[TransformationDescription] =
        List.empty[TransformationDescription],
      lastMappingKeyFieldValue: Option[Any] = None,
      lastWriterMessageNumber: Long = 0L,
      mappingKeyField: Option[MappingKeyFieldDefinition] = None,
      transformations: List[TransformationDescription] = List.empty[TransformationDescription]
  )

  /**
    * A factory method to create an actor that processes a one to one mapping.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @param fetcher The actor ref of the data fetcher.
    * @param maxLoops The number of times the recipe will be executed at most. This value passed down from the [[RecipeWorker]].
    * @param sequenceRow An option to the currently processed sequence row.
    * @param sourceDataTrees A list of source data trees paired with their dfasdl.
    * @param targetDfasdl The target DFASDL.
    * @param targetTree The xml document tree of the target dfasdl.
    * @param targetTreeWalker A tree walker used to traverse the target dfasdl tree.
    * @param writer An actor ref of the data writer actor.
    * @return The props the create the actor.
    */
  def props(agentRunIdentifier: Option[String],
            fetcher: ActorRef,
            maxLoops: Long,
            sequenceRow: Option[Long],
            sourceDataTrees: List[SourceDataTreeListEntry],
            targetDfasdl: DFASDL,
            targetTree: Document,
            targetTreeWalker: TreeWalker,
            writer: ActorRef): Props =
    Props(classOf[MappingOneToOneWorker],
          agentRunIdentifier,
          fetcher,
          maxLoops,
          sequenceRow,
          sourceDataTrees,
          targetDfasdl,
          targetTree,
          targetTreeWalker,
          writer)

}

/**
  * This actor processes a given mapping in the `OneToOne` mode.
  *
  * In the `OneToOne` mode each input (source) field will be mapped into its defined target field.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  * @param fetcher The actor ref of the data fetcher.
  * @param maxLoops The number of times the recipe will be executed at most. This value passed down from the [[RecipeWorker]].
  * @param sequenceRow An option to the currently processed sequence row.
  * @param sourceDataTrees A list of source data trees paired with their dfasdl.
  * @param targetDfasdl The target DFASDL
  * @param targetTree The xml document tree of the target dfasdl.
  * @param targetTreeWalker A tree walker used to traverse the target dfasdl tree.
  * @param writer An actor ref of the data writer actor.
  */
class MappingOneToOneWorker(
    agentRunIdentifier: Option[String],
    fetcher: ActorRef,
    maxLoops: Long,
    sequenceRow: Option[Long],
    sourceDataTrees: List[SourceDataTreeListEntry],
    targetDfasdl: DFASDL,
    targetTree: Document,
    targetTreeWalker: TreeWalker,
    writer: ActorRef
) extends Actor
    with FSM[MappingOneToOneState, MappingOneToOneStateData]
    with ActorLogging
    with WorkerHelpers
    with ProcessorHelpers {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  override val supervisorStrategy: SupervisorStrategy =
    AllForOneStrategy() {
      case _: Exception => Escalate
    }

  val autoIncBufferPath =
    s"../../../../../../${AutoIncrementValueBuffer.AUTO_INCREMENT_BUFFER_NAME}" // The path to the auto-increment value buffer.

  val defaultEncoding: String = {
    val root = targetTree.getDocumentElement
    if (root.hasAttribute(AttributeNames.DEFAULT_ENCODING))
      root.getAttribute(AttributeNames.DEFAULT_ENCODING)
    else
      "UTF-8"
  }

  startWith(MappingOneToOneState.Idle, MappingOneToOneStateData())

  when(MappingOneToOneState.Idle) {
    case Event(msg: MapperMessages.ProcessMapping, data) =>
      log.debug("Received process mapping message.")
      if (msg.recipeMode != Recipe.MapOneToOne) {
        log.error("Received mapping with wrong recipe mapping mode!")
        stay() using data
      } else {
        \/.fromTryCatch {
          msg.mapping.sources zip msg.mapping.targets map { t =>
            MappingElementPair(
              source = findElementAndDataTreeActorRef(t._1, sourceDataTrees).get,
              target = targetTree.getElementById(t._2.elementId)
            )
          }
        } match {
          case -\/(e) =>
            log.error(e, "An error occurred while trying to create the mapping pairs!")
            stop()
          case \/-(pairs) =>
            goto(MappingOneToOneState.Processing) using MappingOneToOneStateData(
              atomics = msg.mapping.atomicTransformations,
              elementPairQueue = pairs,
              lastWriterMessageNumber = msg.lastWriterMessageNumber,
              mappingKeyField = msg.mapping.mappingKey,
              transformations = msg.mapping.transformations
            )
        }
      }
  }

  when(MappingOneToOneState.Processing) {
    case Event(MapperMessages.ProcessNextPair, data) =>
      if (data.elementPairQueue.isEmpty) {
        // We have no more element pairs in the mapping.
        context.parent ! MapperMessages.MappingProcessed(
          lastWriterMessageNumber = data.currentWriterMessageNumber
        )
        stop()
      } else {
        // We process the next element pair.
        val pair = data.elementPairQueue.head
        val loc = FetchDataLocator(
          mappingKeyFieldId = data.mappingKeyField.map(f => Option(f.name)).getOrElse(None),
          mappingKeyFieldValue = data.lastMappingKeyFieldValue,
          sequenceRow = sequenceRow
        )
        fetcher ! FetcherMessages.FetchData(
          element = pair.source.element,
          sourceRef = pair.source.dataTreeRef,
          locator = loc,
          transformations = data.atomics
        )
        // We may need to append the auto-increment transformer if we have referenced a foreign key that is an auto-increment value.
        if (pair.target.hasAttribute(AttributeNames.UNIQUE) && pair.target.getAttribute(
              AttributeNames.UNIQUE
            ) == "true") {}
        val elementTransformations: List[TransformationDescription] =
          if (pair.target.hasAttribute(AttributeNames.DB_FOREIGN_KEY)) {
            val foreignKeyReference =
              targetTree.getElementById(pair.target.getAttribute(AttributeNames.DB_FOREIGN_KEY))
            if (foreignKeyReference == null)
              throw new IllegalArgumentException(
                s"Referenced foreign key element '${pair.target.getAttribute(AttributeNames.DB_FOREIGN_KEY)}' does not exist in target DFASDL ${targetDfasdl.id}!"
              )

            if (foreignKeyReference.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && foreignKeyReference
                  .getAttribute(AttributeNames.DB_AUTO_INCREMENT) == "true") {
              val r = ElementReference(dfasdlId = targetDfasdl.id,
                                       elementId = foreignKeyReference.getAttribute("id"))
              val fetchForeignKeyValue = TransformationDescription(
                transformerClassName = "com.wegtam.tensei.agent.transformers.FetchForeignKeyValue",
                options = TransformerOptions(
                  srcType = classOf[String],
                  dstType = classOf[String],
                  params = List(
                    ("autoIncBufferPath", autoIncBufferPath),
                    ("reference", r.asJson.nospaces)
                  )
                )
              )
              data.transformations ::: fetchForeignKeyValue :: Nil
            } else
              data.transformations
          } else
            data.transformations

        stay() using data.copy(elementTransformationQueue = elementTransformations) // Initialise the transformation queue.
      }

    case Event(c: ParserDataContainer, data) =>
      // Buffer a possible candidate for a last key field value if desired.
      val lastMappingKeyFieldValue =
        if (data.mappingKeyField.isDefined && data.lastMappingKeyFieldValue.isEmpty && c.elementId == data.mappingKeyField.get.name)
          Option(c.data) // FIXME What do we do if the last value was actually `None`?
        else
          data.lastMappingKeyFieldValue

      if (data.elementTransformationQueue.isEmpty) {
        // We can continue because no transformations need to be applied.
        val target  = data.elementPairQueue.head.target
        val default = getDefaultValue(target)
        // FIXME Check properly for empty data and replace with default value!
        val d: Any = c.data match {
          case s: ByteString => if (s.isEmpty) ByteString(default.getOrElse("")) else s
          case None          => default.getOrElse(None)
          case _             => c.data
        }
        // Calculate writer message number.
        val number =
          if (data.currentWriterMessageNumber == 0L) data.lastWriterMessageNumber + 1
          else data.currentWriterMessageNumber + 1
        // Try to create the writer message and proceed accordingly.
        createWriterMessage(target, d, number) match {
          case -\/(e) =>
            log.error(e, s"Couldn't create writer message $number!")
            stop()
          case \/-(m) =>
            // Check if we have to omit the stop sign.
            targetTreeWalker.setCurrentNode(target)
            val message =
              if (sequenceRow.getOrElse(0L) == maxLoops - 1 && targetTreeWalker
                    .nextSibling() == null)
                m.copy(options = m.options ::: List((BaseWriter.SKIP_STOP_SIGN_OPTION, "true")))
              else
                m
            // Send the message to the writer.
            writer ! message
            // Move to the next element pair.
            self ! MapperMessages.ProcessNextPair
            stay() using data.copy(currentWriterMessageNumber = number,
                                   elementPairQueue = data.elementPairQueue.tail,
                                   lastMappingKeyFieldValue = lastMappingKeyFieldValue)
        }
      } else {
        // We need to apply some additional transformations.
        val transformer = context.actorOf(TransformationWorker.props(agentRunIdentifier))
        transformer ! TransformationWorkerMessages.Start(
          container = c,
          element = data.elementPairQueue.head.source.element,
          target = self,
          transformations = data.elementTransformationQueue
        )
        stay() using data.copy(elementTransformationQueue = List.empty[TransformationDescription],
                               lastMappingKeyFieldValue = lastMappingKeyFieldValue)
      }
  }

  whenUnhandled {
    case Event(MapperMessages.Stop, data) =>
      log.debug("Received stop message.")
      stop()
  }

  onTransition {
    case MappingOneToOneState.Idle -> MappingOneToOneState.Processing =>
      self ! MapperMessages.ProcessNextPair
  }

  initialize()

  /**
    * Create a writer message from the given parameters.
    *
    * @param target      The data description.
    * @param targetData  The actual data.
    * @param number      The current writer message number.
    * @return Either a writer message or an error.
    */
  private def createWriterMessage(target: Element,
                                  targetData: Any,
                                  number: Long): Throwable \/ BaseWriterMessages.WriteData =
    \/.fromTryCatch {
      // Extract meta information.
      val charset: Option[String] =
        if (target.hasAttribute(AttributeNames.ENCODING))
          Option(target.getAttribute(AttributeNames.ENCODING))
        else
          Option(defaultEncoding)
      val meta = WriterMessageMetaData(id = target.getAttribute("id"), charset = charset)
      // Prepare the options.
      val options: List[(String, String)] =
        if (target.hasAttribute(AttributeNames.STOP_SIGN))
          List((AttributeNames.STOP_SIGN, target.getAttribute(AttributeNames.STOP_SIGN)))
        else
          List.empty[(String, String)]
      // The target data needs now to be verified and processed into the expected target format.
      val dataToBeWritten = processTargetData(targetData, target)
      // Create the writer message.
      BaseWriterMessages.WriteData(number, dataToBeWritten, options, Option(meta))
    }

}
