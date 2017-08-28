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

import java.nio.charset.Charset

import argonaut._
import Argonaut._
import akka.actor._
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.adt.ParserDataContainer
import com.wegtam.tensei.agent.helpers.{ LoggingHelpers, ProcessorHelpers }
import com.wegtam.tensei.agent.processor.Fetcher.FetcherMessages
import com.wegtam.tensei.agent.processor.MappingAllToAllWorker.{
  MappingAllToAllState,
  MappingAllToAllStateData
}
import com.wegtam.tensei.agent.processor.TransformationWorker.TransformationWorkerMessages
import com.wegtam.tensei.agent.writers.BaseWriter
import com.wegtam.tensei.agent.writers.BaseWriter.{ BaseWriterMessages, WriterMessageMetaData }
import org.dfasdl.utils.{ AttributeNames, DataElementType }
import org.w3c.dom.{ Document, Element }
import org.w3c.dom.traversal.TreeWalker

import scalaz._

object MappingAllToAllWorker {

  /**
    * A sealed trait for the state of the all to all mapping actor.
    */
  sealed trait MappingAllToAllState

  /**
    * A companion for the trait to keep the namespace clean.
    */
  object MappingAllToAllState {

    /**
      * The actor is available for work.
      */
    case object Idle extends MappingAllToAllState

    /**
      * The actor is fetching the source data fields.
      */
    case object Fetching extends MappingAllToAllState

    /**
      * The actor is processing a mapping.
      */
    case object Processing extends MappingAllToAllState

  }

  /**
    * The state data for the all to all mapping actor.
    *
    * @param atomics A list of atomic transformation descriptions.
    * @param currentWriterMessageNumber The current writer message number.
    * @param elementTransformationQueue A list of transformation descriptions that have to be executed on the current element.
    * @param lastMappingKeyFieldValue An option to the last value of a mapping key field.
    * @param lastWriterMessageNumber The number of the last sent writer message.
    * @param mappingKeyField An option to the mapping key field if defined.
    * @param sourceDataBuffer A buffer for the source data values which have to be prefetched in all to all mode.
    * @param sourceFields A list of source data fields that need to be fetched.
    * @param targetFields A list of target data fields.
    * @param transformations A list of transformations descriptions.
    * @param transformedSourceData A buffer for the transformed source data.
    */
  case class MappingAllToAllStateData(
      atomics: List[AtomicTransformationDescription] = List.empty[AtomicTransformationDescription],
      currentWriterMessageNumber: Long = 0L,
      elementTransformationQueue: List[TransformationDescription] =
        List.empty[TransformationDescription],
      lastMappingKeyFieldValue: Option[Any] = None,
      lastWriterMessageNumber: Long = 0L,
      mappingKeyField: Option[MappingKeyFieldDefinition] = None,
      sourceDataBuffer: List[ParserDataContainer] = List.empty[ParserDataContainer],
      sourceFields: List[SourceElementAndDataTree] = List.empty[SourceElementAndDataTree],
      targetFields: List[Element] = List.empty[Element],
      transformations: List[TransformationDescription] = List.empty[TransformationDescription],
      transformedSourceData: List[Any] = List.empty[Any]
  )

  /**
    * A factory method to create an actor that processes an all to all mapping.
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
    Props(classOf[MappingAllToAllWorker],
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
  * This actor processes a given mapping in the `AllToAll` mode.
  *
  * In the `AllToAll` mode all input (source) fields will be mapped into all target fields.
  * Therefore we collect the source data into a list and work on that list.
  * This list is then "written" into each of the target fields.
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
  */
class MappingAllToAllWorker(
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
    with FSM[MappingAllToAllState, MappingAllToAllStateData]
    with ActorLogging
    with WorkerHelpers
    with ProcessorHelpers {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  val autoIncBufferPath =
    s"../../../../../../${AutoIncrementValueBuffer.AUTO_INCREMENT_BUFFER_NAME}" // The path to the auto-increment value buffer.
  val defaultEncoding: String = {
    val root = targetTree.getDocumentElement
    if (root.hasAttribute(AttributeNames.DEFAULT_ENCODING))
      root.getAttribute(AttributeNames.DEFAULT_ENCODING)
    else
      "UTF-8"
  }

  startWith(MappingAllToAllState.Idle, MappingAllToAllStateData())

  when(MappingAllToAllState.Idle) {
    case Event(msg: MapperMessages.ProcessMapping, data) =>
      log.debug("Received process mapping message.")
      if (msg.recipeMode != Recipe.MapAllToAll) {
        log.error("Received mapping with wrong recipe mapping mode!")
        stay() using data
      } else {
        \/.fromTryCatch(
          if (msg.mapping.sources.isEmpty) List.empty[SourceElementAndDataTree]
          else
            msg.mapping.sources.map(id => findElementAndDataTreeActorRef(id, sourceDataTrees).get)
        ) match {
          case -\/(e) =>
            log.error(e, "An error occurred while trying to calculate the source data refs!")
            stop()
          case \/-(pairs) =>
            \/.fromTryCatch(msg.mapping.targets.map { targetElement =>
              val e = targetTree.getElementById(targetElement.elementId)
              require(e ne null, s"Target element $targetElement not found in target DFASDL tree!")
              e
            }) match {
              case -\/(e) =>
                log.error(e, "An error occurred while trying to get the target data elements!")
                stop()
              case \/-(targets) =>
                // If we have no source fields we move directly to the processing state. Otherwise to the fetching state.
                if (pairs.isEmpty)
                  goto(MappingAllToAllState.Processing) using MappingAllToAllStateData(
                    atomics = msg.mapping.atomicTransformations,
                    elementTransformationQueue = msg.mapping.transformations,
                    lastWriterMessageNumber = msg.lastWriterMessageNumber,
                    mappingKeyField = msg.mapping.mappingKey,
                    sourceDataBuffer = List.empty[ParserDataContainer],
                    sourceFields = pairs,
                    targetFields = targets,
                    transformations = msg.mapping.transformations
                  )
                else
                  goto(MappingAllToAllState.Fetching) using MappingAllToAllStateData(
                    atomics = msg.mapping.atomicTransformations,
                    lastWriterMessageNumber = msg.lastWriterMessageNumber,
                    mappingKeyField = msg.mapping.mappingKey,
                    sourceFields = pairs,
                    targetFields = targets,
                    transformations = msg.mapping.transformations
                  )
            }
        }
      }
  }

  when(MappingAllToAllState.Fetching) {
    case Event(c: ParserDataContainer, data) =>
      // Buffer a possible candidate for a last key field value if desired.
      val lastMappingKeyFieldValue =
        if (data.mappingKeyField.isDefined && data.lastMappingKeyFieldValue.isEmpty && c.elementId == data.mappingKeyField.get.name)
          Option(c.data) // FIXME What do we do if the last value was actually `None`?
        else
          data.lastMappingKeyFieldValue

      val buffer =
        if (data.sourceDataBuffer.contains(c)) {
          log.warning("Data for element {} already received! Ignoring message!", c.elementId)
          data.sourceDataBuffer
        } else
          data.sourceDataBuffer ::: c :: Nil

      if (buffer.size == data.sourceFields.size) {
        log.debug("Fetched all source data fields, moving to processing.")
        goto(MappingAllToAllState.Processing) using data.copy(
          elementTransformationQueue = data.transformations,
          lastMappingKeyFieldValue = lastMappingKeyFieldValue,
          sourceDataBuffer = buffer
        )
      } else {
        // Request the next missing data field.
        val sids    = buffer.map(_.elementId)
        val missing = data.sourceFields.filterNot(f => sids.contains(f.element.getAttribute("id")))
        missing.headOption.foreach { m =>
          val loc =
            FetchDataLocator(mappingKeyFieldId =
                               data.mappingKeyField.map(f => Option(f.name)).getOrElse(None),
                             mappingKeyFieldValue = lastMappingKeyFieldValue,
                             sequenceRow = sequenceRow)
          fetcher ! FetcherMessages.FetchData(
            element = m.element,
            sourceRef = m.dataTreeRef,
            locator = loc,
            transformations = data.atomics
          )
        }
        stay() using data.copy(lastMappingKeyFieldValue = lastMappingKeyFieldValue,
                               sourceDataBuffer = buffer)
      }
  }

  when(MappingAllToAllState.Processing) {
    case Event(MapperMessages.ProcessNextPair, data) =>
      if (data.transformedSourceData.isEmpty && data.sourceDataBuffer.nonEmpty) {
        // We need to apply transformations to the collected source data.
        val d: List[Any] = data.sourceDataBuffer.map(_.data)

        val elementTransformationQueue: List[TransformationDescription] =
          if (data.targetFields.exists(_.hasAttribute(AttributeNames.DB_FOREIGN_KEY))) {
            val ts =
              data.targetFields.filter(_.hasAttribute(AttributeNames.DB_FOREIGN_KEY)).flatMap {
                e =>
                  val foreignKeyReference =
                    targetTree.getElementById(e.getAttribute(AttributeNames.DB_FOREIGN_KEY))
                  if (foreignKeyReference == null)
                    throw new IllegalArgumentException(
                      s"Referenced foreign key element '${e.getAttribute(AttributeNames.DB_FOREIGN_KEY)}' does not exist in target DFASDL ${targetDfasdl.id}!"
                    )

                  if (foreignKeyReference
                        .hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && foreignKeyReference
                        .getAttribute(AttributeNames.DB_AUTO_INCREMENT) == "true") {
                    val r = ElementReference(dfasdlId = targetDfasdl.id,
                                             elementId = foreignKeyReference.getAttribute("id"))
                    val fetchForeignKeyValue = TransformationDescription(
                      transformerClassName =
                        "com.wegtam.tensei.agent.transformers.FetchForeignKeyValue",
                      options = TransformerOptions(
                        srcType = classOf[String],
                        dstType = classOf[String],
                        params = List(
                          ("autoIncBufferPath", autoIncBufferPath),
                          ("reference", r.asJson.nospaces)
                        )
                      )
                    )
                    data.elementTransformationQueue ::: fetchForeignKeyValue :: Nil
                  } else
                    data.elementTransformationQueue
              }

            // This is needed to avoid returning an empty list by chance.
            if (ts.nonEmpty)
              ts
            else
              data.elementTransformationQueue
          } else
            data.elementTransformationQueue

        if (elementTransformationQueue.nonEmpty) {
          // Create a transformer worker and start it.
          val transformer = context.actorOf(TransformationWorker.props(agentRunIdentifier))
          transformer ! TransformationWorkerMessages.Start(
            container = ParserDataContainer(data = d,
                                            elementId =
                                              data.sourceFields.head.element.getAttribute("id")),
            element = data.sourceFields.head.element,
            target = self,
            transformations = elementTransformationQueue
          )

          stay() using data.copy(
            elementTransformationQueue = List.empty[TransformationDescription]
          )
        } else {
          // There are no transformations pending.

          self ! MapperMessages.ProcessNextPair

          stay() using data.copy(
            sourceDataBuffer = List.empty[ParserDataContainer],
            sourceFields = List.empty[SourceElementAndDataTree],
            transformedSourceData = d
          )
        }
      } else {
        // Process the next target element if it exists.
        data.targetFields.headOption
          .map { target =>
            // Buffer target data.
            val targetData = data.transformedSourceData
            // Combine the target data and replace it with a default value if needed.
            val default = getDefaultValue(target)
            val writerData: Any = getDataElementType(target.getNodeName) match {
              case DataElementType.BinaryDataElement =>
                val fd = targetData.foldLeft(Array.empty[Byte])(
                  (left, right) => Array.concat(left, right.asInstanceOf[Array[Byte]])
                )
                if (fd.isEmpty)
                  default
                    .map(s => s.getBytes(Charset.defaultCharset()))
                    .getOrElse(Array.empty[Byte])
                else
                  fd
              case DataElementType.StringDataElement =>
                // We need to filter out `None` values before we fold!
                if (targetData.length > 1) {
                  val sd =
                    targetData.filterNot(_ == None).foldLeft("")((left, right) => s"$left$right")
                  if (sd.isEmpty)
                    // If all elements are `None`, we must return the `None` type
                    if (targetData.count(_ == None) == targetData.length)
                      // If the and only if the default value is not empty, we must return the default value
                      if (default.isDefined && default.nonEmpty)
                        default.get
                      else
                        None
                    else
                      default.getOrElse("")
                  else
                    sd
                } else
                  targetData
                    .filterNot(_ == None)
                    .headOption
                    .getOrElse(default.getOrElse(None)) // FIXME: Workaround for T-1017 for not loosing the data type
              case DataElementType.UnknownElement =>
                log.error(
                  "The target element '{}({})' is no data element! This may produce unpredictable results!",
                  target.getNodeName,
                  target.getAttribute("id")
                )
                // FIXME The fallback behaviour allows some nifty things like creating json without an explicit json writer, but maybe we should omit the data here?
                // We fall back to string data element procedure.
                val sd =
                  targetData.filterNot(_ == None).foldLeft("")((left, right) => s"$left$right")
                if (sd.isEmpty)
                  default.getOrElse("")
                else
                  sd
            }
            // Calculate writer message number.
            val number =
              if (data.currentWriterMessageNumber == 0L) data.lastWriterMessageNumber + 1
              else data.currentWriterMessageNumber + 1
            // Try to create the writer message and proceed accordingly.
            createWriterMessage(data.targetFields.head, writerData, number) match {
              case -\/(e) =>
                log.error(e, s"Couldn't create writer message $number!")
                stop()
              case \/-(m) =>
                // Check if we have to omit the stop sign.
                targetTreeWalker.setCurrentNode(data.targetFields.head)
                val message =
                  if (sequenceRow.getOrElse(0L) == maxLoops - 1 && targetTreeWalker
                        .nextSibling() == null)
                    m.copy(
                      options = m.options ::: List((BaseWriter.SKIP_STOP_SIGN_OPTION, "true"))
                    )
                  else
                    m
                // Send the message to the writer.
                writer ! message
                // Move to the next element pair.
                self ! MapperMessages.ProcessNextPair
                stay() using data.copy(currentWriterMessageNumber = number,
                                       targetFields = data.targetFields.tail)
            }
          }
          .getOrElse {
            // We have no more element pairs in the mapping.
            context.parent ! MapperMessages.MappingProcessed(
              lastWriterMessageNumber = data.currentWriterMessageNumber
            )
            stop()
          }
      }

    case Event(c: ParserDataContainer, data) =>
      log.debug("Received result of transformer worker.")

      val transformedData: List[Any] = c.data match {
        case l: List[Any] => l
        case _            => List(c.data)
      }

      self ! MapperMessages.ProcessNextPair

      stay() using data.copy(
        sourceDataBuffer = List.empty[ParserDataContainer],
        sourceFields = List.empty[SourceElementAndDataTree],
        transformedSourceData = transformedData
      )
  }

  whenUnhandled {
    case Event(MapperMessages.Stop, data) =>
      log.debug("Received stop message.")
      stop()
  }

  onTransition {
    case MappingAllToAllState.Idle -> MappingAllToAllState.Fetching =>
      // Fire the first fetch request.
      nextStateData.sourceFields.headOption.foreach { m =>
        val loc = FetchDataLocator(
          mappingKeyFieldId =
            nextStateData.mappingKeyField.map(f => Option(f.name)).getOrElse(None),
          mappingKeyFieldValue = nextStateData.lastMappingKeyFieldValue,
          sequenceRow = sequenceRow
        )
        fetcher ! FetcherMessages.FetchData(
          element = m.element,
          sourceRef = m.dataTreeRef,
          locator = loc,
          transformations = nextStateData.atomics
        )
      }

    case MappingAllToAllState.Fetching -> MappingAllToAllState.Processing =>
      self ! MapperMessages.ProcessNextPair

    case MappingAllToAllState.Idle -> MappingAllToAllState.Processing =>
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
