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

package com.wegtam.tensei.agent.writers

import java.io.{ File, OutputStream }
import java.nio.file.{ Files, StandardOpenOption }
import java.time.LocalDateTime

import akka.actor._
import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import akka.util.ByteString
import argonaut.Argonaut._
import argonaut._
import com.wegtam.tensei.adt.{ ConnectionInformation, DFASDL }
import com.wegtam.tensei.agent.helpers.ArgonautJavaTime._
import com.wegtam.tensei.agent.helpers.LoggingHelpers
import com.wegtam.tensei.agent.processor.UniqueValueBuffer
import com.wegtam.tensei.agent.processor.UniqueValueBuffer.UniqueValueBufferMessages
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages.{ AreYouReady, ReadyToWork }
import com.wegtam.tensei.agent.writers.BaseWriter.State.{ Closing, Initializing, Working }
import com.wegtam.tensei.agent.writers.BaseWriter._
import com.wegtam.tensei.agent.writers.JsonFileWriterActor.JsonFileWriterActorMessages.CloseResources
import com.wegtam.tensei.agent.writers.JsonFileWriterActor.{
  JsonFileWriterData,
  WriteStructureDirection
}
import org.dfasdl.utils._
import org.w3c.dom.traversal.{ DocumentTraversal, NodeFilter }
import org.w3c.dom.{ Document, Element }

import scala.annotation.tailrec
import scala.collection.SortedSet
import scala.collection.immutable.VectorBuilder
import scala.concurrent.duration._
import scalaz.Scalaz._
import scalaz._

object JsonFileWriterActor {

  /**
    * Helper method to create a json file writer actor.
    *
    * @param target              The connection information for the target data sink.
    * @param dfasdl              The dfasdl describing the target file. It is needed to write sequence columns in the correct order.
    * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
    * @return The props to generate the actor.
    */
  def props(target: ConnectionInformation,
            dfasdl: DFASDL,
            agentRunIdentifier: Option[String]): Props =
    Props(new JsonFileWriterActor(target, dfasdl, agentRunIdentifier))

  /**
    * A sealed trait for messages that are specific for this actor.
    */
  sealed trait JsonFileWriterActorMessages

  /**
    * A companion object to keep the namespace clean.
    */
  object JsonFileWriterActorMessages {

    /**
      * Tell the actor to close the resources.
      */
    case object CloseResources extends JsonFileWriterActorMessages

  }

  /**
    * A class that buffers the state of the file writer.
    *
    * @param closeRequester     An option to the actor ref that requested the closing of the writer.
    * @param lastDataElementId  An option to the ID of the last written data element.
    * @param messages           The message buffer with the already received writer messages.
    * @param readyRequests      A list of actor refs that have asked if we are ready to work.
    * @param writer             A java file writer.
    */
  final case class JsonFileWriterData(
      closeRequester: Option[ActorRef],
      lastDataElementId: Option[String],
      messages: SortedSet[BaseWriterMessages.WriteData],
      readyRequests: List[ActorRef],
      writer: Option[OutputStream]
  )

  /**
    * This object holds several implicit argonaut codecs for encoding types into json.
    * It can be imported where needed.
    */
  object JsonCodecs {

    implicit lazy val DateEncode: EncodeJson[java.sql.Date] = EncodeJson(
      (d: java.sql.Date) => jString(d.toString)
    )

    implicit lazy val TimeEncode: EncodeJson[java.sql.Time] = EncodeJson(
      (t: java.sql.Time) => jString(t.toString)
    )

    implicit lazy val TimestampEncode: EncodeJson[java.sql.Timestamp] = EncodeJson(
      (t: java.sql.Timestamp) => jString(t.toString)
    )

    implicit lazy val LocalDateTimeEncode: EncodeJson[LocalDateTime] = EncodeJson(
      (l: LocalDateTime) => jString(l.toString)
    )

    implicit lazy val BigDecimalEncode: EncodeJson[java.math.BigDecimal] = EncodeJson(
      (b: java.math.BigDecimal) => jString(b.toString)
    )

  }

  /**
    * A sealed trait for the "direction" when traversin the json structure tree.
    */
  sealed trait WriteStructureDirection

  /**
    * Keep the namespace clean via a companion object for the trait.
    */
  object WriteStructureDirection {

    /**
      * Implies that we need to "open" the structures, e.g. write opening markers and attribute names...
      */
    case object Open extends WriteStructureDirection

    /**
      * Implies that we need to "close" the structures, e.g. write closing markers.
      */
    case object Close extends WriteStructureDirection

  }
}

/**
  * An actor that writes given informations into a JSON file.
  *
  * @param target              The connection information for the target data sink.
  * @param dfasdl              The dfasdl describing the target JSON file. It is needed to write sequence columns in the correct order.
  * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
  */
class JsonFileWriterActor(target: ConnectionInformation,
                          dfasdl: DFASDL,
                          agentRunIdentifier: Option[String])
    extends BaseWriter(target = target)
    with Actor
    with FSM[BaseWriter.State, JsonFileWriterData]
    with ActorLogging
    with BaseWriterFunctions
    with DocumentHelpers {
  // Create a distributed pub sub mediator.
  import DistributedPubSubMediator.Publish
  private val mediator = DistributedPubSub(context.system).mediator

  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  lazy val writeTriggerInterval = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.writers.file.write-interval", MILLISECONDS),
    MILLISECONDS
  )
  setTimer("writeTrigger",
           BaseWriterMessages.WriteBufferedData,
           writeTriggerInterval,
           repeat = true)

  val JSON_SEQUENCE_OPEN = "["

  val JSON_SEQUENCE_CLOSE = "]"

  val JSON_OBJECT_OPEN = "{"

  val JSON_OBJECT_CLOSE = "}"

  val JSON_ATTRIBUTE_MARKER = ":"

  val JSON_ELEMENT_SEPARATOR = ","

  lazy val dfasdlTree: Document = createNormalizedDocument(dfasdl.content)
  lazy val orderedDataElementIds: Vector[String] =
    try {
      val doc       = dfasdlTree
      val traversal = doc.asInstanceOf[DocumentTraversal]
      val iterator = traversal.createNodeIterator(doc.getDocumentElement,
                                                  NodeFilter.SHOW_ELEMENT,
                                                  new DataElementFilter(),
                                                  true)
      val builder  = Vector.newBuilder[String]
      var nextNode = iterator.nextNode()
      while (nextNode != null) {
        builder += nextNode.asInstanceOf[Element].getAttribute("id")
        nextNode = iterator.nextNode()
      }
      builder.result()
    } catch {
      case e: Throwable =>
        log.error(
          e,
          "An error occurred while trying to calculate the ordered target data element ids!"
        )
        Vector.empty[String]
    }
  lazy val uniqueDataElementIds: Set[String] =
    getUniqueDataElements(dfasdlTree).map(_.getAttribute("id"))

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.clearMDC()
    cancelTimer("writeTrigger")
    super.postStop()
  }

  startWith(
    Initializing,
    JsonFileWriterData(closeRequester = None,
                       lastDataElementId = None,
                       messages = SortedSet.empty[BaseWriterMessages.WriteData],
                       readyRequests = List.empty[ActorRef],
                       writer = None)
  )

  when(Initializing) {
    case Event(BaseWriterMessages.InitializeTarget, data) =>
      initializeTarget
      goto(Working) using data.copy(
        writer = Option(
          Files.newOutputStream(new File(target.uri.getSchemeSpecificPart).toPath,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.APPEND)
        )
      )
    case Event(AreYouReady, data) =>
      stay() using data.copy(readyRequests = sender() :: data.readyRequests)
  }

  when(Working) {
    case Event(msg: BaseWriterMessages.WriteData, data) =>
      log.debug("Got write request.")
      stay() using data.copy(messages = data.messages + msg)
    case Event(BaseWriterMessages.WriteBatchData(batch), data) =>
      log.debug("Got bulk write request containing {} messages.", batch.size)
      stay() using data.copy(messages = data.messages ++ batch)
    case Event(BaseWriterMessages.WriteBufferedData, data) =>
      log.debug("Received write buffered data request.")
      val lastElementId: Option[String] = data.writer.fold({
        log.error("No json file writer defined!")
        data.lastDataElementId
      })(
        w =>
          writeMessages(w, data.messages, data.lastDataElementId) match {
            case -\/(failure) => throw failure
            case \/-(success) => success
        }
      )
      stay() using data.copy(messages = SortedSet.empty[BaseWriterMessages.WriteData],
                             lastDataElementId = lastElementId)
    case Event(AreYouReady, data) =>
      sender() ! ReadyToWork
      stay() using data
    case Event(BaseWriterMessages.CloseWriter, data) =>
      log.debug("Got close request for JsonFileWriter.")
      val lastElementId: Option[String] = data.writer.fold({
        log.error("No json file writer defined!")
        data.lastDataElementId
      })(
        w =>
          writeMessages(w, data.messages, data.lastDataElementId) match {
            case -\/(failure) => throw failure
            case \/-(success) => success
        }
      )
      self ! CloseResources
      goto(Closing) using data.copy(closeRequester = Option(sender()),
                                    lastDataElementId = lastElementId)
  }

  when(Closing) {
    case Event(CloseResources, data) =>
      data.writer.foreach(w => w.close())
      if (data.closeRequester.isDefined)
        data.closeRequester.get ! BaseWriterMessages.WriterClosed("".right[String])
      stay() using data
  }

  onTransition {
    case _ -> Working => nextStateData.readyRequests foreach (a => a ! ReadyToWork)
    case _ -> Closing =>
      nextStateData.lastDataElementId.foreach { id =>
        val openElements = getParentElements(id)
        stateData.writer.fold(log.error("No json file writer defined!"))(
          w =>
            writeStructure(w, openElements, DEFAULT_CHARSET, WriteStructureDirection.Close) match {
              case -\/(f) =>
                log.error(f, "An error occurred while closing open elements!")
              case \/-(s) =>
                log.debug("Successfully closed open elements.")
          }
        )
      }
  }

  whenUnhandled {
    case Event(msg: BaseWriterMessages.WriteData, data) =>
      log.warning("Got unhandled writer message!")
      stay() using data
    case Event(msg: BaseWriterMessages.WriteBatchData, data) =>
      log.warning("Got unhandled bulk writer message!")
      stay() using data
  }

  initialize()

  /**
    * Initialize the target.
    *
    * @return Returns `true` upon success and `false` if an error occurred.
    */
  override def initializeTarget: Boolean = {
    val file = new File(target.uri.getSchemeSpecificPart)
    file.createNewFile()
  }

  /**
    * Process a batch of `WriterMessage`s and write them to the target filewriter.
    *
    * @param target The filewriter that should consume the data.
    * @param messages A list of `WriterMessage`s.
    * @param lastWrittenElementId An option to the last written ID of an element.
    * @return Either an option to the id of the last written element or an exception.
    */
  private def writeMessages(target: OutputStream,
                            messages: SortedSet[BaseWriterMessages.WriteData],
                            lastWrittenElementId: Option[String]): Throwable \/ Option[String] = {
    val uniqueValues = getUniqueMessageValues(dfasdl, messages, uniqueDataElementIds)
    // Process the messages via `foldLeft` and exit hard via Exception that will be catched.
    val lastWrittenId = \/.fromTryCatch(
      messages.foldLeft[Throwable \/ Option[String]](lastWrittenElementId.right)(
        (prev, next) =>
          prev match {
            case -\/(failure) => throw failure
            case \/-(success) => writeMessage(target, next, success)
        }
      )
    )
    target.flush() // Force writing of the buffered data to disk.
    uniqueValues.foreach(
      p =>
        mediator ! Publish(UniqueValueBuffer.UNIQUE_VALUE_BUFFER_CHANNEL,
                           UniqueValueBufferMessages.StoreS(p._1, p._2))
    )
    lastWrittenId match {
      case -\/(failure) => failure.left
      case \/-(success) =>
        success match {
          case -\/(f) => f.left
          case \/-(s) => s.right
        }
    }
  }

  /**
    * Pass the given data into a filewrite.
    *
    * @param target               The filewriter that should consume the data.
    * @param message              The `WriterMessage` containing the data and possible options.
    * @param lastWrittenElementId An option to the last written ID of an element.
    * @return Either an option to the id of the written element or an exception.
    */
  private def writeMessage(
      target: OutputStream,
      message: BaseWriterMessages.WriteData,
      lastWrittenElementId: Option[String] = None
  ): Throwable \/ Option[String] = {
    val charset = getOption(AttributeNames.ENCODING, message.options).getOrElse(DEFAULT_CHARSET)

    \/.fromTryCatch(
      createJsonStructure(target,
                          message.metaData.map(m => Option(m.id)).getOrElse(None),
                          lastWrittenElementId,
                          charset) match {
        case -\/(failure) =>
          log.error(failure, "An error occurred while trying to create json structure!")
          throw failure
        case \/-(success) =>
          import com.wegtam.tensei.agent.writers.JsonFileWriterActor.JsonCodecs._
          message.data match {
            case binary: Array[Byte] => target.write(binary)
            case value: java.math.BigDecimal =>
              target.write(value.asJson.nospaces.getBytes(charset))
            case byteString: ByteString =>
              target.write(byteString.utf8String.asJson.nospaces.getBytes(charset))
            case value: Long =>
              target
                .write(value.toString.getBytes(charset)) // TODO: We hardcode the `toString` of `Long` here because argonaut adds quotes! Maybe this can be solved better in the future.
            case value: String              => target.write(value.asJson.nospaces.getBytes(charset))
            case value: java.sql.Date       => target.write(value.asJson.nospaces.getBytes(charset))
            case value: java.time.LocalDate => target.write(value.asJson.nospaces.getBytes(charset))
            case value: java.sql.Time       => target.write(value.asJson.nospaces.getBytes(charset))
            case value: java.time.LocalTime => target.write(value.asJson.nospaces.getBytes(charset))
            case value: java.sql.Timestamp  => target.write(value.asJson.nospaces.getBytes(charset))
            case value: java.time.OffsetDateTime =>
              target.write(value.asJson.nospaces.getBytes(charset))
            case value: LocalDateTime => target.write(value.asJson.nospaces.getBytes(charset))
            case None =>
              val dummyValue: Option[String] = None
              target.write(dummyValue.asJson.nospaces.getBytes(charset))
          }

          message.metaData.map(m => Option(m.id)).getOrElse(None)
      }
    )
  }

  /**
    * Create the neccessary JSON structure for the current element. Check whether the structure must be opened
    * or closed and whether the current element expects a label.
    *
    * @param target     The filewriter that consumes the data.
    * @param currentId  The current ID of the DFASDL element.
    * @param lastId     The ID of the element before the current element.
    * @param charset    The actual charset for the data.
    * @return Success or failure.
    */
  private def createJsonStructure(target: OutputStream,
                                  currentId: Option[String],
                                  lastId: Option[String],
                                  charset: String): Throwable \/ String =
    \/.fromTryCatch {
      val cid            = currentId.get
      val currentElement = dfasdlTree.getElementById(cid)
      if (currentElement != null) {
        val lastElement = lastId.map(l => Option(dfasdlTree.getElementById(l))).getOrElse(None)

        if (lastElement.isEmpty) {
          // No element has been written before us.
          val parentElements = getParentElements(cid).reverse
          writeStructure(target, parentElements, charset) match {
            case -\/(f) => throw f
            case \/-(s) => s
          }
        } else {
          val parentElementsCurrent = getParentElements(cid).reverse
          if (cid == lastId.get) {
            // We are within a sequence (json array) and are about to be written again.
            target.write(JSON_ELEMENT_SEPARATOR.getBytes(charset))
            "Wrote json element separator."
          } else {
            val parentElementsLast = getParentElements(lastElement.get.getAttribute("id")).reverse

            if (parentElementsCurrent.length == parentElementsLast.length &&
                parentElementsCurrent.reverse.tail
                  .zip(parentElementsLast.reverse.tail)
                  .forall(e => e._1.getAttribute("id") == e._2.getAttribute("id"))) {
              // The previous element is our sibling within the same structure.
              if (currentElement.hasAttribute(AttributeNames.JSON_ATTRIBUTE_NAME)) {
                // We must include the json attribute name.
                target.write(s"""$JSON_ELEMENT_SEPARATOR"${currentElement.getAttribute(
                  AttributeNames.JSON_ATTRIBUTE_NAME
                )}"$JSON_ATTRIBUTE_MARKER""".getBytes(charset))
                s"Wrote json element separator, attribute name and marker."
              } else {
                // There is no json attribute name.
                target.write(JSON_ELEMENT_SEPARATOR.getBytes(charset))
                "Wrote json element separator."
              }
            } else {
              // We need to find traverse the paths of the elements until the meeting point within the structure.
              parentElementsCurrent
                .zip(parentElementsLast)
                .takeWhile(e => e._1.getAttribute("id") == e._2.getAttribute("id"))
                .map(_._1)
                .reverse
                .headOption match {
                case None =>
                  val currentIds = parentElementsCurrent.map(_.getAttribute("id"))
                  val lastIds    = parentElementsLast.map(_.getAttribute("id"))
                  throw new NoSuchElementException(
                    s"No meeting point in json structures! $currentIds <-> $lastIds"
                  )
                case Some(lce) =>
                  val uniqueCurrentPathElements =
                    parentElementsCurrent.dropWhile(_.getAttribute("id") != lce.getAttribute("id"))
                  val uniqueLastPathElements =
                    parentElementsLast.dropWhile(_.getAttribute("id") != lce.getAttribute("id"))
                  // First we need to close the structure of the previous element.
                  writeStructure(target,
                                 uniqueLastPathElements.tail.reverse,
                                 charset,
                                 WriteStructureDirection.Close) match {
                    case -\/(failure) => throw failure
                    case \/-(success) =>
                      val children = getChildDataElementsFromElement(lce)
                      val lastChildMatched = lastElement.exists(
                        l =>
                          children.reverse.headOption
                            .exists(_.getAttribute("id") == l.getAttribute("id"))
                      )
                      val currentChildMatched = children.headOption.exists(
                        _.getAttribute("id") == currentElement.getAttribute("id")
                      )
                      if (currentChildMatched && lastChildMatched)
                        target.write(
                          s"$JSON_OBJECT_CLOSE$JSON_ELEMENT_SEPARATOR$JSON_OBJECT_OPEN"
                            .getBytes(charset)
                        ) // Close the last element and open the next one.
                      else
                        target.write(JSON_ELEMENT_SEPARATOR.getBytes(charset)) // First we need to insert an element seperator before we open up the next structure.
                      // Now we need to open the structure of the current element.
                      writeStructure(target, uniqueCurrentPathElements.tail, charset) match {
                        case -\/(f) => throw f
                        case \/-(s) => s
                      }
                  }
              }
            }
          }
        }
      } else
        throw new RuntimeException(s"Element with id $cid not found in DFASDL!")
    }

  /**
    * Write the JSON structure for the given elements.
    *
    * @param target         The filewriter that consumes the data.
    * @param parentElements The elements that will be parsed for the structure.
    * @param charset        The charset for the data.
    * @param direction      The write direction.
    * @return Either a success message or an exception in case of a failure.
    */
  private def writeStructure(
      target: OutputStream,
      parentElements: Vector[Element] = Vector.empty[Element],
      charset: String,
      direction: WriteStructureDirection = WriteStructureDirection.Open
  ): Throwable \/ String =
    \/.fromTryCatch {
      if (parentElements.nonEmpty) {
        val written = parentElements.zipWithIndex.map { zip =>
          val currentElement = zip._1
          val currentIndex   = zip._2

          getElementType(currentElement.getNodeName) match {
            case ElementType.DataElement =>
              direction match {
                case WriteStructureDirection.Open =>
                  if (currentElement.hasAttribute(AttributeNames.JSON_ATTRIBUTE_NAME)) {
                    target.write(s""""${currentElement.getAttribute(
                      AttributeNames.JSON_ATTRIBUTE_NAME
                    )}"$JSON_ATTRIBUTE_MARKER""".getBytes(charset))
                    "Wrote json attribute name and marker.".right
                  } else
                    "Nothing to write (no json attribute name).".right
                case WriteStructureDirection.Close =>
                  "Closing data elements needs no action".right
              }
            case ElementType.StructuralElement =>
              if (StructureElementType.isSequence(
                    getStructureElementType(currentElement.getNodeName)
                  )) {
                val s =
                  direction match {
                    case WriteStructureDirection.Open =>
                      JSON_SEQUENCE_OPEN
                    case WriteStructureDirection.Close =>
                      JSON_SEQUENCE_CLOSE
                  }
                target.write(s.getBytes(charset))
                s"Structure for sequence written: $s".right
              } else {
                if (currentElement.hasAttribute(AttributeNames.JSON_ATTRIBUTE_NAME)) {
                  val s =
                    direction match {
                      case WriteStructureDirection.Open =>
                        val default = s""""${currentElement.getAttribute(
                          AttributeNames.JSON_ATTRIBUTE_NAME
                        )}"$JSON_ATTRIBUTE_MARKER"""
                        // Check we are the direct child of a sequence.
                        parentElements
                          .index(currentIndex + 1)
                          .map(
                            next =>
                              if (StructureElementType
                                    .isSequence(getStructureElementType(next.getNodeName)))
                                default // We don't open the object for a sequence (json array).
                              else
                                s"$default$JSON_OBJECT_OPEN"
                          )
                          .getOrElse(s"$default$JSON_OBJECT_OPEN")
                      case WriteStructureDirection.Close =>
                        parentElements
                          .index(currentIndex - 1)
                          .map(
                            next =>
                              if (StructureElementType
                                    .isSequence(getStructureElementType(next.getNodeName)))
                                "" // Don't close the object because we didn't open one for a sequence.
                              else
                              JSON_OBJECT_CLOSE
                          )
                          .getOrElse(JSON_OBJECT_CLOSE)
                    }
                  target.write(s.getBytes(charset))
                  s"Structure for element written: $s".right
                } else {
                  getChildDataElementsFromElement(currentElement).headOption
                    .map { child =>
                      if (child.hasAttribute(AttributeNames.JSON_ATTRIBUTE_NAME)) {
                        val s =
                          direction match {
                            case WriteStructureDirection.Open =>
                              JSON_OBJECT_OPEN
                            case WriteStructureDirection.Close =>
                              JSON_OBJECT_CLOSE
                          }
                        target.write(s.getBytes(charset))
                        s"Structure JSON object written: $s".right
                      } else
                        s"No structure written for JSON object ${currentElement.getAttribute("id")}.".right
                    }
                    .getOrElse(
                      s"No structure written for JSON object ${currentElement.getAttribute("id")}.".right
                    )
                }
              }
            case ElementType.RootElement =>
              "No structure written for dfasdl root element.".right
            case ElementType.UnknownElement =>
              log.error("Unknown element {}", currentElement.getNodeName)
              throw new RuntimeException(
                s"Structure unknown element: ${currentElement.getNodeName}"
              )
          }
        }
        written.filter(_.isRight).head match {
          case -\/(f) => throw new RuntimeException("An unknown error occurred!")
          case \/-(s) => s
        }
      } else
        throw new RuntimeException("No elements available to write a structure.")
    }

  /**
    * Return the a Vector with the parent elements of the current element.
    *
    * @param id The ID of the current element.
    * @return A Vector with parent elements that are above the current element in the DFASDL tree.
    */
  private def getParentElements(id: String): Vector[Element] = {
    val elements = new VectorBuilder[Element]()

    @tailrec
    def loop(id: String): Vector[Element] = {
      val element = dfasdlTree.getElementById(id)
      if (element == null) elements.result()
      else {
        val parent = element.getParentNode
        if (parent == null) elements.result()
        else {
          elements += element
          loop(parent.asInstanceOf[Element].getAttribute("id"))
        }
      }
    }

    loop(id)
  }
}
