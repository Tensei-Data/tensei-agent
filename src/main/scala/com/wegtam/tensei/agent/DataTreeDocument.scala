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
import akka.cluster.{ Cluster, Member, MemberStatus }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import akka.remote.RemoteScope
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.{
  DataTreeDocumentMessages,
  DataTreeDocumentState,
  DataTreeDocumentStateData
}
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.adt.ParserDataContainer
import com.wegtam.tensei.agent.helpers.{ GenericHelpers, LoggingHelpers, XmlHelpers }
import com.wegtam.tensei.agent.parsers.BaseParserMessages
import org.dfasdl.utils._
import org.w3c.dom.Document

import scalaz._

object DataTreeDocument {

  /**
    * Helper method for creating a data tree document actor.
    *
    * @param dfasdl              The dfasdl that provides information for the data structure.
    * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
    * @param idWhiteList         A set containing element ids that should be saved. If the set is empty then all(!) elements will be saved!
    * @return The props to generate an actor.
    */
  def props(dfasdl: DFASDL, agentRunIdentifier: Option[String], idWhiteList: Set[String]): Props =
    Props(classOf[DataTreeDocument], dfasdl, agentRunIdentifier, idWhiteList)

  sealed trait DataTreeDocumentMessages

  object DataTreeDocumentMessages {

    /**
      * Find the data container of the given element id that holds the specified data.
      * This message is required if we mix data from several data sources which are "linked"
      * by a mapping key field. Usually the `elementId` would be the id of the mapping key field
      * and the data it's actual data.
      *
      * @param elementId The ID of the data element.
      * @param data      The actual data which must match the data hold by the container.
      */
    final case class FindDataContainer(elementId: String, data: Any)
        extends DataTreeDocumentMessages

    /**
      * Advise the data tree document to return the sequence row count of the
      * sequence with the given id.
      *
      * @param ref The element reference to the sequence element.
      */
    final case class GetSequenceRowCount(ref: ElementReference) extends DataTreeDocumentMessages

    /**
      * Holds the sequence row counter for a sequence.
      *
      * @param ref  The element reference to the sequence element.
      * @param rows An option to the number of rows which may be `None` if no data was collected.
      */
    final case class SequenceRowCount(ref: ElementReference, rows: Option[Long])
        extends DataTreeDocumentMessages

    /**
      * Tells the data tree document to check if the element with the given id and optional
      * sequence row counter the last data element.
      *
      * @param ref         The element reference to the element.
      * @param sequenceRow An optional sequence row which is used to check the exact element from a sequence.
      */
    final case class IsLastDataElement(ref: ElementReference, sequenceRow: Option[Long] = None)
        extends DataTreeDocumentMessages

    /**
      * The response for an `IsLastDataElement` message.
      * It contains the element id that was queried and a flag indicating if it is the last data element.
      *
      * @param ref           The element reference to the element.
      * @param isLastElement Indicates if the element is the last data element (`true`) or not (`false`).
      * @param sequenceRow   If a sequence row was given and checked then it is returned here.
      */
    final case class IsLastDataElementResponse(ref: ElementReference,
                                               isLastElement: Boolean,
                                               sequenceRow: Option[Long] = None)
        extends DataTreeDocumentMessages

    /**
      * Return the data saved under the given element id.
      * This message is usually relayed to the data tree node which holds and will then return
      * the data to the original sender.
      *
      * @param elementId   The id of the data element.
      * @param sequenceRow An optional sequence row which is used to return the exact element from a sequence.
      */
    final case class ReturnData(elementId: String, sequenceRow: Option[Long] = None)
        extends DataTreeDocumentMessages

    /**
      * Return the data saved within the actor that is mapped to the given hash.
      *
      * @param elementId       The id of the data element.
      * @param elementDataHash The hash identifing the sequence rows to the data element.
      * @param sequenceRow     An optional sequence row which is used to return the exact element from a sequence.
      */
    final case class ReturnHashedData(elementId: String,
                                      elementDataHash: Long,
                                      sequenceRow: Option[Long] = None)
        extends DataTreeDocumentMessages

    /**
      * Make the data tree document return it's xml structure tree.
      */
    case object ReturnXmlStructure extends DataTreeDocumentMessages

    /**
      * Return the hash for the given sequence row.
      *
      * @param sequenceId  The id of the sequence.
      * @param sequenceRow The row.
      */
    final case class ReturnSequenceHash(sequenceId: String, sequenceRow: Long)
        extends DataTreeDocumentMessages

    /**
      * Save the given data into the tree.
      *
      * @param data     The data to save.
      * @param dataHash The data hash to specify the save location.
      */
    final case class SaveData(data: ParserDataContainer, dataHash: Long)
        extends DataTreeDocumentMessages

    /**
      * Save a date element but collect the actual saved data from the given reference id.
      * If the referenced data element lies within a sequence the last saved instance is used.
      *
      * @param data              The data container.
      * @param dataHash          The data hash to specify the save location.
      * @param referenceId       The id of the reference data element(!) from which to take the data.
      * @param sourceSequenceRow An option to the sequence row in which the ref is located.
      */
    final case class SaveReferenceData(data: ParserDataContainer,
                                       dataHash: Long,
                                       referenceId: String,
                                       sourceSequenceRow: Option[Long] = None)
        extends DataTreeDocumentMessages

    /**
      * A container for sending the sequence hash for a specific sequence row.
      *
      * @param sequenceId  The ID of the sequence.
      * @param hash        The hash of the sequence row.
      * @param row         The sequence row.
      */
    final case class SequenceHash(sequenceId: String, hash: Long, row: Long)
        extends DataTreeDocumentMessages

    /**
      * This messages holds the id of the dfasdl and the xml document of the dfasdl.
      *
      * @param dfasdlId The ID of the DFASDL.
      * @param document The XML document of the DFASDL.
      */
    final case class XmlStructure(dfasdlId: String, document: Document)
        extends DataTreeDocumentMessages

  }

  sealed trait DataTreeDocumentState

  object DataTreeDocumentState {

    /**
      * The data tree document has just been initialized an holds no data yet.
      */
    case object Clean extends DataTreeDocumentState

    /**
      * The data tree document has received at least one `SaveData` message.
      */
    case object Working extends DataTreeDocumentState

  }

  /**
    * The state data for the data tree document fsm.
    *
    * FIXME We should check if the map does not copy the actor ref that is mapped to the same keys!
    *
    * @param lastUsedMember        An option to the cluster member that was last used to store data e.g. create a data tree node.
    * @param sequenceDataRows      A map holding the actor refs that store the data mapped to their data hashes that represent their rows.
    * @param sequenceChildren      A helper map that buffers all data element ids mapped to their parent sequence id.
    * @param sequenceHashes        All sequence data hashes mapped to their sequence id. This means we can access the n'th row of the sequence by getting the (n - 1)'th entry from the set.
    * @param sequenceHashesHelpers A helper structure to avoid expensive lookups via `contains` on the sequence hash rows.
    */
  case class DataTreeDocumentStateData(
      lastUsedMember: Option[Member] = None,
      sequenceDataRows: scala.collection.mutable.Map[Long, ActorRef] =
        scala.collection.mutable.Map.empty[Long, ActorRef],
      sequenceChildren: scala.collection.mutable.Map[String, Set[String]] =
        scala.collection.mutable.Map.empty[String, Set[String]],
      sequenceHashes: scala.collection.mutable.Map[String, Vector[(Long, Long)]] =
        scala.collection.mutable.Map.empty[String, Vector[(Long, Long)]],
      sequenceHashesHelpers: scala.collection.mutable.Map[String, Set[Long]] =
        scala.collection.mutable.Map.empty[String, Set[Long]]
  )
}

/**
  * The data tree document is the base for the parse process.
  * It creates actors for the xml nodes that contain the given data.
  * And it constructs an xml tree equal to the data tree that does not hold the data but the
  * actor references to the data nodes (@see DataTreeNode).
  *
  * @param dfasdl              The dfasdl that provides information for the data structure.
  * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
  * @param idWhiteList         A set containing element ids that should be saved. If the set is empty then all(!) elements will be saved!
  */
class DataTreeDocument(dfasdl: DFASDL,
                       agentRunIdentifier: Option[String] = None,
                       idWhiteList: Set[String] = Set.empty[String])
    extends Actor
    with FSM[DataTreeDocumentState, DataTreeDocumentStateData]
    with ActorLogging
    with XmlHelpers
    with DocumentHelpers {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  // Get the list of active cluster members.
  val cluster        = Cluster(context.system)
  val clusterMembers = cluster.state.members.filter(_.status == MemberStatus.Up)

  override def postStop(): Unit = {
    log.clearMDC()
    super.postStop()
  }

  val maxSequenceRowsPerActor =
    context.system.settings.config.getLong("tensei.agents.parser.sequence-rows-per-actor")

  val dfasdlTree = createNormalizedDocument(dfasdl.content)

  startWith(DataTreeDocumentState.Clean, DataTreeDocumentStateData())

  when(DataTreeDocumentState.Clean) {
    // One of our data nodes terminated!
    case Event(Terminated(actor), data) =>
      log.error("One of our data tree nodes terminated at {}!", actor.path)
      // TODO We should notify someone...
      stay() using data
  }

  when(DataTreeDocumentState.Working) {
    // One of our data nodes terminated!
    case Event(Terminated(actor), data) =>
      log.error("One of our data tree nodes terminated at {}!", actor.path)
      // TODO We should notify someone...
      stay() using data

    // Handle `ReturnData` messages.
    case Event(msg: DataTreeDocumentMessages.ReturnData, data) =>
      handleReturnDataEvent(msg.elementId, msg.sequenceRow, data, sender())
      stay() using data

    // Handle `ReturnHashedData` messages.
    case Event(msg: DataTreeDocumentMessages.ReturnHashedData, data) =>
      handleReturnDataEvent(msg.elementId,
                            msg.sequenceRow,
                            data,
                            sender(),
                            Option(msg.elementDataHash))
      stay() using data

    // Handle `ReturnSequenceHash` event.
    case Event(DataTreeDocumentMessages.ReturnSequenceHash(sequenceId, sequenceRow), data) =>
      if (data.sequenceHashes.contains(sequenceId) && data
            .sequenceHashes(sequenceId)
            .size >= sequenceRow) {
        val rowInfo = data.sequenceHashes(sequenceId)(sequenceRow.toInt)
        sender() ! DataTreeDocumentMessages.SequenceHash(sequenceId, rowInfo._1, rowInfo._2)
      } else
        sender() ! StatusMessage(Option(self.path.toSerializationFormat),
                                 s"No hash stored for $sequenceId($sequenceRow)!",
                                 StatusType.FatalError,
                                 None)
      stay() using data
  }

  whenUnhandled {
    case Event(DataTreeDocumentMessages.ReturnXmlStructure, data) =>
      log.debug("Returning xml structure tree.")
      sender() ! DataTreeDocumentMessages.XmlStructure(dfasdlId = dfasdl.id, document = dfasdlTree)
      stay() using data

    case Event(BaseParserMessages.Stop, data) =>
      log.debug("Got STOP message from {}!", sender().path)
      context.children.foreach(c => context.unwatch(c)) // Avoid terminated messages.
      stop()                                            // Terminate...

    // Relay the find data message to the actors that may hold the data.
    case Event(DataTreeDocumentMessages.FindDataContainer(elementId, containerData), data) =>
      val element      = dfasdlTree.getElementById(elementId)
      val reporterPath = self.path.toSerializationFormatWithAddress(self.path.address)
      if (element == null) {
        log.error("Element with id '{}' not found!", elementId)
        sender() ! new StatusMessage(reporter = Option(reporterPath),
                                     message = s"Element with id '$elementId' not found!",
                                     statusType = StatusType.FatalError,
                                     cause = None)
      } else {
        val hasContentMessage =
          DataTreeNodeMessages.HasContent(elementId, containerData, Option(sender()))
        if (element.hasAttribute(AttributeNames.STORAGE_PATH)) {
          // We relay the message to the data tree node that may hold the data.
          val dataTreeNodePath =
            ActorPath.fromString(element.getAttribute(AttributeNames.STORAGE_PATH))
          context.actorSelection(dataTreeNodePath) ! hasContentMessage
        } else {
          // Get the parent sequence id.
          val sequenceData = data.sequenceChildren.find(_._2.contains(elementId))
          if (sequenceData.isDefined) {
            val sequenceId = sequenceData.get._1
            // We need to relay the message to every container that may hold the data.
            data
              .sequenceHashes(sequenceId)
              .map(hash => data.sequenceDataRows(hash._1))
              .toSet[ActorRef]
              .foreach(containerRef => containerRef ! hasContentMessage)
          } else {
            log.error("No data has been stored yet for '{}'!", elementId)
            sender() ! new StatusMessage(
              reporter = Option(reporterPath),
              message = s"No data has been stored yet for '$elementId'!",
              statusType = StatusType.FatalError,
              cause = None
            )
          }
        }
      }
      stay() using data

    // We check if the given element id in consideration of the optional sequence row
    // is actually the last data element.
    case Event(DataTreeDocumentMessages.IsLastDataElement(ref, sequenceRow), data) =>
      val elementId    = ref.elementId
      val element      = dfasdlTree.getElementById(elementId)
      val reporterPath = self.path.toSerializationFormatWithAddress(self.path.address)
      if (element == null) {
        log.error("Element with id '{}' not found!", elementId)
        sender() ! new StatusMessage(reporter = Option(reporterPath),
                                     message = s"Element with id '$elementId' not found!",
                                     statusType = StatusType.FatalError,
                                     cause = None)
      } else {
        val lastOne: Boolean =
          if (element.hasAttribute(AttributeNames.STORAGE_PATH)) {
            // Only simple data elements are stored directly therefore they only exist once.
            // TODO Maybe we could extend this to check if we have a sibling data element in the tree?
            false
          } else {
            val sequenceData = data.sequenceChildren.find(_._2.contains(elementId))
            if (sequenceData.isDefined) {
              val sequenceId = sequenceData.get._1
              data.sequenceHashes.getOrElse(sequenceId, Vector.empty[String]).size > sequenceRow
                .getOrElse(0L)
            } else
              false
          }
        sender() ! DataTreeDocumentMessages.IsLastDataElementResponse(ref, lastOne, sequenceRow)
      }
      stay() using data

    // Return the number of stored sequence rows for the given sequence element id.
    case Event(DataTreeDocumentMessages.GetSequenceRowCount(ref), data) =>
      if (dfasdl.id != ref.dfasdlId) {
        log.error("Got sequence row count request for DFASDL {} but holding {}!",
                  ref.dfasdlId,
                  dfasdl.id)
      } else {
        val sequenceId   = ref.elementId
        val reporterPath = self.path.toSerializationFormatWithAddress(self.path.address)
        if (dfasdlTree.getElementById(sequenceId) == null) {
          log.error("Element with id '{}' not found!", sequenceId)
          sender() ! new StatusMessage(reporter = Option(reporterPath),
                                       message = s"Element with id '$sequenceId' not found!",
                                       statusType = StatusType.FatalError,
                                       cause = None)
        } else {
          if (data.sequenceHashes.contains(sequenceId))
            sender() ! DataTreeDocumentMessages.SequenceRowCount(
              ref,
              Option(data.sequenceHashes(sequenceId).size.toLong)
            )
          else
            sender() ! DataTreeDocumentMessages.SequenceRowCount(ref, None) // No data was saved for the sequence.
        }
      }
      stay() using data

    // If an element with the correct id exists and has no storage path attribute
    // then we simply create a data tree node. Else we either return an error or save
    // the data into the proper sequence position.
    case Event(DataTreeDocumentMessages.SaveData(elementData, elementDataHash), data) =>
      val newData =
        if (idWhiteList.isEmpty || idWhiteList.contains(elementData.elementId)) {
          handleSaveDataEvent(elementData, elementDataHash, data)
        } else {
          log.debug("Ignoring data element '{}' because it is not whitelisted.",
                    elementData.elementId)
          data
        }
      if (stateName == DataTreeDocumentState.Clean)
        goto(DataTreeDocumentState.Working) using newData // Switch to working state if we were in empty state.
      else
        stay() using newData

    // Save data and replace it's value by the referenced data.
    case Event(DataTreeDocumentMessages.SaveReferenceData(elementData,
                                                          elementDataHash,
                                                          referenceId,
                                                          sourceSequenceRow),
               data) =>
      val referenceElement = dfasdlTree.getElementById(referenceId)
      if (referenceElement == null)
        log.error("Reference element with id '{}' not found in DFASDL!", referenceId)
      else {
        val parentSequence = getParentSequence(referenceElement)
        if (parentSequence.isDefined) {
          if (data.sequenceHashes.contains(parentSequence.get.getAttribute("id"))) {
            val rowHash = data.sequenceHashes(parentSequence.get.getAttribute("id")).last._1
            data.sequenceDataRows(rowHash) ! DataTreeNodeMessages.CreateSaveDataForReference(
              elementData.copy(),
              referenceId,
              sourceSequenceRow
            )
          } else {
            log.warning("No data was saved on referenced data element '{}'!", referenceId)
            self ! DataTreeDocumentMessages.SaveData(elementData, elementDataHash)
          }
        } else {
          if (referenceElement.hasAttribute(AttributeNames.STORAGE_PATH)) {
            log.debug("Replacing data for '{}' with saved data from '{}'.",
                      elementData.elementId,
                      referenceId)
            val dataTreeNodePath =
              ActorPath.fromString(referenceElement.getAttribute(AttributeNames.STORAGE_PATH))
            context.actorSelection(dataTreeNodePath) ! DataTreeNodeMessages
              .CreateSaveDataForReference(elementData.copy(), referenceId, sourceSequenceRow)
          } else {
            log.warning("No data was saved on referenced data element '{}'!", referenceId)
            self ! DataTreeDocumentMessages.SaveData(elementData, elementDataHash)
          }
        }
      }
      stay() using data
  }

  initialize()

  /**
    * If the element is present in the structure tree and has a storage path attribute (e.g. stored data)
    * then the data tree node is instructed to return it's content to the sender.
    * Otherwise an error message is returned.
    * If the `providedHash` is defined then it takes priority over all other parameters that may be used
    * to calculate the correct storage actor. This is only true for special elements (in choice or sequence)!
    *
    * @param elementId     The ID of the desired element.
    * @param sequenceRow   An option to a desired sequence row.
    * @param data          The state data to operate on.
    * @param receiver      An actor ref that gets error messages and passed along to the data tree node.
    * @param providedHash  An option to a provided data element hash that can be used to quickly access the correct actor.
    */
  private def handleReturnDataEvent(elementId: String,
                                    sequenceRow: Option[Long],
                                    data: DataTreeDocumentStateData,
                                    receiver: ActorRef,
                                    providedHash: Option[Long] = None): Unit = {
    val element      = dfasdlTree.getElementById(elementId)
    val reporterPath = self.path.toSerializationFormatWithAddress(self.path.address)
    if (element == null) {
      log.error("Element with id '{}' not found!", elementId)
      receiver ! new StatusMessage(reporter = Option(reporterPath),
                                   message = s"Element with id '$elementId' not found!",
                                   statusType = StatusType.FatalError,
                                   cause = None)
    } else {
      if (element.hasAttribute(AttributeNames.STORAGE_PATH)) {
        // We relay the information to the data tree node that holds the data.
        val dataTreeNodePath =
          ActorPath.fromString(element.getAttribute(AttributeNames.STORAGE_PATH))
        context.actorSelection(dataTreeNodePath) ! DataTreeNodeMessages.ReturnContent(
          receiver = Option(receiver)
        )
      } else {
        if (providedHash.isDefined) {
          // Try to use the defined hash.
          if (data.sequenceDataRows.contains(providedHash.get))
            data.sequenceDataRows(providedHash.get) ! DataTreeNodeMessages.ReturnContent(
              receiver = Option(receiver),
              sequenceRow = sequenceRow,
              elementId = Option(elementId)
            )
          else {
            log.error("The hash '{}' does not exist!", providedHash)
            receiver ! new StatusMessage(reporter = Option(reporterPath),
                                         message = s"The hash '$providedHash' does not exist!",
                                         statusType = StatusType.FatalError,
                                         cause = None)
          }
        } else {
          // Get the parent sequence id.
          val sequenceData = data.sequenceChildren.find(_._2.contains(elementId))
          if (sequenceData.isDefined) {
            val sequenceId = sequenceData.get._1
            // Calculate the location of the data tree node by considering how many rows are stored within an actor.
            if (data.sequenceHashes(sequenceId).size > sequenceRow.getOrElse(0L)) {
              // Relay the message to the sequence node and pass the desired sequence row and element id along.
              // FIXME This will not work for sequences with more than `Integer.MAX_VALUE * maxSequenceRowsPerActor` rows because we need to use `toInt` on the desired row!!
              val hash = data.sequenceHashes(sequenceId)(sequenceRow.getOrElse(0L).toInt)
              data.sequenceDataRows(hash._1) ! DataTreeNodeMessages.ReturnContent(
                receiver = Option(receiver),
                sequenceRow = Option(sequenceRow.getOrElse(0L)),
                elementId = Option(elementId)
              )
            } else {
              log.error("The number of sequence rows ({}) is smaller than {}!",
                        data.sequenceHashes(sequenceId).size,
                        sequenceRow)
              receiver ! new StatusMessage(
                reporter = Option(reporterPath),
                message =
                  s"The number of sequence rows (${data.sequenceHashes(sequenceId).size}) is smaller than $sequenceRow!",
                statusType = StatusType.FatalError,
                cause = None
              )
            }
          } else {
            log.error("No data has been stored for the in sequence element '{}'!", elementId)
            receiver ! new StatusMessage(
              reporter = Option(reporterPath),
              message = s"No data has been stored for the in sequence element '$elementId'!",
              statusType = StatusType.FatalError,
              cause = None
            )
          }
        }
      }
    }
  }

  /**
    * Handle a `SaveData` event and return the updated state data.
    *
    * @param elementData     The element data to save.
    * @param elementDataHash The hash that is used to specify the storage location.
    * @param data            The current state data of the data tree document.
    * @return The updated state data.
    */
  private def handleSaveDataEvent(elementData: ParserDataContainer,
                                  elementDataHash: Long,
                                  data: DataTreeDocumentStateData): DataTreeDocumentStateData = {
    val containerElement = dfasdlTree.getElementById(elementData.elementId)
    val reporterPath     = self.path.toSerializationFormatWithAddress(self.path.address)
    if (containerElement == null) {
      sender() ! new StatusMessage(reporter = Option(reporterPath),
                                   message =
                                     s"Element with id '${elementData.elementId}' not found!",
                                   statusType = StatusType.FatalError,
                                   cause = None)
      data
    } else {
      // First we need to check if the element is within a sequence.
      val parentSequence = getParentSequence(containerElement)
      if (parentSequence.isEmpty && containerElement.hasAttribute(AttributeNames.STORAGE_PATH)) {
        // We have no parent sequence and the element has already been saved.
        val message =
          s"Element '${elementData.elementId}' has already a storage path! Content would be overriden!"
        log.error(message)
        sender() ! new StatusMessage(reporter = Option(reporterPath),
                                     message = message,
                                     statusType = StatusType.FatalError,
                                     cause = None)
        data
      } else {
        if (parentSequence.isEmpty) {
          // Get the next cluster member where we want to store data and create a storage node.
          val nodeAndMember: (ActorRef, Option[Member]) =
            GenericHelpers
              .roundRobinFromSortedSet[Member](clusterMembers, data.lastUsedMember) match {
              case -\/(failure) =>
                (context.actorOf(DataTreeNode.props(agentRunIdentifier)), None)
              case \/-(member) =>
                val deploy = new Deploy(new RemoteScope(member.address))
                (context.actorOf(DataTreeNode.props(agentRunIdentifier).withDeploy(deploy)),
                 Option(member))
            }
          val node = nodeAndMember._1
          // We watch the node to be notified if it dies.
          context watch node
          // Simply save the data.
          node ! DataTreeNodeMessages.AppendData(elementData.copy())
          // Now we need to save the actor path.
          containerElement.setAttribute(AttributeNames.STORAGE_PATH,
                                        node.path.toSerializationFormat)
          data.copy(lastUsedMember = nodeAndMember._2)
        } else {
          // We are within a sequence therefore we need to buffer the actor ref
          // to be able to collect all sequence data later on and update the state data.
          val sequenceId = parentSequence.get.getAttribute("id")
          val storedSequenceRows =
            if (data.sequenceHashes.contains(sequenceId))
              data.sequenceHashes(sequenceId).size
            else
              0
          val maxSequenceRowsPerActorTrigger = storedSequenceRows > 0 && storedSequenceRows % maxSequenceRowsPerActor > 0

          // Calculate and get the correct storage actor ref.
          val containerAndMember: (ActorRef, Option[Member]) =
            if (maxSequenceRowsPerActorTrigger || data.sequenceDataRows.contains(elementDataHash)) {
              // Get the container.
              if (data.sequenceDataRows.contains(elementDataHash))
                (data.sequenceDataRows(elementDataHash), None)
              else {
                val previousHash = data.sequenceHashes(sequenceId).last
                (data.sequenceDataRows(previousHash._1), None)
              }
            } else {
              // Get the next cluster member where we want to store data.
              GenericHelpers
                .roundRobinFromSortedSet[Member](clusterMembers, data.lastUsedMember) match {
                case -\/(failure) =>
                  val node = context.actorOf(DataTreeNode.props(agentRunIdentifier))
                  // We watch the node to be notified if it dies.
                  context watch node
                  (node, None)
                case \/-(member) =>
                  val deploy = new Deploy(new RemoteScope(member.address))
                  val node =
                    context.actorOf(DataTreeNode.props(agentRunIdentifier).withDeploy(deploy))
                  // We watch the node to be notified if it dies.
                  context watch node
                  (node, Option(member))
              }
            }
          val containerRef: ActorRef = containerAndMember._1
          // Save the data.
          containerRef ! DataTreeNodeMessages.AppendData(elementData.copy())
          // Update the sequence hashes and data rows.
          data.sequenceDataRows += (elementDataHash -> containerRef)
          val sequenceHashHelpers =
            data.sequenceHashesHelpers.getOrElse(sequenceId, Set.empty[Long])
          if (!sequenceHashHelpers.contains(elementDataHash)) {
            data.sequenceHashesHelpers(sequenceId) = sequenceHashHelpers + elementDataHash
            data.sequenceHashes += (sequenceId -> (data.sequenceHashes.getOrElse(
              sequenceId,
              Vector.empty[(Long, Long)]
            ) :+ ((elementDataHash, elementData.sequenceRowCounter))))
          }

          if (elementData.sequenceRowCounter > 0 && elementData.sequenceRowCounter % 10000 == 0) {
            log.debug("Saved {} rows for element '{}' in sequence '{}'.",
                      elementData.sequenceRowCounter,
                      elementData.elementId,
                      sequenceId)
          }

          val newChildren = data.sequenceChildren
            .getOrElse(sequenceId, Set.empty[String]) + elementData.elementId
          data.sequenceChildren += (sequenceId -> newChildren)
          if (containerAndMember._2.isDefined)
            data.copy(lastUsedMember = containerAndMember._2)
          else
            data
        }
      }
    }
  }
}
