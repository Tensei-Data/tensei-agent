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
import com.wegtam.tensei.adt.DFASDL
import com.wegtam.tensei.agent.DataTreeDocumentsManager.DataTreeDocumentsManagerMessages
import com.wegtam.tensei.agent.helpers.{ GenericHelpers, LoggingHelpers }

import scalaz._

object DataTreeDocumentsManager {
  val name = "DataTreeDocumentsManager"

  /**
    * Helper method for creating actors for the data tree documents manager.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @return The props to generate the actor.
    */
  def props(agentRunIdentifier: Option[String]): Props =
    Props(new DataTreeDocumentsManager(agentRunIdentifier))

  sealed trait DataTreeDocumentsManagerMessages

  object DataTreeDocumentsManagerMessages {

    /**
      * Create a `DataTreeDocument` actor based upon the given dfasdl.
      *
      * @param dfasdl The dfasdl describing the data structure.
      */
    final case class CreateDataTreeDocument(dfasdl: DFASDL) extends DataTreeDocumentsManagerMessages

    /**
      * Create a `DataTreeDocument` actor based upon the given dfasdl using the provided id white list.
      *
      * @param dFASDL      The dfasdl describing the data structure.
      * @param idWhiteList A set of strings holding the ids that are whitelisted for storage.
      */
    final case class CreateDataTreeDocumentWithWhiteList(dFASDL: DFASDL, idWhiteList: Set[String])
        extends DataTreeDocumentsManagerMessages

    /**
      * Reports the creating of a `DataTreeDocument` actor.
      *
      * @param dataTreeRef The ref to the created actor.
      * @param dfasdl      The dfasdl that was used to create the actor.
      */
    final case class DataTreeDocumentCreated(dataTreeRef: ActorRef, dfasdl: DFASDL)
        extends DataTreeDocumentsManagerMessages

  }
}

/**
  * An actor that creates `DataTreeDocument` actors upon request.
  * The created data tree document actor ref is wrapped with the dfasdl id into a message and returned to the sender.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class DataTreeDocumentsManager(agentRunIdentifier: Option[String]) extends Actor with ActorLogging {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  // Get the list of active cluster members.
  val cluster        = Cluster(context.system)
  val clusterMembers = cluster.state.members.filter(_.status == MemberStatus.Up)
  // We need to buffer the last used member.
  var lastUsedMember: Option[Member] = None

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.clearMDC()
    super.postStop()
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override def receive: Receive = {
    case DataTreeDocumentsManagerMessages.CreateDataTreeDocument(dfasdl) =>
      log.debug("Got request to create an data tree document for dfasdl '{}'.", dfasdl.id)
      val dataTreeDocument =
        GenericHelpers.roundRobinFromSortedSet[Member](clusterMembers, lastUsedMember) match {
          case -\/(failure) =>
            context.actorOf(DataTreeDocument.props(dfasdl, agentRunIdentifier, Set.empty[String]))
          case \/-(member) =>
            val deploy = new Deploy(new RemoteScope(member.address))
            lastUsedMember = Option(member)
            context.actorOf(
              DataTreeDocument
                .props(dfasdl, agentRunIdentifier, Set.empty[String])
                .withDeploy(deploy)
            )
        }
      sender() ! DataTreeDocumentsManagerMessages.DataTreeDocumentCreated(dataTreeDocument, dfasdl)

    case DataTreeDocumentsManagerMessages
          .CreateDataTreeDocumentWithWhiteList(dfasdl, idWhiteList) =>
      log.debug(
        "Got request to create an data tree document for dfasdl '{}' using a white list with {} entries.",
        dfasdl.id,
        idWhiteList.size
      )
      val dataTreeDocument =
        GenericHelpers.roundRobinFromSortedSet[Member](clusterMembers, lastUsedMember) match {
          case -\/(failure) =>
            context.actorOf(DataTreeDocument.props(dfasdl, agentRunIdentifier, idWhiteList))
          case \/-(member) =>
            val deploy = new Deploy(new RemoteScope(member.address))
            lastUsedMember = Option(member)
            context.actorOf(
              DataTreeDocument.props(dfasdl, agentRunIdentifier, idWhiteList).withDeploy(deploy)
            )
        }
      sender() ! DataTreeDocumentsManagerMessages.DataTreeDocumentCreated(dataTreeDocument, dfasdl)
  }
}
