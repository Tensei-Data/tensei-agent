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

import akka.actor.ActorRef
import com.wegtam.tensei.adt.DFASDL
import org.w3c.dom.Document
import org.w3c.dom.traversal.TreeWalker

/**
  * A sealed trait for the possible states of a mapper actor.
  */
sealed trait MapperState

/**
  * A companion object for the trait to keep the namespace clean.
  */
object MapperState {

  /**
    * The actor has not been initialised with the data needed to
    * process messages correctly.
    */
  case object Empty extends MapperState

  /**
    * The actor has been correctly initialised and is ready to
    * process messages.
    */
  case object Ready extends MapperState

}

/**
  * The state data of a mapping worker.
  *
  * @param fetcher An option to the actor ref of the data fetcher.
  * @param sourceDataTrees A list of source data trees paired with their dfasdl.
  * @param targetDfasdl An option to the target DFASDL.
  * @param targetTree The xml document tree of the target dfasdl.
  * @param targetTreeWalker A tree walker used to traverse the target dfasdl tree.
  * @param writer An option to the actor ref of the data writer actor.
  */
case class MapperStateData(
    fetcher: Option[ActorRef] = None,
    sourceDataTrees: List[SourceDataTreeListEntry] = List.empty[SourceDataTreeListEntry],
    targetDfasdl: Option[DFASDL] = None,
    targetTree: Option[Document] = None,
    targetTreeWalker: Option[TreeWalker] = None,
    writer: Option[ActorRef] = None
)
