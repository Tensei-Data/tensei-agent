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
import org.w3c.dom.Document

/**
  * A wrapper for data tree entries.
  *
  * @param dfasdlId The ID of the dfasdl document.
  * @param document An option to the dfasdl document that describes the data.
  */
final case class DataTreeListEntry(
    dfasdlId: String,
    document: Option[Document]
) {
  require(dfasdlId != null, "The DFASDL ID must not be null!")
  require(dfasdlId.length > 0, "The DFASDL ID must not be empty!")
}

/**
  * A simple case class for data tree lists.
  *
  * @param dfasdlId The ID of the dfasdl document.
  * @param document An option to the dfasdl document that describes the stored data.
  * @param actorRef An actor ref to the [[com.wegtam.tensei.agent.DataTreeDocument]] that holds the data.
  */
final case class SourceDataTreeListEntry(
    dfasdlId: String,
    document: Option[Document],
    actorRef: ActorRef
) {
  require(dfasdlId != null, "The DFASDL ID must not be null!")
  require(dfasdlId.length > 0, "The DFASDL ID must not be empty!")

  /**
    * Create an instance of a [[DataTreeListEntry]] class from this one.
    *
    * @return A data tree list entry with the appropriate attributes.
    */
  def toDataTreeListEntry: DataTreeListEntry =
    DataTreeListEntry(dfasdlId = dfasdlId, document = document)

}
