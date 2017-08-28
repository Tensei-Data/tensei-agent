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

import com.wegtam.tensei.adt.ElementReference

/**
  * A trait for functions that are needed accross processor workers.
  */
trait WorkerHelpers {

  /**
    * Search the given list of source data trees for the element with the given id.
    *
    * @param elementId The id of an element.
    * @param dataTrees A list of source data tree entries.
    * @return An option to a tuple holding the first found element and the appropriate data tree actor ref.
    */
  def findElementAndDataTreeActorRef(
      elementId: String,
      dataTrees: List[SourceDataTreeListEntry]
  ): Option[SourceElementAndDataTree] =
    dataTrees
      .filter(_.document.isDefined)
      .find(_.document.get.getElementById(elementId) != null)
      .map(
        f =>
          Option(
            SourceElementAndDataTree(element = f.document.get.getElementById(elementId),
                                     dataTreeRef = f.actorRef)
        )
      )
      .getOrElse(None)

  /**
    * Search the given list of source data trees for the element with the given id.
    *
    * @param elementRef A reference to an element.
    * @param dataTrees A list of source data tree entries.
    * @return An option to a tuple holding the first found element and the appropriate data tree actor ref.
    */
  def findElementAndDataTreeActorRef(
      elementRef: ElementReference,
      dataTrees: List[SourceDataTreeListEntry]
  ): Option[SourceElementAndDataTree] =
    dataTrees
      .filter(d => d.dfasdlId == elementRef.dfasdlId && d.document.isDefined)
      .find(_.document.get.getElementById(elementRef.elementId) != null)
      .map(
        f =>
          Option(
            SourceElementAndDataTree(element = f.document.get.getElementById(elementRef.elementId),
                                     dataTreeRef = f.actorRef)
        )
      )
      .getOrElse(None)

}
