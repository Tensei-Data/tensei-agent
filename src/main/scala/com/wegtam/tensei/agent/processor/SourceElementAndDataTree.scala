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
import org.w3c.dom.Element

/**
  * A wrapper class that holds a data element description and the actor
  * ref of the data tree document that holds the actual data.
  *
  * @param element A dfasdl data element description.
  * @param dataTreeRef An actor ref of a [[com.wegtam.tensei.agent.DataTreeDocument]].
  */
case class SourceElementAndDataTree(
    element: Element,
    dataTreeRef: ActorRef
)
