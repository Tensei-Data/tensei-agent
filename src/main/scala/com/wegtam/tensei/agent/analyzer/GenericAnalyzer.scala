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

package com.wegtam.tensei.agent.analyzer

import akka.actor.{ Actor, ActorLogging }
import com.wegtam.tensei.agent.adt.ParserDataContainer
import org.dfasdl.utils.DataElementProcessors
import org.w3c.dom.Element

object GenericAnalyzer {
  sealed trait GenericAnalyzerMessages

  object NumericAnalyzerMessages {

    /**
      * A message with the data that should be analyzed by the analyzer.
      *
      * @param data  The complete container with additional information.
      */
    case class AnalyzeData(data: ParserDataContainer) extends GenericAnalyzerMessages
  }
}

/**
  * A generic analyzer that represents the base class of all specific analyzers.
  *
  * @param elementId The ID of the element that should be analyzed by the analyzer.
  * @param element   The element of the DFASDL that represents the container for the data.
  * @param percent   The amount of data that should be analyzed from the total amount of elements.
  */
abstract case class GenericAnalyzer(elementId: String, element: Element, percent: Int = 100)
    extends Actor
    with ActorLogging
    with DataElementProcessors
