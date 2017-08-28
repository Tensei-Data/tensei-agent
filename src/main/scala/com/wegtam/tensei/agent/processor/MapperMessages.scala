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
import com.wegtam.tensei.adt.{ DFASDL, MappingTransformation, Recipe }
import org.w3c.dom.traversal.TreeWalker

/**
  * A sealed trait for the messages a mapper actor can send and receive.
  */
sealed trait MapperMessages

/**
  * A companion object for the trait to keep the namespace clean.
  */
object MapperMessages {

  /**
    * Initialise the mapper worker with some basic state data that is needed
    * for each mapping.
    *
    * @param fetcher The actor ref of the data fetcher.
    * @param sourceDataTrees A list of source data trees paired with their dfasdl.
    * @param targetDfasdl The target DFASDL.
    * @param targetTreeWalker A tree walker used to traverse the target dfasdl tree.
    * @param writer An actor ref of the data writer actor.
    */
  case class Initialise(
      fetcher: ActorRef,
      sourceDataTrees: List[SourceDataTreeListEntry],
      targetDfasdl: DFASDL,
      targetTreeWalker: TreeWalker,
      writer: ActorRef
  ) extends MapperMessages

  /**
    * Reports that a mapping has been processed successfully.
    *
    * @param lastWriterMessageNumber The number of the last writer message that was sent out.
    */
  case class MappingProcessed(
      lastWriterMessageNumber: Long
  ) extends MapperMessages

  /**
    * Instruct the actor to process the given mapping obeying the given parameters.
    *
    * @param mapping The mapping that should be processed.
    * @param lastWriterMessageNumber The number of the last writer message that was sent out.
    * @param maxLoops The number of times the recipe will be executed at most. This value passed down from the [[RecipeWorker]].
    * @param recipeMode The mode of the parent recipe which triggers different processing/mapping modes.
    * @param sequenceRow An option to the currently processed sequence row.
    */
  case class ProcessMapping(
      mapping: MappingTransformation,
      lastWriterMessageNumber: Long,
      maxLoops: Long,
      recipeMode: Recipe.RecipeMode,
      sequenceRow: Option[Long] = None
  ) extends MapperMessages

  /**
    * Process the next element pair in the currently processed mapping.
    */
  case object ProcessNextPair extends MapperMessages

  /**
    * This messages indicates that the actor is ready to process a mapping.
    */
  case object Ready extends MapperMessages

  /**
    * Instruct the actor to stop itself.
    */
  case object Stop extends MapperMessages

}
