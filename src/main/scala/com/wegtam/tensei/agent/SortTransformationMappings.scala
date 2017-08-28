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

import akka.actor.{ Actor, ActorLogging, Props }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.SortTransformationMappings.SortTransformationMappingsMessages
import com.wegtam.tensei.agent.helpers.XmlHelpers
import org.dfasdl.utils.{ AttributeNames, DocumentHelpers }
import org.w3c.dom.Document

import scala.collection.mutable.ListBuffer

import scalaz._

object SortTransformationMappings {
  def props: Props = Props(classOf[SortTransformationMappings])

  val name = "Sorter"

  sealed trait SortTransformationMappingsMessages

  object SortTransformationMappingsMessages {
    case class SortMappings(cookbook: Cookbook) extends SortTransformationMappingsMessages

    case class SortedMappings(cookbook: Cookbook) extends SortTransformationMappingsMessages
  }
}

/**
  * This trait holds the functionality for the sort transformation mappings actor to
  * ease testing.
  */
trait SortTransformationMappingsFunctions extends XmlHelpers with DocumentHelpers {

  /**
    * Finds the given list of element references (needles) in the provided list of
    * element references (haystack) and returns the smallest index.
    *
    * @param haystack A list of element references.
    * @param needles A list of element references to search for within the haystack.
    * @return The smallest index of the found element references.
    */
  def findFirstId(haystack: Vector[ElementReference], needles: List[ElementReference]): Long = {
    val hits = ListBuffer[(ElementReference, Long)]()
    needles.foreach(needle => hits += ((needle, haystack.indexOf(needle).toLong)))
    hits.sortWith((a, b) => a._2 < b._2).head._2
  }

  /**
    * Get a sorted list of target IDs of the target dfasdl and sort the List[MappingTransformation]
    * of the recipe. If no target DFASDL is defined then the passed cookbook is returned.
    *
    * @param cookbook The cookbook that contains the recipes and mapping transformations that should be sorted.
    * @return A cookbook containing the sorted content.
    */
  def sortRecipes(cookbook: Cookbook): Cookbook =
    // If no target DFASDL is defined we simply return the cookbook as it is.
    cookbook.target
      .map { target =>
        val sortedTargetElements = getSortedIdList(cookbook.target.get.content)
          .map(id => ElementReference(dfasdlId = cookbook.target.get.id, elementId = id))
        if (sortedTargetElements.isEmpty)
          cookbook // Return the cookbook because we have no target ids that we could use for sorting.
        else {
          // Sort the list of recipes in the cookbook.
          val sortedRecipes = cookbook.recipes
            .map(r => sortMappings(r)(sortedTargetElements))
            .sortWith(
              (a, b) =>
                findFirstId(sortedTargetElements, a.mappings.head.targets) < findFirstId(
                  sortedTargetElements,
                  b.mappings.head.targets
              )
            )
          // Sort the list of mapping transformations within the list of sorted recipes.
          val sortedRecipesWithSortedMappings = sortedRecipes.map(
            recipe =>
              recipe.mode match {
                case Recipe.MapAllToAll =>
                  val ms =
                    recipe.mappings.map(m => sortAllToAllMappingPairs(m)(sortedTargetElements))
                  recipe.copy(mappings = ms)
                case Recipe.MapOneToOne =>
                  val ms =
                    recipe.mappings.map(m => sortOneToOneMappingPairs(m)(sortedTargetElements))
                  recipe.copy(mappings = ms)
            }
          )
          val rs = sortRecipesByForeignKeys(sortedRecipesWithSortedMappings)(
            createNormalizedDocument(target.content)
          )
          // Return the cookbook with the sorted values.
          cookbook.copy(recipes = rs)
        }
      }
      .getOrElse(cookbook)

  /**
    * This function sorts the given pre-sorted list of recipes by the occurences of foreign keys.
    *
    * @param rs A list of recipes that is already sorted by our other matching criteria.
    * @param t The DFASDL document tree of the target DFASDL.
    * @return A list of sorted recipes.
    */
  def sortRecipesByForeignKeys(rs: List[Recipe])(t: Document): List[Recipe] = {
    val sortedRecipes = new ListBuffer[Recipe]
    sortedRecipes ++= rs

    val foreignKeyElements = getSortedIdList(t)
      .map(
        id =>
          if (t.getElementById(id).hasAttribute(AttributeNames.DB_FOREIGN_KEY))
            Option(t.getElementById(id))
          else None
      )
      .filter(_.isDefined)
      .map(_.get)
    foreignKeyElements.foreach { e =>
      val id   = e.getAttribute("id")
      val fIds = e.getAttribute(AttributeNames.DB_FOREIGN_KEY).split(",").map(_.trim)
      // Find all recipes that include the target id referenced by the db-foreign-key attribute.
      val fRecipes = fIds
        .flatMap(fid => rs.filter(r => r.mappings.exists(_.targets.exists(_.elementId == fid))))
        .toSet
      // Find all recipes that include the current element id.
      rs.filter(r => r.mappings.exists(_.targets.exists(_.elementId == id)))
        .foreach(
          r =>
            fRecipes.foreach(
              fr =>
                if (sortedRecipes.indexOf(fr) > sortedRecipes.indexOf(r)) {
                  if (sortedRecipes.head == r) {
                    // Special case: If the right element is the head of the list we just need to prepend the left to the cleaned up list.
                    sortedRecipes.remove(sortedRecipes.indexOf(fr))
                    sortedRecipes.prepend(fr)
                  } else {
                    // Split the list at the index of the right element.
                    val s     = sortedRecipes.splitAt(sortedRecipes.indexOf(r))
                    val left  = s._1
                    val right = s._2
                    // Remove the left element.
                    right.remove(right.indexOf(fr))
                    // Clear our buffer and construct the new sorted list.
                    sortedRecipes.clear()
                    sortedRecipes ++= left
                    sortedRecipes += fr
                    sortedRecipes ++= right
                  }
              }
          )
        )
    }
    sortedRecipes.toList
  }

  /**
    * Sort the list of mappings within a recipe.
    *
    * @param r A recipe.
    * @param sortedTargetElements A sorted list of target elements.
    * @return A recipe with sorted list of mappings.
    */
  def sortMappings(r: Recipe)(sortedTargetElements: Vector[ElementReference]): Recipe =
    if (sortedTargetElements.isEmpty)
      r
    else {
      val sortedMappings = new ListBuffer[MappingTransformation]

      sortedTargetElements foreach { e =>
        val ms = r.mappings diff sortedMappings
        sortedMappings ++= ms.filter(_.targets.contains(e))
      }

      r.copy(mappings = sortedMappings.toList)
    }

  /**
    * Sort the mapping pairs (e.g. the source and target ids) within the given mapping transformation
    * according to the given order of target elements for all to all mappings.
    * <br/><br/>
    * '''Note:''' For an all to all mapping this simply means the target elements will be sorted.
    *
    * @param m A mapping transformation.
    * @param sortedTargetElements A sorted list of target elements.
    * @return A mapping transformation with sorted mapping pairs.
    */
  def sortAllToAllMappingPairs(
      m: MappingTransformation
  )(sortedTargetElements: Vector[ElementReference]): MappingTransformation = {
    val sortedTargets = m.targets.sortWith(
      (a, b) => sortedTargetElements.indexOf(a) < sortedTargetElements.indexOf(b)
    )
    m.copy(targets = sortedTargets)
  }

  /**
    * Sort the mapping pairs (e.g. the source and target ids) within the given mapping transformation
    * according to the given order of target elements.
    * <br/><br/>
    * '''Note:''' This function only works for 1:1 mappings meaning they must have the same number of source and target elements!
    *
    * @param m A mapping transformation.
    * @param sortedTargetElements A sorted list of target elements.
    * @return A mapping transformation with sorted mapping pairs.
    */
  def sortOneToOneMappingPairs(
      m: MappingTransformation
  )(sortedTargetElements: Vector[ElementReference]): MappingTransformation = {
    val pairs = m.sources zip m.targets
    val sortedPairs = pairs.sortWith(
      (a, b) => sortedTargetElements.indexOf(a._2) < sortedTargetElements.indexOf(b._2)
    )
    val sources = Vector.newBuilder[ElementReference]
    val targets = Vector.newBuilder[ElementReference]

    sortedPairs.foreach { p =>
      sources += p._1
      targets += p._2
    }

    m.copy(sources = sources.result().toList, targets = targets.result().toList)
  }

}

/**
  * This actor receives a `SortMappings` message and sorts the list of mapping transformations within
  * the `Recipe` to make sure that they are in the correct order to be processed.
  * After the sorting is done it sends a `SortedMappings` message to the sender containing the
  * sorted data.
  */
class SortTransformationMappings
    extends Actor
    with ActorLogging
    with SortTransformationMappingsFunctions {

  override def receive: Receive = {
    case SortTransformationMappingsMessages.SortMappings(cookbook) =>
      log.debug("Sorting transformation mappings.")
      \/.fromTryCatch(sortRecipes(cookbook)) match {
        case -\/(failure) =>
          log.error(failure, "An error occurred while trying to sort the transformation mappings!")
          sender() ! GlobalMessages.ErrorOccured(
            StatusMessage(
              reporter = Option(self.path.toString),
              message = "An error occurred while trying to sort the transformation mappings!",
              statusType = StatusType.FatalError,
              cause = None
            )
          )
        case \/-(sortedCookbook) =>
          sender() ! SortTransformationMappingsMessages.SortedMappings(cookbook = sortedCookbook)
      }
  }

}
