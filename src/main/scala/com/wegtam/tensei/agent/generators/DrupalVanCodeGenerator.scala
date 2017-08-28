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

package com.wegtam.tensei.agent.generators

import akka.actor.Props
import akka.util.ByteString
import com.wegtam.tensei.agent.generators.BaseGenerator.BaseGeneratorMessages.{
  GeneratorResponse,
  PrepareToGenerate,
  ReadyToGenerate,
  StartGenerator
}

import scala.annotation.tailrec
import scala.collection.mutable

object DrupalVanCodeGenerator {
  val name = "DrupalVanCodeGenerator"

  def props: Props = Props(classOf[DrupalVanCodeGenerator])
}

/**
  * A generator which creates Drupals vancode out of commentID, articleID and parent commentID.
  * Example:
  * commentID = 1, articleID = 1, parent = 0 -> vancode = 01/
  * commentID = 2, articleID = 1, parent = 1 -> vancode = 01.00/
  * commentID = 3, articleID = 2, parent = 0 -> vancode = 01/
  *
  * The generator accepts the following parameters:
  * - article (required)   - The article ID of the related article
  * - commentid (required) - The origin comment ID.
  * - parent               - The comment ID of the parent. Default value is 0.
  */
class DrupalVanCodeGenerator extends BaseGenerator {
  val ids = mutable.HashMap[Long, (Long, Long, Long)]() //id -> (article,parent,vancodeid)

  override def receive: Receive = {
    case PrepareToGenerate =>
      context become generate
      sender() ! ReadyToGenerate
  }

  override def generate: Receive = {
    case msg: StartGenerator => //Nachricht vom Transformator
      log.debug("Generating new vancode")

      val params = msg.data

      val article = if (params.exists(p => p.asInstanceOf[(String, String)]._1 == "article")) {
        params
          .find(p => p.asInstanceOf[(String, String)]._1 == "article")
          .get
          .asInstanceOf[(String, String)]
          ._2
          .toLong
      } else {
        log.error("Missing field name in {}", params.mkString(", "))
        throw new NoSuchElementException("Vancode transformer couldn't find the article field.")
      }

      val id = if (params.exists(p => p.asInstanceOf[(String, String)]._1 == "commentid")) {
        params
          .find(p => p.asInstanceOf[(String, String)]._1 == "commentid")
          .get
          .asInstanceOf[(String, String)]
          ._2
          .toLong
      } else {
        log.error("Missing field name in {}", params.mkString(", "))
        throw new NoSuchElementException("Vancode transformer couldn't find the commentid field.")
      }

      val parent = if (params.exists(p => p.asInstanceOf[(String, String)]._1 == "parent")) {
        params
          .find(p => p.asInstanceOf[(String, String)]._1 == "parent")
          .get
          .asInstanceOf[(String, String)]
          ._2
          .toLong
      } else {
        0
      }

      val max = if (ids.nonEmpty) {
        ids
          .map(
            e =>
              if (e._2._1 == article && e._2._2 == parent) {
                e._2._3
              } else if (parent != 0) {
                -1L
              } else {
                0L
            }
          )
          .max
      } else {
        0
      }

      val newid = max + 1
      ids.put(id, (article, parent, newid))
      val vancode = ByteString(s"${getparentcode(parent)("")}${toBase36(newid.toInt)}/")
      sender() ! GeneratorResponse(List(vancode))
  }

  /**
    * look for the vancodes of all parents and creates the new vancode
    *
    * @param parent the comment ID of the parent comment
    * @return the new vancode
    */
  @tailrec
  private def getparentcode(parent: Long)(acc: String): String =
    if (parent == 0)
      acc
    else {
      val (_, parentId, vancodeId) = ids(parent)
      getparentcode(parentId)(s"$acc${toBase36(vancodeId.toInt)}.")
    }

  /**
    * Converts a base 10 number to a base 36 number
    *
    * @param number a base 10 number
    * @return a base 36 number
    */
  private def toBase36(number: Int): String = {
    val b36 = Integer.toString(number, 36)
    s"${b36.length - 1}$b36"
  }

}
