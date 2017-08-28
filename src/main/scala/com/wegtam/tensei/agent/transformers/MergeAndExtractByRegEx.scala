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

package com.wegtam.tensei.agent.transformers

import akka.actor.Props
import akka.util.ByteString
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}

import scala.util.Try
import scala.util.matching.Regex

object MergeAndExtractByRegEx {
  def props: Props = Props(new MergeAndExtractByRegEx())
}

/**
  * This transformer returns the result of the regular expression or an empty string.
  *
  * The transformer accepts the following parameters:
  * - `regexp`    The pattern that is used as regular expression. If empty, the given string will be returned.
  * - `filler`    A string that represents the fill element between the groups. (Default "" (empty))
  * - `groups`    Return the given groups (Comma separated list of groups starting with 0)
  *               If no groups are given, all matched groups are returned.
  */
class MergeAndExtractByRegEx extends BaseTransformer {
  override def transform: Receive = {
    case msg: StartTransformation =>
      log.debug("Starting regexp match of the data!")

      val params = msg.options.params
      val regexp =
        if (params.exists(p => p._1 == "regexp"))
          params.find(p => p._1 == "regexp").get._2.asInstanceOf[String]
        else
          ""
      val filler: String =
        if (params.exists(p => p._1 == "filler") && params
              .find(p => p._1 == "filler")
              .get
              ._2
              .nonEmpty)
          params.find(p => p._1 == "filler").get._2.asInstanceOf[String]
        else
          ""
      val groups: List[Int] =
        if (params.exists(p => p._1 == "groups") && params
              .find(p => p._1 == "groups")
              .get
              ._2
              .nonEmpty)
          params
            .find(p => p._1 == "groups")
            .get
            ._2
            .asInstanceOf[String]
            .split(",")
            .map(_.trim.toInt)
            .toList
        else
          List.empty

      val p: Regex = s"""$regexp""".r
      val matchedParts: List[ByteString] = msg.src.flatMap { e =>
        val part: String = e match {
          case bd: java.math.BigDecimal => bd.toPlainString
          case bs: ByteString           => bs.utf8String
          case None                     => ""
          case otherType                => otherType.toString
        }
        val ms = p.findAllIn(part).matchData.toList
        log.debug("Found {} matches in {}.", ms.size, part) // DEBUG
        if (regexp.isEmpty)
          List(ByteString(part))
        else
          ms.map(
            md =>
              if (groups.nonEmpty)
                ByteString(groups.flatMap(g => Try(md.group(g + 1)).toOption).mkString(filler))
              else
                ByteString(md.subgroups.mkString(filler))
          )
      }
      val response = if (matchedParts.isEmpty) List(ByteString("")) else matchedParts

      context become receive
      sender() ! TransformerResponse(response, classOf[String])
  }
}
