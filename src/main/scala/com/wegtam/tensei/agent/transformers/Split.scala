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

import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}
import akka.actor.Props
import akka.util.ByteString

import scala.util.Try

object Split {
  def props: Props = Props(classOf[Split])
}

/**
  * A simple transformer that splits a source.
  *
  * The transformer accepts the following parameters:
  * - `pattern`    The pattern that is used to split the element.
  * - `limit`      Return only the first x elements of the split array. (Default -1 for all)
  * - `selected`   Return only the elements at the given positions. (Comma separated list of Int, starting with 0)
  */
class Split extends BaseTransformer {
  override def transform: Receive = {
    case msg: StartTransformation =>
      log.debug("Starting splitting of source: {}", msg.src)

      val params = msg.options.params
      val pattern =
        if (params.exists(p => p._1 == "pattern"))
          params.find(p => p._1 == "pattern").get._2.asInstanceOf[String]
        else
          ""
      val limit: Int =
        if (params.exists(p => p._1 == "limit") && params
              .find(p => p._1 == "limit")
              .get
              ._2
              .nonEmpty)
          params.find(p => p._1 == "limit").get._2.asInstanceOf[String].toInt
        else
          -1
      val selected: Seq[Int] =
        if (params.exists(p => p._1 == "selected") && params
              .find(p => p._1 == "selected")
              .get
              ._2
              .nonEmpty)
          params
            .find(p => p._1 == "selected")
            .get
            ._2
            .asInstanceOf[String]
            .split(",")
            .map(_.trim.toInt)
        else
          List.empty

      val splittedSource: List[ByteString] =
        if (msg.src.nonEmpty) {
          val concatenatedString = msg.src.map {
            case bs: ByteString => bs.utf8String
            case otherData      => otherData.toString
          }.mkString
          val parts: List[String] =
            if (pattern.nonEmpty)
              concatenatedString.split(pattern).map(_.trim).toList
            else
              List(concatenatedString)
          parts match {
            case p1 :: p2 :: ps =>
              // We have at least 2 entries.
              val sliced =
                if (limit > 0 && parts.size > limit)
                  parts.slice(0, limit)
                else
                  parts
              if (selected.nonEmpty) {
                val candidates = sliced.toVector
                selected
                  .map(
                    pos =>
                      Try(candidates(pos)) match {
                        case scala.util.Failure(e) => ByteString("")
                        case scala.util.Success(c) => ByteString(c)
                    }
                  )
                  .toList
              } else
                sliced.map(p => ByteString(p))
            case _ => parts.map(p => ByteString(p))
          }
        } else
          List(ByteString(""))

      log.debug("Finished splitting of source.")
      log.debug("Splitted {} into {}. -> {} elements",
                msg.src,
                splittedSource,
                splittedSource.length)

      context become receive
      sender() ! TransformerResponse(splittedSource, classOf[String])
  }
}
