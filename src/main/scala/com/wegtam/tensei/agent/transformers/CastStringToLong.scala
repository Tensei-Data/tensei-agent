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

import scala.language.existentials

object CastStringToLong {
  def props: Props = Props(classOf[CastStringToLong])
}

/**
  * Simply cast the given string to long.
  */
class CastStringToLong extends BaseTransformer {
  override def transform: Receive = {
    case msg: StartTransformation =>
      val ret =
        if (msg.src.isEmpty)
          (List(None), None.getClass)
        else {
          val strings = msg.src.map {
            case bs: ByteString => bs.utf8String
            case None           => ""
            case otherData      => otherData.toString
          }
          val casted: List[Option[Long]] = strings.map(
            e =>
              if (e.matches("-?\\d+"))
                Option(e.toLong)
              else
              None
          )
          if (casted.contains(None))
            (List(None), None.getClass)
          else
            (casted.flatten, Long.getClass)
        }

      context become receive

      sender() ! TransformerResponse(ret._1, ret._2)
  }
}
