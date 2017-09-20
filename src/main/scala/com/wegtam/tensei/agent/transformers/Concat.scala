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

object Concat {
  def props: Props = Props(new Concat())
}

/**
  * A simple transformer that concatenates a list of sources.
  *
  * The transformer accepts the following parameters:
  * - `separator` A list of characters to put between the elements.
  * - `prefix` A list of characters to put in front of the concatenated elements.
  * - `suffix` A list of characters to put at the end of the concatenated elements.
  */
class Concat extends BaseTransformer {
  override def transform: Receive = {
    case msg: StartTransformation =>
      log.debug("Starting concatenation of sources: {}", msg.src)
      val params    = msg.options.params
      val separator = paramValue("separator")(params)
      val prefix    = paramValue("prefix")(params)
      val suffix    = paramValue("suffix")(params)

      val strings = msg.src.map {
        case bs: ByteString => bs.utf8String
        case None           => ""
        case otherData      => otherData.toString
      }

      val concatenatedSources = strings.mkString(prefix, separator, suffix)
      log.debug("Finnished concatenation of sources.")
      log.debug("Concatenated {} elements into {} characters.",
                msg.src.length,
                concatenatedSources.length)

      context become receive
      sender() ! TransformerResponse(List(ByteString(concatenatedSources)), classOf[String])
  }
}
