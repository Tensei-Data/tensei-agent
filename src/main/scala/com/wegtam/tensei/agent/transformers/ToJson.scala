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
import argonaut._, Argonaut._

import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}

object ToJson {
  def props: Props = Props(classOf[ToJson])
}

/**
  * A transformer that converts the given data into a json string.
  *
  * It accepts an option named `label` to label to json value.
  */
class ToJson extends BaseTransformer with JsonHelpers {
  override def transform: Receive = {
    case StartTransformation(src, options) =>
      log.debug("Starting conversion to json.")
      val label =
        if (options.params.exists(p => p._1 == "label"))
          options.params.find(p => p._1 == "label").get._2.asInstanceOf[String]
        else
          ""

      val jsonString: ByteString =
        src.size match {
          case 0 =>
            ByteString("")
          case 1 =>
            ByteString(createJson(src.head, label).nospaces)
          case _ =>
            val parts: List[Json] = src.map(data => createJson(data, ""))
            val js =
              if (label.isEmpty)
                jArray(parts)
              else
                Json(s"$label" := jArray(parts))
            ByteString(js.nospaces)
        }

      context.become(receive)
      sender() ! TransformerResponse(List(jsonString), classOf[String])
  }
}
