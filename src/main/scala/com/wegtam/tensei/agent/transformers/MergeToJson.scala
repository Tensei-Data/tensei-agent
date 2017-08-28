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
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}

import scala.collection.mutable.ListBuffer

object MergeToJson {
  def props: Props = Props(classOf[MergeToJson])
}

/**
  * Transforms a given set of data into a json object.
  *
  * You must pass options the labels for the values as options e.g.
  * if you send 10 values then you'll have to send 10 options named
  * `label1`, `label2` and so on.
  */
class MergeToJson extends BaseTransformer with JsonHelpers {
  override def transform: Receive = {
    case StartTransformation(src, options) =>
      log.debug("Starting merging to json.")

      val json =
        if (src.isEmpty)
          ""
        else {
          // Collect the labels.
          val labels = ListBuffer[String]()
          var index  = 0
          while (index < src.size) {
            val name = s"label${index + 1}"
            labels += options.params.find(p => p._1 == name).get._2
            index += 1
          }

          createJsonObject(src, labels.toList).nospaces
        }

      context.become(receive)
      sender() ! new TransformerResponse(List(json), classOf[String])
  }
}
