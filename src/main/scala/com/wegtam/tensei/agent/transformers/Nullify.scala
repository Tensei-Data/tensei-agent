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

object Nullify {
  def props: Props = Props(classOf[Nullify])
}

/**
  * This transformer simply returns no data. It can be used to erase data from mapped columns
  * that you don't care about.
  * For general convenience it returns a `List(None)` and a `None.getClass` type info.
  */
class Nullify extends BaseTransformer {
  override def transform: Receive = {
    case msg: StartTransformation =>
      log.debug("Erasing data from {} sources.", msg.src.size)
      context become receive
      sender() ! TransformerResponse(List(None), None.getClass)
  }
}
