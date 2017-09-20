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

import akka.actor.{ ActorRef, Props }
import com.wegtam.tensei.adt.ClusterConstants
import com.wegtam.tensei.agent.Processor
import com.wegtam.tensei.agent.generators.BaseGenerator.BaseGeneratorMessages.{
  GeneratorResponse,
  StartGenerator
}
import com.wegtam.tensei.agent.generators.IDGenerator
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}

object IDTransformer {
  val name         = "IDTransformer"
  def props: Props = Props(classOf[IDTransformer])
}

/**
  * A transformer that gets a new ID from the generator and returns it to the processor.
  *
  * The transformer accepts the following parameters:
  * - `type`              - The type of ID (long or UUID). Default is long.
  * - `field` (required)  - The name of the field that gets the new ID.
  * - `start`             - The start value of the long. Default is 0.
  */
class IDTransformer extends BaseTransformer {
  var s: Option[ActorRef] = None

  override def transform: Receive = {
    case msg: GeneratorResponse => //Nachricht vom Generator
      s.foreach(_ ! TransformerResponse(msg.data, classOf[String]))

    case msg: StartTransformation => //Nachricht vom Prozessor
      s = Option(sender())

      val params = msg.options.params

      val idtype = paramValueO("type")(params).getOrElse("long")

      val field = paramValueO("field")(params).getOrElse("unnamed")

      val stp = paramValue("start")(params)
      val start =
        if (stp.nonEmpty)
          stp
        else
          "0"

      val actor =
        if (self.path.toString.contains("/singleton/"))
          context.actorSelection(
            s"/user/singleton/${ClusterConstants.topLevelActorNameOnAgent}/${Processor.name}/${IDGenerator.name}"
          )
        else
          context.actorSelection(
            s"/user/${ClusterConstants.topLevelActorNameOnAgent}/${Processor.name}/${IDGenerator.name}"
          )

      val generatorparams = List(("type", idtype), ("field", field), ("start", start))
      actor ! StartGenerator(generatorparams)
  }
}
