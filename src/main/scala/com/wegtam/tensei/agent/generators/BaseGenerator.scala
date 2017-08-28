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

import akka.actor.{ Actor, ActorLogging, Props }

object BaseGenerator {
  def props: Props = Props(classOf[BaseGenerator])

  sealed trait BaseGeneratorMessages

  object BaseGeneratorMessages {
    case class StartGenerator(data: List[Any]) extends BaseGeneratorMessages

    case class GeneratorResponse(data: List[Any]) extends BaseGeneratorMessages

    case object PrepareToGenerate extends BaseGeneratorMessages

    case object ReadyToGenerate extends BaseGeneratorMessages
  }
}

abstract class BaseGenerator extends Actor with ActorLogging {
  override def receive: Receive

  def generate: Receive
}
