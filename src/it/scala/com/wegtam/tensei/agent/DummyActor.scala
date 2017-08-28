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

package com.wegtam.tensei.agent

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import com.wegtam.tensei.agent.DummyActor.DummyActorRelay

object DummyActor {
  def props(): Props = Props(classOf[DummyActor])

  case class DummyActorRelay(msg: Any, receiver: ActorRef)
}

/**
  * A dummy actor for testing.
  *
  * It can relay messages that has been sent to it.
  */
class DummyActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case DummyActorRelay(msg, receiver) =>
      receiver ! msg
    case msg =>
      log.debug("DummyActor got an unhandled message: {}", msg)
  }
}
