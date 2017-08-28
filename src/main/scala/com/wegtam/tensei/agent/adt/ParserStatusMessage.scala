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

package com.wegtam.tensei.agent.adt

import akka.actor.ActorRef
import com.wegtam.tensei.agent.adt.ParserStatus.ParserStatusType

/**
  * A class wrapper for a parser status message. Usually this message is send when the parser has been completed
  * or aborted because of an error.
  *
  * @param status The status of the Parser.
  * @param sender The actor ref of the original sender.
  */
case class ParserStatusMessage(status: ParserStatusType, sender: Option[ActorRef] = None)
