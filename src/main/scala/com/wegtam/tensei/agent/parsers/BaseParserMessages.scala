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

package com.wegtam.tensei.agent.parsers

import akka.actor.ActorRef
import com.wegtam.tensei.agent.adt.ParserStatus.ParserStatusType

/**
  * A sealed trait for basic parser commands.
  */
sealed trait BaseParserMessages

object BaseParserMessages {

  /**
    * This message is send when the parser has completed it's run.
    * It contains a list of all status messages of the sub parsers and a list of actor refs to
    * the data tree documents.
    *
    * @param conditions  The status messages that were returned by the sub parsers.
    * @param dataTrees   The actor refs to the data tree documents holding the parsed data.
    */
  case class Completed(conditions: List[ParserStatusType], dataTrees: List[ActorRef])
      extends BaseParserMessages

  case object Start extends BaseParserMessages

  case object Stop extends BaseParserMessages

  case object Status extends BaseParserMessages

  case object SubParserInitialize extends BaseParserMessages

  case object SubParserInitialized extends BaseParserMessages
}
