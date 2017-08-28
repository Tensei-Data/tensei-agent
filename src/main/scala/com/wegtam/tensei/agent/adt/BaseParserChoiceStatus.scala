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

/**
  * The different status flags for a choice.
  *
  * `BROKEN` : An error occurred during the choice mapping e.g. a choice-subtree didn't match.
  * `MATCHED` : A choice-subtree did match.
  * `UNMATCHED` : No choice-subtree did match.
  */
object BaseParserChoiceStatus {

  sealed trait BaseParserChoiceStatusType

  case object BROKEN extends BaseParserChoiceStatusType

  case object MATCHED extends BaseParserChoiceStatusType

  case object UNMATCHED extends BaseParserChoiceStatusType

}
