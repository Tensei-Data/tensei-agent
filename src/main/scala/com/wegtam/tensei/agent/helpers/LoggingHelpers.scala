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

package com.wegtam.tensei.agent.helpers

/**
  * Some helper methods for logging.
  */
object LoggingHelpers {

  /**
    * Generate the map for adding to the `log.mdc` for having custom log variables.
    *
    * @param agentRunIdentifier An option to the agent run identifier which is usually a uuid.
    * @return The map holding the uuid or an empty string if it wasn't defined.
    */
  def generateMdcEntryForRunIdentifier(agentRunIdentifier: Option[String]): Map[String, String] =
    Map("runId" -> agentRunIdentifier.getOrElse("DEFAULT-RUN"))
}
