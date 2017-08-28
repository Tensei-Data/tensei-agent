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

import java.net.URI
import com.wegtam.tensei.agent.adt.{
  ConnectionType,
  ConnectionTypeDatabase,
  ConnectionTypeFile,
  ConnectionTypeFileFromNetwork
}
import com.wegtam.tensei.agent.exceptions.NoSuchConnectionTypeException

object URIHelpers {
  private val typeMapper: Map[ConnectionType, List[String]] = Map(
    ConnectionTypeFile            -> List("file"),
    ConnectionTypeFileFromNetwork -> List("http", "https", "sftp", "ftp", "smb"),
    ConnectionTypeDatabase        -> List("jdbc")
  )

  /**
    * Return the connection type for the given URI.
    *
    * @param uri An URI to analyze.
    * @return The connection type.
    * @throws NoSuchElementException if no matching connection type is found!
    */
  def connectionType(uri: URI): ConnectionType =
    try {
      typeMapper.filter(_._2.contains(uri.getScheme)).head._1
    } catch {
      case e: NoSuchElementException =>
        throw new NoSuchConnectionTypeException(s"No connection type for ${uri.getScheme} ($uri)!")
    }
}
