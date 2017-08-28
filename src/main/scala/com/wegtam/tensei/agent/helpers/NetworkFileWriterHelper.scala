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

import java.util.StringTokenizer

import akka.stream.alpakka.ftp.FtpCredentials
import akka.stream.alpakka.ftp.FtpCredentials.{ AnonFtpCredentials, NonAnonFtpCredentials }
import com.wegtam.tensei.adt.ConnectionInformation
import com.wegtam.tensei.agent.writers.NetworkFileWriterActor.NetworkConnectionType
import com.wegtam.tensei.agent.writers.NetworkFileWriterActor.NetworkConnectionType.{
  FtpConnection,
  FtpsConnection,
  SftpConnection
}

trait NetworkFileWriterHelper {

  /**
    * Return the Ftp credentials that are used for the settings.
    *
    * @param target The ConnectionInformation of the current target.
    * @return Either [[AnonFtpCredentials]] or [[NonAnonFtpCredentials]] depending on the
    *         supplied `username` and `password` of the target.
    */
  protected def getFtpCredentials(target: ConnectionInformation): FtpCredentials =
    (target.username, target.password) match {
      case (Some(username), Some(password)) =>
        NonAnonFtpCredentials(username, password)
      case _ =>
        if (target.uri.getUserInfo != null) {
          val st = new StringTokenizer(target.uri.getUserInfo, ":")
          if (st.countTokens() == 2)
            NonAnonFtpCredentials(st.nextToken(), st.nextToken())
          else
            AnonFtpCredentials
        } else
          AnonFtpCredentials
    }

  /**
    * Determine the concrete type of the connection.
    *
    * @param target ConnectionInformation of the current connection
    * @return The concrete [[NetworkConnectionType]] object.
    */
  protected def getConnectionType(target: ConnectionInformation): NetworkConnectionType =
    target.uri.getScheme match {
      case "ftp"  => FtpConnection
      case "ftps" => FtpsConnection
      case "sftp" => SftpConnection
    }

}
