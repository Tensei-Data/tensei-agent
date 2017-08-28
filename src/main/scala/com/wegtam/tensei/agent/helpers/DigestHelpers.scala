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

import java.io.{ File, InputStream }
import java.nio.file.Files
import java.security.MessageDigest

import scala.annotation.tailrec

object DigestHelpers {
  def apply(algorithm: String): DigestHelpers =
    new DigestHelpers(MessageDigest.getInstance(algorithm))
}

/**
  * Helpers
  *
  * @param dg MessageDigest
  */
class DigestHelpers(dg: MessageDigest) {
  def digestString(file: File): String =
    digest(file).map("%02x" format _).mkString

  def digest(file: File): Array[Byte] = digest(Files.newInputStream(file.toPath))

  def digest(stream: InputStream): Array[Byte] =
    dg.synchronized {

      @tailrec def readNext(): Unit = {
        val c = stream.read()
        if (c > 0) {
          dg.update(c.toByte)
          readNext()
        }
      }

      dg.reset()
      readNext()
      dg.digest
    }
}
