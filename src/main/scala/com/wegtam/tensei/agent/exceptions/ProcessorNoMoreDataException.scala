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

package com.wegtam.tensei.agent.exceptions

object ProcessorNoMoreDataException {
  def create(msg: String): ProcessorNoMoreDataException = new ProcessorNoMoreDataException(msg)

  def create(msg: String, cause: Throwable) =
    new ProcessorNoMoreDataException(msg).initCause(cause)
}

case class ProcessorNoMoreDataException(msg: String,
                                        targetIdCounter: Integer = 0,
                                        writerMessageNumber: Long = 0)
    extends RuntimeException(msg)
