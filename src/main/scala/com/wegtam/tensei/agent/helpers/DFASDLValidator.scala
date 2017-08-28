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

import java.io._

import org.dfasdl.utils.DocumentHelpers

/**
  * Validation functions for DFASDL documents.
  */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
object DFASDLValidator extends XmlHelpers with DocumentHelpers {
  private val builder = createDocumentBuilder()

  /**
    * Validate the DFASDL located at the local path which is resolveable
    * by using `getResourceAsStream`.
    *
    * @param localPath The path to the DFASDL.
    */
  @throws[java.io.IOException]
  @throws[java.lang.IllegalArgumentException]
  @throws[org.xml.sax.SAXException]
  def validateLocal(localPath: String): Unit = {
    require(
      localPath != null,
      !localPath.isEmpty
    )
    val _ = builder.parse(getClass.getResourceAsStream(localPath))
  }

  /**
    * Validate the given string which should contain a DFASDL.
    *
    * @param content A DFASDL.
    */
  @throws[java.io.IOException]
  @throws[java.lang.IllegalArgumentException]
  @throws[org.xml.sax.SAXException]
  def validateString(content: String): Unit = {
    require(content != null)
    val _ = builder.parse(new ByteArrayInputStream(content.getBytes))
  }

  /**
    * Validate the DFASDL located in the given file.
    *
    * @param xmlFile A file containing a DFASDL.
    */
  @throws[java.io.IOException]
  @throws[java.io.FileNotFoundException]
  @throws[java.lang.IllegalArgumentException]
  @throws[org.xml.sax.SAXException]
  def validate(xmlFile: File): Unit = {
    require(
      xmlFile != null
    )
    if (xmlFile.exists()) {
      val _ = builder.parse(xmlFile)
    } else {
      throw new FileNotFoundException("File for validation not found: " + xmlFile)
    }
  }
}
