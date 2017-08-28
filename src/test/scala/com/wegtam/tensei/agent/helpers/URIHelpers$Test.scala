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
import com.wegtam.tensei.agent.DefaultSpec
import com.wegtam.tensei.agent.adt.{
  ConnectionTypeDatabase,
  ConnectionTypeFile,
  ConnectionTypeFileFromNetwork
}
import com.wegtam.tensei.agent.exceptions.NoSuchConnectionTypeException

class URIHelpers$Test extends DefaultSpec {
  describe("URIHelpers") {
    describe("connectionType") {
      describe("when given an uri that does not match") {
        it("should throw a NoSuchConnectionTypeException") {
          intercept[NoSuchConnectionTypeException] {
            URIHelpers.connectionType(new URI(""))
          }
        }
      }

      describe("when given a file uri") {
        it("should return ConnectionTypeFile") {
          val uri = new URI("file:///tmp/some-file.tmp")
          URIHelpers.connectionType(uri) should be(ConnectionTypeFile)
        }
      }

      describe(
        "when given one of the following uris it should return ConnectionTypeFileFromNetwork"
      ) {
        it("HTTP") {
          val uri = new URI("http://example.com")
          URIHelpers.connectionType(uri) should be(ConnectionTypeFileFromNetwork)
        }

        it("HTTPS") {
          val uri = new URI("https://example.com")
          URIHelpers.connectionType(uri) should be(ConnectionTypeFileFromNetwork)
        }

        it("SFTP") {
          val uri = new URI("sftp://example.com")
          URIHelpers.connectionType(uri) should be(ConnectionTypeFileFromNetwork)
        }

        it("FTP") {
          val uri = new URI("ftp://example.com")
          URIHelpers.connectionType(uri) should be(ConnectionTypeFileFromNetwork)
        }

        it("SMB") {
          val uri = new URI("smb://example.com")
          URIHelpers.connectionType(uri) should be(ConnectionTypeFileFromNetwork)
        }
      }

      describe("when given a JDBC uri") {
        it("should return ConnectionTypeDatabase") {
          val uri = new URI("jdbc:mysql://localhost:12345/dbname")
          URIHelpers.connectionType(uri) should be(ConnectionTypeDatabase)
        }
      }
    }
  }
}
