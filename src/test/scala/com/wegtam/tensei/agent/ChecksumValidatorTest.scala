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

package com.wegtam.tensei.agent

import java.net.URI

import akka.testkit.TestActorRef
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.ChecksumValidator.ChecksumValidatorMessages

import scalaz._, Scalaz._

class ChecksumValidatorTest extends ActorSpec {
  val agentRunIdentifier = Option("ChecksumValidatorTest")

  describe("ChecksumValidator") {
    describe("when given a valid file and checksum") {
      it("should return a success") {
        val sourceFilePath = getClass.getResource("/checksum-test-01.xsd")
        val source = ConnectionInformation(
          new URI(s"$sourceFilePath"),
          None,
          None,
          None,
          Option("df0e7baec373a950f815a93d16c6ab371b716373aeae9ee72b2acfa289382edb")
        )

        val checksumValidator = TestActorRef(new ChecksumValidator(agentRunIdentifier))

        checksumValidator ! ChecksumValidatorMessages.ValidateChecksums(List(source))

        val expected = ChecksumValidatorMessages.ValidateChecksumsResults(
          results = List(source.checksum.get.successNel[String])
        )

        expectMsg(expected)
      }
    }

    describe("when given a valid file and an invalid checksum") {
      it("should return a failure containing the actual checksum") {
        val sourceFilePath = getClass.getResource("/checksum-test-01.xsd")
        val source = ConnectionInformation(new URI(s"$sourceFilePath"),
                                           None,
                                           None,
                                           None,
                                           Option("this-is-not-a-checksum"))

        val checksumValidator = TestActorRef(new ChecksumValidator(agentRunIdentifier))

        checksumValidator ! ChecksumValidatorMessages.ValidateChecksums(List(source))

        val expected = ChecksumValidatorMessages.ValidateChecksumsResults(
          results = List(
            "df0e7baec373a950f815a93d16c6ab371b716373aeae9ee72b2acfa289382edb".failNel[String]
          )
        )

        expectMsg(expected)
      }
    }

    describe("when given an empty file and a checksum") {
      it("should return a failure containing the sha256 checksum for an empty string") {
        val sourceFilePath = getClass.getResource("/checksum-test-02.txt")
        val source = ConnectionInformation(
          new URI(s"$sourceFilePath"),
          None,
          None,
          None,
          Option("df0e7baec373a950f815a93d16c6ab371b716373aeae9ee72b2acfa289382edb")
        )

        val checksumValidator = TestActorRef(new ChecksumValidator(agentRunIdentifier))

        checksumValidator ! ChecksumValidatorMessages.ValidateChecksums(List(source))

        val expected = ChecksumValidatorMessages.ValidateChecksumsResults(
          results = List(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".failNel[String]
          )
        )

        expectMsg(expected)
      }
    }

    describe("when given a non-existing file and a checksum") {
      it("should return a failure containing the io exceptions message") {
        val sourceFilePath = "/this/path/should/not/exist/upps.txt"
        val source = ConnectionInformation(
          new URI(s"file:$sourceFilePath"),
          None,
          None,
          None,
          Option("df0e7baec373a950f815a93d16c6ab371b716373aeae9ee72b2acfa289382edb")
        )

        val checksumValidator = TestActorRef(new ChecksumValidator(agentRunIdentifier))

        checksumValidator ! ChecksumValidatorMessages.ValidateChecksums(List(source))

        val response = expectMsgType[ChecksumValidatorMessages.ValidateChecksumsResults]
        response.results.head.isFailure should be(true)
        response.results.head
          .map(f => f should contain(sourceFilePath)) // The file not found exceptions includes the file path.
      }
    }

    describe("when given a not supported connection type and a checksum") {
      it("should return a failure containing the NoSuchConnectionTypeException message") {
        val sourceFilePath = "/this/path/should/not/exist/upps.txt"
        val source = ConnectionInformation(
          new URI(s"NOT-SUPPORTED:$sourceFilePath"),
          None,
          None,
          None,
          Option("df0e7baec373a950f815a93d16c6ab371b716373aeae9ee72b2acfa289382edb")
        )

        val checksumValidator = TestActorRef(new ChecksumValidator(agentRunIdentifier))

        checksumValidator ! ChecksumValidatorMessages.ValidateChecksums(List(source))

        val response = expectMsgType[ChecksumValidatorMessages.ValidateChecksumsResults]
        response.results.head.isFailure should be(true)
        response.results.head
          .map(f => f should contain("No such connection type")) // The file not found exceptions includes the file path.
      }
    }
  }
}
