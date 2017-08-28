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

import java.io.File

import akka.actor.{ Actor, ActorLogging, Props }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.wegtam.tensei.adt.ConnectionInformation
import com.wegtam.tensei.agent.ChecksumValidator.ChecksumValidatorMessages
import com.wegtam.tensei.agent.adt.ConnectionTypeFile
import com.wegtam.tensei.agent.helpers.{ DigestHelpers, LoggingHelpers, URIHelpers }

import scalaz._, Scalaz._

object ChecksumValidator {

  /**
    * Helper method for creating the checksum validator actor.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @return The props to generate the actor.
    */
  def props(agentRunIdentifier: Option[String]): Props =
    Props(classOf[ChecksumValidator], agentRunIdentifier)

  sealed trait ChecksumValidatorMessages

  object ChecksumValidatorMessages {

    case class ValidateChecksums(connectionInformations: List[ConnectionInformation])
        extends ChecksumValidatorMessages

    case class ValidateChecksumsResults(results: List[ValidationNel[String, String]])
        extends ChecksumValidatorMessages

  }
}

/**
  * Validates the checksums of the given source files.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  */
case class ChecksumValidator(agentRunIdentifier: Option[String]) extends Actor with ActorLogging {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.clearMDC()
    super.postStop()
  }

  def receive = {
    case ChecksumValidatorMessages.ValidateChecksums(cons) =>
      log.debug("Starting checksum validation...")
      // Validate all connections that are a file and have a checksum defined.
      // TODO We should add more types here like network files.
      try {
        val results = cons filter (
            c => URIHelpers.connectionType(c.uri) == ConnectionTypeFile && c.checksum.isDefined
        ) map (c => validateFileChecksum(c.uri.getSchemeSpecificPart, c.checksum.get))
        sender() ! ChecksumValidatorMessages.ValidateChecksumsResults(results)
      } catch {
        case e: Throwable =>
          log.error(e, "An error occurred while validation a checksum!")
          sender() ! ChecksumValidatorMessages.ValidateChecksumsResults(
            List(e.getMessage.failNel[String])
          )
      }
  }

  /**
    * Validate the given SHA-256 checksum for the given file path.
    *
    * @param path              The path to the file.
    * @param expectedChecksum  The expected checksum.
    * @return A failure containing the wrong checksum or an occurred exception message or a success containing the valid checksum.
    */
  private def validateFileChecksum(path: String,
                                   expectedChecksum: String): ValidationNel[String, String] =
    try {
      val file     = new File(path)
      val checksum = DigestHelpers("SHA-256").digestString(file)
      if (checksum == expectedChecksum)
        checksum.successNel[String]
      else
        checksum.failNel[String]
    } catch {
      case e: Throwable =>
        log.error(e, "An error occurred while trying to validate a file checksum!")
        e.getMessage.failNel[String]
    }
}
