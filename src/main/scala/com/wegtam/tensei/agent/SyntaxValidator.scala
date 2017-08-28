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

import akka.actor.{ Actor, ActorLogging, Props }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.wegtam.tensei.adt.DFASDL
import com.wegtam.tensei.agent.SyntaxValidator.SyntaxValidatorMessages
import com.wegtam.tensei.agent.helpers.{ DFASDLValidator, LoggingHelpers }

import scalaz._, Scalaz._

object SyntaxValidator {

  /**
    * Helper method to create the access validator actor.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @return The props to generate the actor.
    */
  def props(agentRunIdentifier: Option[String]): Props =
    Props(classOf[SyntaxValidator], agentRunIdentifier)

  sealed trait SyntaxValidatorMessages

  object SyntaxValidatorMessages {

    case class ValidateDFASDLs(dfasdls: List[DFASDL]) extends SyntaxValidatorMessages

    case class ValidateDFASDLsResults(results: List[ValidationNel[String, DFASDL]])
        extends SyntaxValidatorMessages

  }
}

/**
  * Validates a given set of dfasdls. It returns a list of validations that hold either the error message
  * or the valid dfasdl.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  */
class SyntaxValidator(agentRunIdentifier: Option[String]) extends Actor with ActorLogging {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.clearMDC()
    super.postStop()
  }

  def receive = {
    case SyntaxValidatorMessages.ValidateDFASDLs(dfasdls) =>
      log.debug("Validating syntax for {} dfasdls.", dfasdls.size)
      val results = dfasdls map { dfasdl =>
        try {
          DFASDLValidator.validateString(dfasdl.content) // The validator throws an exception if the DFASDL is not valid.
          dfasdl.successNel[String]
        } catch {
          case e: Throwable =>
            log.error(e, "DFASDL syntax validation error!")
            e.getMessage.failNel[DFASDL]
        }
      }
      sender() ! SyntaxValidatorMessages.ValidateDFASDLsResults(results)
  }
}
