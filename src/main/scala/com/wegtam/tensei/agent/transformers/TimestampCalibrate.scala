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

package com.wegtam.tensei.agent.transformers

import akka.actor.Props
import akka.util.ByteString
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}

import scalaz._

object TimestampCalibrate {
  def props: Props = Props(new TimestampCalibrate())
}

/**
  * The TimestampCalibrate receives a list of timestamps and transforms the
  * value of each timestamp.
  *
  * Available parameters:
  * `perform` : Add or reduce the value. Values: 'add' -> x*1000 (default), 'reduce' -> x:1000
  */
class TimestampCalibrate extends BaseTransformer {
  override def transform: Receive = {
    case msg: StartTransformation =>
      log.debug("Calibrate timestamp of {} values.", msg.src.size)

      val params = msg.options.params

      val perform = paramValue("perform")(params)

      val results = msg.src.map { e =>
        \/.fromTryCatch {
          val v = e match {
            case bs: ByteString => bs.utf8String.toLong
            case i: Int         => i.toLong
            case l: Long        => l
            case otherType      => otherType.toString.toLong
          }
          if (perform == "reduce")
            v / 1000
          else
            v * 1000
        } match {
          case -\/(failure) =>
            log.error(failure,
                      s"An error occurred while trying to calibrate the timestamp value '$e'!")
            e
          case \/-(success) =>
            success
        }
      }

      context become receive
      sender() ! new TransformerResponse(results, classOf[String])
  }
}
