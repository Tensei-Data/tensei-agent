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

package com.wegtam.tensei.agent.transformers.atomic

import akka.actor.Props
import akka.util.ByteString
import com.wegtam.tensei.agent.transformers.BaseTransformer
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}

import scala.util.Try

object TimestampAdjuster {
  def props: Props = Props(classOf[TimestampAdjuster])
}

/**
  * The TimestampAdjuster receives a list of timestamps and transforms the
  * value of each timestamp. The atomic approach performs the action before the
  * general transformers that are executed during the migration.
  *
  * Available parameters:
  * `perform` : Add or reduce the value. Values: 'add' -> x*1000 (default), 'reduce' -> x:1000
  *
  *
  */
class TimestampAdjuster extends BaseTransformer {
  override def transform: Receive = {
    case msg: StartTransformation =>
      val params = msg.options.params

      val perform: String =
        if (params.exists(p => p._1 == "perform"))
          params.find(p => p._1 == "perform").get._2.asInstanceOf[String]
        else
          ""

      def adjustTimestamp(t: Long): Long =
        perform match {
          case "reduce" => t / 1000
          case _        => t * 1000
        }

      /**
        * Try to convert the given data into a long value and apply
        * the adjust operation on it.
        *
        * @param t The data that should hold a timestamp.
        * @return Either the adjusted timestamp or the passed in data if an error occured.
        */
      def tryToAdjustTimestamp(t: Any): Any =
        Try {
          val num = t match {
            case d: java.math.BigDecimal => d.longValue()
            case bs: ByteString          => bs.utf8String.toLong
            case i: Int                  => i.toLong
            case l: Long                 => l
            case otherData               => otherData.toString.toLong
          }
          adjustTimestamp(num)
        } match {
          case scala.util.Failure(e) =>
            log.error(e, "An error occured while trying to adjust the timestamp value {}!", t)
            t
          case scala.util.Success(a) => a
        }

      val results = msg.src.map(e => tryToAdjustTimestamp(e))

      context become receive
      sender() ! TransformerResponse(results, classOf[String])
  }
}
