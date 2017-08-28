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

import scala.collection.immutable.Seq

object ExtractBiggestValue {
  def props: Props = Props(classOf[ExtractBiggestValue])
}

/**
  * Returns the "maximum" value from the given input.
  * If the given input only contains number then the maximum number is returned.
  * Otherwise the longest string is returned.
  */
class ExtractBiggestValue extends BaseTransformer {
  override def transform: Receive = {
    case msg: StartTransformation =>
      val returnValue =
        if (msg.src.nonEmpty) {
          val nums: Seq[java.math.BigDecimal] = msg.src.flatMap(anyToDecimal)
          if (nums.size != msg.src.size) {
            // Some values couldn't be converted to numerical values.
            val longestString = msg.src
              .map {
                case bs: ByteString => bs.utf8String
                case otherData      => otherData.toString
              }
              .sortBy(_.length)
              .reverse
              .headOption
              .getOrElse("")
            ByteString(longestString)
          } else
            ByteString(nums.max.toPlainString)
        } else
          ByteString("")

      context become receive
      sender() ! TransformerResponse(List(returnValue), classOf[String])
  }

  /**
    * Try to convert any given value into a BigDecimal.
    *
    * @param a A value that should ideally be a number or a string containing a number.
    * @return An option to the created decimal value.
    */
  private def anyToDecimal(a: Any): Option[java.math.BigDecimal] = {
    import scala.util.control.Exception._

    a match {
      case d: java.math.BigDecimal => Option(d)
      case bs: ByteString =>
        catching(classOf[NumberFormatException]).opt(new java.math.BigDecimal(bs.utf8String))
      case _ => catching(classOf[NumberFormatException]).opt(new java.math.BigDecimal(a.toString))
    }
  }

}
