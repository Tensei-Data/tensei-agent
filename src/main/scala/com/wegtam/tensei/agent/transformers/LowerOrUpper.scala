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

import java.util.Locale

import akka.actor.Props
import akka.util.ByteString
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}

object LowerOrUpper {
  def props: Props = Props(classOf[LowerOrUpper])
}

/**
  * This transformer returns a lower or upper version of the provided string.
  * Available parameter:
  * - `locale` : The locale that shall be used for the operation. Upper and lowercase functions differ depending on the locale! If none is given or an unknown locale string is given then the default locale will be used.
  * - `perform` : Perform the specified transformation. Possible values are:
  *      'lower' : All characters as lower chararcters.
  *      'upper' : All characters as upper characters.
  *      'firstlower: Only the first character as lower character, the other ones are not changed.
  *      'firstupper': Only the first character as upper character, the other ones are not changed.
  */
class LowerOrUpper extends BaseTransformer {
  override def transform: Receive = {
    case msg: StartTransformation =>
      log.debug("Starting LowerOrUpper transformer!")

      val params = msg.options.params

      val locale = params.find(p => p._1 == "locale").fold(Locale.getDefault) { p =>
        val (_, loc) = p
        val l        = Locale.forLanguageTag(loc)
        if (Locale.getAvailableLocales.contains(l))
          l
        else
          Locale.getDefault
      }

      def performOp(mode: String)(data: String): ByteString =
        if (data.nonEmpty) {
          mode.toLowerCase match {
            case "lower"      => ByteString(data.toLowerCase(locale))
            case "upper"      => ByteString(data.toUpperCase(locale))
            case "firstlower" => ByteString(s"${data.take(1).toLowerCase(locale)}${data.drop(1)}")
            case "firstupper" => ByteString(s"${data.take(1).toUpperCase(locale)}${data.drop(1)}")
            case _            => ByteString(data)
          }
        } else
          ByteString("")

      val response: TransformerResponse =
        if (msg.src.nonEmpty) {
          params.find(p => p._1 == "perform").fold(TransformerResponse(msg.src, classOf[String])) {
            perform =>
              val res = msg.src.map {
                case bs: ByteString => performOp(perform._2)(bs.utf8String)
                case st: String     => performOp(perform._2)(st)
                case otherData      => otherData
              }
              TransformerResponse(res, classOf[String])
          }
        } else
          TransformerResponse(List(ByteString("")), classOf[String])

      log.debug("Finished lower or upper of src string!")

      context become receive
      sender() ! response
  }
}
