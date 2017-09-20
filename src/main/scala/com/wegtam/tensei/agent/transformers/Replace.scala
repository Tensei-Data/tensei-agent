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

import scala.annotation.tailrec
import scala.util.Try

object Replace {
  def props: Props = Props(classOf[Replace])

  /**
    * Replace some occurences of the given regular expression in the given string
    * with the provided replacement.
    *
    * @param regex A regular expression.
    * @param replace The string that is used to replace found occurences.
    * @param max The maximum number of occurences to replace. A value lower than 1 will return the original string.
    * @param str The string that shall be worked upon.
    * @return A string in which the specified number of occurences has been replaced if they were found.
    */
  def replaceSome(regex: String, replace: String, max: Int, str: String): String = {
    val r = regex.r
    def replaceSomeRec(cnt: Int, prefix: String, suffix: String): String =
      if (cnt < 1)
        s"$prefix$suffix"
      else {
        r.findFirstMatchIn(suffix).fold(replaceSomeRec(cnt - 1, prefix, suffix)) { m =>
          val p = s"$prefix${suffix.substring(0, m.start)}$replace"
          val s = suffix.substring(m.end)
          replaceSomeRec(cnt - 1, p, s)
        }
      }
    replaceSomeRec(max, "", str)
  }

}

/**
  * A simple transformer that replaces all occurences of the search string by
  * another string.
  *
  * The transformer accepts the following parameters:
  * - `search`  - The given string that should be replaced. A comma separated list of search
  *               strings can be used to define multiple search strings that should be replaced.
  *               Each search string must be within ': 'SEARCH STRING'
  *             - When there are special characters in the string, they must be escaped. Special
  *               characters are e.g. ",", "{", "}"
  * - `replace` - The string that will be used for the replacement.
  * - `count`   - Number of occurences that will be replaced. Default is 0 and replaces all
  *               occurences in the text.
  */
class Replace extends BaseTransformer {
  override def transform: Receive = {
    case msg: StartTransformation =>
      val params = msg.options.params
      val search = paramValueO("search")(params).fold(List.empty[String])(
        _.split("'\\s*?,\\s*?'").toList
          .map(_.replaceAll("\\\\,", ",").trim().replaceFirst("'", "").stripSuffix("'"))
      )
      val replace    = paramValue("replace")(params)
      val count: Int = paramValueO("count")(params).flatMap(c => Try(c.toInt).toOption).getOrElse(0)

      /**
        * Loop over the list of provided regular expressions and try to
        * replace them.
        *
        * @param rs A list of regular expressions.
        * @param str The string that is worked upon.
        * @return The result string.
        */
      @tailrec
      def replaceAllRegs(rs: List[String])(str: String): String =
        rs match {
          case Nil => str
          case regex :: tail =>
            if (count < 1)
              replaceAllRegs(tail)(str.replaceAll(regex, replace))
            else {
              val rs = Replace.replaceSome(regex, replace, count, str)
              replaceAllRegs(tail)(rs)
            }
        }

      log.debug("Starting replace transformer on {} sources using {} regular expressions.",
                msg.src.size,
                search.size)

      val result: List[Any] =
        if (msg.src.nonEmpty) {
          if (search.nonEmpty) {
            val sourceStrings = msg.src.map {
              case bs: ByteString => bs.utf8String
              case None           => ""
              case otherData      => otherData.toString
            }
            sourceStrings.map(s => ByteString(replaceAllRegs(search)(s)))
          } else
            msg.src
        } else
          List(ByteString(""))

      log.debug("Finished replacing of source.")

      context become receive
      sender() ! TransformerResponse(result, classOf[String])
  }
}
