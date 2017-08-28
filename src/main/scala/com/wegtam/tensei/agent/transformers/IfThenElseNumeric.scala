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

import scala.math.BigDecimal.RoundingMode
import scala.util.Try

/**
  * A transformer that emulates simple if-else-branches on numeric values.
  *
  * The transformer accepts the following parameters:
  * - `if`         - A function as string which decides whether to execute the `then` or the `else` branch. Supported operators
  *                 are ==, !=, <, <=, >= and >. The condition have to be in a form like x>42 or 3.141 != x.
  * - `then`       - A function as string that describes the transformation. Supported Operators are +, -, * and /. The function
  *                 have to be in a form like x=x+1 or x=3-x for assignments or 42 for constants.
  * - `else`      - A function as string that describes the transformation. Supported Operators are +, -, * and /. The function
  *                 have to be in a form like x=x+1 or x=3-x for assignments or 42 for constants.
  * - `format`    - A string that specifies if the return values are Long ("num") or BigDecimal ("dec").
  */
class IfThenElseNumeric extends BaseTransformer {

  override def transform: Receive = {
    case msg: StartTransformation =>
      log.debug("Starting If-Then-Else-Transformer on {}", msg.src)

      val params = msg.options.params

      val result: List[Any] = msg.src.map { e =>
        val x = e match {
          case bs: ByteString => bs.utf8String
          case otherData      => otherData.toString
        }
        if (ifcondition(x, params)) thenbranch(x, params) else elsebranch(x, params)
      }

      log.debug("If-Then-Else-Transformer finished.")

      context become receive
      sender() ! TransformerResponse(result, classOf[String])
  }

  /**
    * Converts a string to an option on a bigdecimal
    *
    * @param s the value as string
    * @return the value as an option on a bigdecimal
    */
  def parseBigDecimal(s: String): Option[BigDecimal] =
    Try(BigDecimal(s)).toOption

  /**
    * evaluates if the conditon is true or false
    *
    * @param a the value from the user
    * @param params a list of parameters with condition, if branch and else branch
    * @return the evaluated condition
    */
  def ifcondition(a: Any, params: TransformerParameters): Boolean =
    paramValueO("if")(params) match {
      case None =>
        true
      case Some(fn) =>
        val func_as_str = fn.trim
        val function =
          if (func_as_str.contains("=>"))
            func_as_str.split("=>").apply(1).trim
          else
            func_as_str

        val regex        = "\\w+\\s*(>|<|>=|<=|==|!=)\\s*(\\d+[(,|.)\\d+]*)".r
        val regexreverse = "(\\d+[(,|.)\\d+]*)\\s*(>|<|>=|<=|==)\\s*\\w+".r

        val listp: Iterator[(String, Option[BigDecimal], Boolean)] =
          if (regex.findFirstIn(function).isDefined) {
            regex.findAllIn(function).matchData map { x =>
              (x.group(1), parseBigDecimal(x.group(2)), false)
            }
          } else if (regexreverse.findFirstIn(function).isDefined) {
            regexreverse.findAllIn(function).matchData map { x =>
              (x.group(2), parseBigDecimal(x.group(1)), true)
            }
          } else {
            Iterator(("", Option(BigDecimal(0)), false))
          }

        val p = listp.next()
        p._1 match {
          case ">=" =>
            if (p._3) parseBigDecimal(a.toString).get <= p._2.get
            else parseBigDecimal(a.toString).get >= p._2.get
          case "<=" =>
            if (p._3) parseBigDecimal(a.toString).get >= p._2.get
            else parseBigDecimal(a.toString).get <= p._2.get
          case "==" => parseBigDecimal(a.toString).get == p._2.get
          case "!=" => parseBigDecimal(a.toString).get != p._2.get
          case "<" =>
            if (p._3) parseBigDecimal(a.toString).get > p._2.get
            else parseBigDecimal(a.toString).get < p._2.get
          case ">" =>
            if (p._3) parseBigDecimal(a.toString).get < p._2.get
            else parseBigDecimal(a.toString).get > p._2.get
          case _ => true
        }
    }

  /**
    * executes the if branch
    *
    * @param a the value from the user
    * @param params a list of parameters with condition, if branch and else branch
    * @return the result of the if branch
    */
  def thenbranch(a: Any, params: TransformerParameters): Any = {
    val number = parseBigDecimal(a.toString).get
    paramValueO("then")(params) match {
      case None =>
        new java.math.BigDecimal(number.toString())
      case Some(fn) =>
        val func_as_str = fn.replaceAll(">", "").trim //makes x=>x+3 to x=x+3
        val format =
          if (params.exists(p => p._1 == "format")) {
            params.find(p => p._1 == "format").get._2.trim //num or dec
          } else {
            "dec"
          }
        execute(func_as_str, number, format)
    }
  }

  /**
    * executes the else branch
    *
    * @param a the value from the user
    * @param params a list of parameters with condition, if branch and else branch
    * @return the result of the else branch
    */
  def elsebranch(a: Any, params: TransformerParameters): Any = {
    val number = parseBigDecimal(a.toString).get
    paramValueO("else")(params) match {
      case None =>
        new java.math.BigDecimal(number.toString())
      case Some(fn) =>
        val func_as_str = fn.replaceAll(">", "").trim //makes x=>x+3 to x=x+3
        val format =
          if (params.exists(p => p._1 == "format")) {
            params.find(p => p._1 == "format").get._2.trim //num or dec
          } else {
            "dec"
          }
        execute(func_as_str, number, format)
    }
  }

  /**
    * converts a string to the result of the the function
    *
    * @param func_as_str the function as string
    * @param number the value from the user
    * @return the result of the function
    */
  def execute(func_as_str: String, number: BigDecimal, format: String): Any = {
    val regex_const = "^(\\d+[(,|.)\\d+]*)$".r                                       //zb 42
    val regex_func1 = "\\w+\\s*=\\s*\\w+\\s*(\\+|-|\\*|\\/)\\s*(\\d+[(,|.)\\d+]*)".r //zb x=x+42
    val regex_func2 = "\\w+\\s*=\\s*(\\d+[(,|.)\\d+]*)\\s*(\\+|-|\\*|\\/)\\s*\\w+".r //zb x=42+x
    val regex_func3 = "\\w+\\s*=\\s*(\\d+[(,|.)\\d+]*)".r                            //zb x=42

    val listp: Iterator[(String, Option[BigDecimal], Boolean)] =
      if (regex_const.findFirstIn(func_as_str).isDefined) {
        regex_const.findAllIn(func_as_str).matchData map { x =>
          ("", parseBigDecimal(x.group(1)), false)
        }
      } else {
        if (regex_func1.findFirstIn(func_as_str).isDefined) {
          regex_func1.findAllIn(func_as_str).matchData map { x =>
            (x.group(1), parseBigDecimal(x.group(2)), false)
          }
        } else if (regex_func2.findFirstIn(func_as_str).isDefined) {
          regex_func2.findAllIn(func_as_str).matchData map { x =>
            (x.group(2), parseBigDecimal(x.group(1)), true)
          }
        } else if (regex_func3.findFirstIn(func_as_str).isDefined) {
          regex_func3.findAllIn(func_as_str).matchData map { x =>
            ("", parseBigDecimal(x.group(1)), false)
          }
        } else {
          Iterator(("", Option(number), false))
        }
      }

    val p = listp.next()

    val res =
      if (p._1 != "") {
        p._1 match {
          case "+" => number + p._2.get
          case "-" => if (p._3) p._2.get - number else number - p._2.get
          case "*" => number * p._2.get
          case "/" => if (p._3) p._2.get / number else number / p._2.get
        }
      } else {
        p._2.get
      }

    if (format.equalsIgnoreCase("num")) {
      res.setScale(0, RoundingMode.HALF_UP).toLongExact
    } else {
      new java.math.BigDecimal(res.toString())
    }
  }
}

object IfThenElseNumeric {
  def props: Props = Props(classOf[IfThenElseNumeric])
}
