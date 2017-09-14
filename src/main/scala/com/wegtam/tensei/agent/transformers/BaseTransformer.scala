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

import akka.actor.{ Actor, ActorLogging }
import com.wegtam.tensei.adt.TransformerOptions
import com.wegtam.tensei.agent.adt.TransformerStatus
import com.wegtam.tensei.agent.adt.TransformerStatus.TransformerStatusType
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform
}

import scala.collection.immutable.Seq
import scala.language.existentials

object BaseTransformer {

  final case class StartTransformation(src: List[Any], options: TransformerOptions)

  final case class TransformerResponse(data: List[Any],
                                       dataType: Class[_],
                                       status: TransformerStatusType = TransformerStatus.OK)

  case object PrepareForTransformation

  case object ReadyToTransform

}

/**
  * The base for a transformer actor.
  *
  * It can receive a `StartTransformation` message that contains the data sources and the
  * transformation options.
  */
abstract class BaseTransformer extends Actor with ActorLogging {
  type TransformerParameter  = (String, String)
  type TransformerParameters = Seq[TransformerParameter]

  /**
    * Check if the given transformer parameter is the named one.
    *
    * @param n The name of the desired parameter.
    * @param p A parameter (name and value).
    * @return Either true or false.
    */
  private def isCorrectParameter(n: String, p: TransformerParameter): Boolean = p match {
    case (name, _) => name == n
  }

  /**
    * Return the value for the requested parameter name from the given list
    * of parameters. If the parameter is not present an empty string is returned.
    *
    * @param name The name of the desired parameter.
    * @param ps   A list of transformer parameters.
    * @return The value of the parameter or an empty string.
    */
  protected def paramValue(name: String)(ps: TransformerParameters): String =
    paramValueO(name)(ps).getOrElse("")

  /**
    * Return an option to the value of the requested parameter name from the given
    * list of parameters.
    *
    * @param name The name of the desired parameter.
    * @param ps   A list of transformer parameters.
    * @return An option to the value of the parameter if found in the list.
    */
  protected def paramValueO(name: String)(ps: TransformerParameters): Option[String] =
    ps.find(p => isCorrectParameter(name, p)).map {
      case (_, value) => value
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override def receive: Receive = {
    case PrepareForTransformation =>
      context become transform
      sender() ! ReadyToTransform
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def transform: Receive

}
