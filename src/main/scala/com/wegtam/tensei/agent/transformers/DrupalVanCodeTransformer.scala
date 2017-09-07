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

import akka.actor.{ ActorRef, Props }
import akka.util.ByteString
import com.wegtam.tensei.adt.ClusterConstants
import com.wegtam.tensei.agent.Processor
import com.wegtam.tensei.agent.generators.BaseGenerator.BaseGeneratorMessages.{
  GeneratorResponse,
  StartGenerator
}
import com.wegtam.tensei.agent.generators.DrupalVanCodeGenerator
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}

object DrupalVanCodeTransformer {
  val name = "DrupalVanCodeTransformer"

  def props: Props = Props(classOf[DrupalVanCodeTransformer])
}

/**
  * A transformer which creates Drupals vancode out of commentID, articleID and parent commentID. The right order of
  * the parameters is necessary.
  * Example:
  * commentID = 1, articleID = 1, parent = 0 -> vancode = 01/
  * commentID = 2, articleID = 1, parent = 1 -> vancode = 01.00/
  * commentID = 3, articleID = 2, parent = 0 -> vancode = 01/
  */
class DrupalVanCodeTransformer extends BaseTransformer {
  var receiver: Option[ActorRef] = None

  override def transform: Receive = {
    case GeneratorResponse(generatorData) =>
      receiver match {
        case None =>
          log.error("No receiver defined for DrupalVanCodeTransformer relay!")
          context.stop(self)
        case Some(ref) =>
          ref ! TransformerResponse(generatorData, classOf[String])
      }

    case StartTransformation(src, options) =>
      receiver = Option(sender())

      val params: List[String] = src
        .map {
          case bs: ByteString => bs.utf8String
          case None           => ""
          case otherData      => otherData.toString
        }
        .filterNot(_.isEmpty)

      params match {
        case id :: article :: parent :: Nil =>
          initialiseDrupalVanCodeGenerator(id, article, parent)
        case id :: article :: Nil => initialiseDrupalVanCodeGenerator(id, article, "0")
        case _ =>
          throw new IllegalArgumentException(
            s"Illegal number of parameters for DrupalVanCodeTransformer ${params.size}!"
          )
      }
  }

  private def initialiseDrupalVanCodeGenerator(id: String,
                                               article: String,
                                               parent: String): Unit = {
    val actor =
      if (self.path.toString.contains("/singleton/"))
        context.actorSelection(
          s"/user/singleton/${ClusterConstants.topLevelActorNameOnAgent}/${Processor.name}/${DrupalVanCodeGenerator.name}"
        )
      else
        context.actorSelection(
          s"/user/${ClusterConstants.topLevelActorNameOnAgent}/${Processor.name}/${DrupalVanCodeGenerator.name}"
        )
    actor ! StartGenerator(List(("commentid", id), ("article", article), ("parent", parent)))
  }

}
