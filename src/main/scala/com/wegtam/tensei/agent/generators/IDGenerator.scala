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

package com.wegtam.tensei.agent.generators

import java.util.UUID

import akka.actor.Props
import akka.util.ByteString
import com.wegtam.tensei.agent.generators.BaseGenerator.BaseGeneratorMessages.{
  GeneratorResponse,
  PrepareToGenerate,
  ReadyToGenerate,
  StartGenerator
}

object IDGenerator {
  val name = "IDGenerator"

  def props: Props = Props(classOf[IDGenerator])
}

/**
  * A generator that creates new IDs.
  *
  * The generator accepts the following parameters:
  * - `type`              - The type of ID (long or UUID). Default is long.
  * - `field` (required)  - The name of the field that gets the new ID.
  * - `start`             - The start value of the long. Default is 0.
  */
class IDGenerator extends BaseGenerator {
  // We need to initialise the map with field values that we know have specific limitations.
  var ids: Map[String, Long] = Map(
    "JoomlaUserID" -> 820L
  ).withDefaultValue(1L)

  override def receive: Receive = {
    case PrepareToGenerate =>
      sender() ! ReadyToGenerate
      context become generate
  }

  override def generate: Receive = {
    case msg: StartGenerator =>
      val params = msg.data

      val idtype =
        if (params.exists(p => p.asInstanceOf[(String, String)]._1.equalsIgnoreCase("type"))) {
          params
            .find(p => p.asInstanceOf[(String, String)]._1.equalsIgnoreCase("type"))
            .get
            .asInstanceOf[(String, String)]
            ._2
            .trim
        } else {
          "long"
        }

      if (idtype.equalsIgnoreCase("UUID")) {
        val uuid = ByteString(UUID.randomUUID().toString)
        sender() ! new GeneratorResponse(List(uuid))
      } else {
        val field =
          if (params.exists(p => p.asInstanceOf[(String, String)]._1.equalsIgnoreCase("field"))) {
            params
              .find(p => p.asInstanceOf[(String, String)]._1.equalsIgnoreCase("field"))
              .get
              .asInstanceOf[(String, String)]
              ._2
              .trim
          } else {
            log.error("Missing field name in {}", params.mkString(", "))
            throw new IllegalArgumentException("Parameter 'field' missing!")
          }

        val start =
          if (params.exists(p => p.asInstanceOf[(String, String)]._1.equalsIgnoreCase("start"))) {
            params
              .find(p => p.asInstanceOf[(String, String)]._1.equalsIgnoreCase("start"))
              .get
              .asInstanceOf[(String, String)]
              ._2
              .toLong
          } else {
            0L
          }

        val nextId =
          if (ids(field) < start)
            start
          else
            ids(field)

        ids += field -> (nextId + 1)

        sender() ! GeneratorResponse(List(nextId))
      }
  }
}
