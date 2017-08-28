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

import argonaut._
import akka.actor.{ ActorRef, ActorSelection, Props }
import akka.util.ByteString
import com.wegtam.tensei.adt.ElementReference
import com.wegtam.tensei.agent.adt.TenseiForeignKeyValueType
import com.wegtam.tensei.agent.processor.AutoIncrementValueBuffer.AutoIncrementValueBufferMessages
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}
import com.wegtam.tensei.agent.transformers.FetchForeignKeyValue.{ FetchTimeout, ReFetch }

import scala.concurrent.duration._
import scalaz._

/**
  * This transformer is appended to the end of the transformation queue if needed.
  *
  * It queries the [[com.wegtam.tensei.agent.processor.AutoIncrementValueBuffer]] for the
  * correct value.
  */
class FetchForeignKeyValue extends BaseTransformer {

  import context.dispatcher

  lazy val fetchTimeout = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.processor.fetch-auto-increment-value-timeout", MILLISECONDS),
    MILLISECONDS
  )
  lazy val reFetchInterval = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.processor.fetch-auto-increment-value-refetch", MILLISECONDS),
    MILLISECONDS
  )

  var buffer: Option[ActorSelection] = None
  var receiver: Option[ActorRef]     = None

  override def transform: Receive = {
    case msg: StartTransformation =>
      val params = msg.options.params
      val bufferPath: String = params
        .find(_._1 == "autoIncBufferPath")
        .map(_._2)
        .getOrElse(throw new IllegalArgumentException("Missing buffer path parameter!"))
      val ref: String \/ ElementReference = params
        .find(_._1 == "reference")
        .map(p => Parse.decodeEither[ElementReference](p._2))
        .getOrElse(throw new IllegalArgumentException("Missing element reference parameter!"))
      ref match {
        case -\/(failure) =>
          throw new RuntimeException(s"Could not parse element reference: $failure")
        case \/-(success) =>
          receiver = Option(sender())
          val sel = context.actorSelection(bufferPath)
          buffer = Option(sel)
          if (msg.src.head != None)
            msg.src.head match {
              case d: ByteString =>
                sel ! AutoIncrementValueBufferMessages.Return(
                  ref = success,
                  value = TenseiForeignKeyValueType.FkString(Option(d.utf8String))
                )
              case d: Long =>
                sel ! AutoIncrementValueBufferMessages.Return(
                  ref = success,
                  value = TenseiForeignKeyValueType.FkLong(Option(d))
                )
              case d: String =>
                sel ! AutoIncrementValueBufferMessages.Return(
                  ref = success,
                  value = TenseiForeignKeyValueType.FkString(Option(d))
                )
            } else {
            // FIXME We don't know what type to use here because we haven't implemented strong typing!
            log.warning("Possibly missing foreign key value for None type!")
          }
          val _ = context.system.scheduler.scheduleOnce(fetchTimeout, self, FetchTimeout)
      }

    case AutoIncrementValueBufferMessages.ChangedValue(ref, oldValue, newValue) =>
      // FIXME This `.getOrElse(None)` is a workaround for not having strong typing!
      newValue match {
        case x: TenseiForeignKeyValueType.FkDate =>
          receiver.get ! TransformerResponse(List(x.value.getOrElse(None)), classOf[java.sql.Date])
        case x: TenseiForeignKeyValueType.FkLong =>
          receiver.get ! TransformerResponse(List(x.value.getOrElse(None)), classOf[Long])
        case x: TenseiForeignKeyValueType.FkString =>
          receiver.get ! TransformerResponse(List(x.value.fold(None: Any)(v => ByteString(v))),
                                             classOf[String])
      }
      receiver = None
      buffer = None
      context become receive

    case AutoIncrementValueBufferMessages.ValueNotFound(ref, value) =>
      log.debug("Auto-increment value not found. Retrying.")
      val _ = context.system.scheduler.scheduleOnce(reFetchInterval, self, ReFetch(ref, value))

    case ReFetch(ref, value) =>
      log.debug("Trying to re-fetch auto-increment value.")
      buffer.get ! AutoIncrementValueBufferMessages.Return(ref, value)

    case FetchTimeout =>
      log.error("Could not fetch auto-increment value in time!")
      buffer = None
      receiver = None
      context become receive
  }

}

object FetchForeignKeyValue {

  def props: Props = Props(classOf[FetchForeignKeyValue])

  case object FetchTimeout

  final case class ReFetch(ref: ElementReference, value: TenseiForeignKeyValueType)

}
