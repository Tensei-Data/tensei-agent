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

import java.time.{ OffsetDateTime, ZoneOffset }

import akka.actor.Props
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  StartTransformation,
  TransformerResponse
}
import com.wegtam.tensei.agent.transformers.TimestampOffsetTransformer.{
  DefaultOffset,
  TimestampOffsetTransformerMode
}

import scala.util.Try

/**
  * Convert a given timestamp value using the given zone offset.
  *
  * The transformer accepts the following parameters:
  * <dl>
  *   <dt>`offset`</dt>
  *   <dd>The desired offset given in `+|-Hours:Minutes` notation which defaults to `UTC` (`00:00`) if not set.</dd>
  *   <dt>`mode`</dt>
  *   <dd>Determines if the actual time will be converted (`convert`) or kept (`keep`). Defaults to `convert`.</dd>
  * </dl>
  */
class TimestampOffsetTransformer extends BaseTransformer {

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override def transform: Receive = {
    case StartTransformation(src, options) =>
      log.debug("Starting TimestampOffsetTransformer.")
      val offset: ZoneOffset =
        paramValueO("offset")(options.params).map(ZoneOffset.of).getOrElse(DefaultOffset)
      val mode: TimestampOffsetTransformerMode = Try(
        TimestampOffsetTransformerMode.fromString(paramValue("mode")(options.params))
      ).getOrElse(TimestampOffsetTransformerMode.Convert)

      def convert(t: OffsetDateTime): OffsetDateTime =
        TimestampOffsetTransformer.transform(offset)(mode)(t)

      val result = src.map {
        case t: OffsetDateTime => convert(t)
        case anyType           => anyType
      }

      context.become(receive)
      sender() ! TransformerResponse(result, classOf[String])
  }
}

object TimestampOffsetTransformer {
  final val DefaultOffset: ZoneOffset = ZoneOffset.UTC

  def props: Props = Props(new TimestampOffsetTransformer())

  sealed trait TimestampOffsetTransformerMode

  object TimestampOffsetTransformerMode {

    case object Convert extends TimestampOffsetTransformerMode

    case object Keep extends TimestampOffsetTransformerMode

    @throws[MatchError](cause = "The given string is no mode!")
    def fromString(s: String): TimestampOffsetTransformerMode = s match {
      case "convert" => Convert
      case "keep"    => Keep
    }
  }

  /**
    * Convert the given offset datetime into the provided zone offset using the
    * specified mode.
    *
    * @param o A zone offset.
    * @param m The conversion mode (`Convert` will use `withOffsetSameInstant` and `Keep` will use `withOffsetSameLocal`).
    * @param t An offset datetime.
    * @return The converted offset datetime.
    */
  def transform(
      o: ZoneOffset
  )(m: TimestampOffsetTransformerMode)(t: OffsetDateTime): OffsetDateTime = m match {
    case TimestampOffsetTransformerMode.Convert => t.withOffsetSameInstant(o)
    case TimestampOffsetTransformerMode.Keep    => t.withOffsetSameLocal(o)
  }

}
