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

import java.time.{ DateTimeException, OffsetDateTime, ZoneOffset }

import akka.testkit.{ EventFilter, TestActorRef }
import com.wegtam.tensei.adt.TransformerOptions
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform,
  StartTransformation,
  TransformerResponse
}
import com.wegtam.tensei.agent.transformers.TimestampOffsetTransformer.TimestampOffsetTransformerMode
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks

class TimestampOffsetTransformerTest extends ActorSpec with PropertyChecks {
  private val offsets = for { o <- Gen.chooseNum(-12, 12) } yield {
    if (o < 0)
      o.toString
    else
      s"+$o"
  }

  describe("TimestampOffsetTransformer") {
    describe("#transform") {
      it("should work correctly") {
        forAll(offsets) { o =>
          val t  = OffsetDateTime.now()
          val z  = if (o.isEmpty) ZoneOffset.UTC else ZoneOffset.of(o)
          val tc = t.withOffsetSameInstant(z)
          val tk = t.withOffsetSameLocal(z)

          TimestampOffsetTransformer.transform(z)(TimestampOffsetTransformerMode.Convert)(t) should be(
            tc
          )
          TimestampOffsetTransformer.transform(z)(TimestampOffsetTransformerMode.Keep)(t) should be(
            tk
          )
        }
      }
    }

    describe("when given no data") {
      it("should return an empty list") {
        val actor = TestActorRef(TimestampOffsetTransformer.props)

        actor ! PrepareForTransformation

        expectMsg(ReadyToTransform)

        actor ! StartTransformation(List(), TransformerOptions(classOf[String], classOf[String]))

        val expectedResponse = TransformerResponse(List(), classOf[String])

        val response = expectMsg(expectedResponse)

        response.data.size should be(0)
      }
    }

    describe("when given one timestamp value") {
      it("should return a list with the transformed value") {
        val actor = TestActorRef(TimestampOffsetTransformer.props)

        actor ! PrepareForTransformation

        expectMsg(ReadyToTransform)

        val t  = OffsetDateTime.now()
        val z  = ZoneOffset.of("+06:30")
        val tc = t.withOffsetSameInstant(z)

        actor ! StartTransformation(List(t),
                                    TransformerOptions(classOf[String],
                                                       classOf[String],
                                                       List(("offset", z.getId),
                                                            ("mode", "convert"))))

        val expectedResponse = TransformerResponse(List(tc), classOf[String])

        val response = expectMsg(expectedResponse)

        response.data.size should be(1)
      }
    }

    describe("when given a list of timestamp values") {
      it("should return a list with the transformed values") {
        val actor = TestActorRef(TimestampOffsetTransformer.props)

        actor ! PrepareForTransformation

        expectMsg(ReadyToTransform)

        val ta  = OffsetDateTime.now()
        val tb  = OffsetDateTime.now().plusMinutes(15L)
        val tc  = OffsetDateTime.now().plusHours(3L)
        val td  = OffsetDateTime.now().plusMinutes(768L)
        val z   = ZoneOffset.of("+06:30")
        val tac = ta.withOffsetSameLocal(z)
        val tbc = tb.withOffsetSameLocal(z)
        val tcc = tc.withOffsetSameLocal(z)
        val tdc = td.withOffsetSameLocal(z)

        actor ! StartTransformation(List(ta, tb, tc, td),
                                    TransformerOptions(classOf[String],
                                                       classOf[String],
                                                       List(("offset", z.getId), ("mode", "keep"))))

        val expectedResponse = TransformerResponse(List(tac, tbc, tcc, tdc), classOf[String])

        val response = expectMsg(expectedResponse)

        response.data.size should be(4)
      }
    }

    describe("when given an invalid mode") {
      it("should use the default mode") {
        val actor = TestActorRef(TimestampOffsetTransformer.props)

        actor ! PrepareForTransformation

        expectMsg(ReadyToTransform)

        val t  = OffsetDateTime.now()
        val z  = ZoneOffset.of("+06:30")
        val tc = t.withOffsetSameInstant(z)

        actor ! StartTransformation(
          List(t),
          TransformerOptions(classOf[String],
                             classOf[String],
                             List(("offset", z.getId),
                                  ("mode", "This is not the mode you are looking for.")))
        )

        val expectedResponse = TransformerResponse(List(tc), classOf[String])

        val response = expectMsg(expectedResponse)

        response.data.size should be(1)
      }
    }

    describe("when given invalid offset") {
      it("should crash") {
        val actor = TestActorRef(TimestampOffsetTransformer.props)

        actor ! PrepareForTransformation

        expectMsg(ReadyToTransform)

        val t = OffsetDateTime.now()

        EventFilter[DateTimeException](source = actor.path.toString, occurrences = 1) intercept {
          actor ! StartTransformation(
            List(t),
            TransformerOptions(classOf[String],
                               classOf[String],
                               List(("offset", "This is not the offset you are looking for."),
                                    ("mode", "convert")))
          )
        }
      }
    }
  }
}
