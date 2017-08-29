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

import java.sql.{ Date, Timestamp }
import java.time._

import akka.testkit.TestActorRef
import com.wegtam.tensei.adt.TransformerOptions
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform,
  StartTransformation,
  TransformerResponse
}
import org.scalatest.prop.PropertyChecks

class DateConverterTest extends ActorSpec with PropertyChecks {
  describe("Transformers") {
    describe("DateConverter") {
      describe("when given an empty list") {
        it("should return an empty list") {
          val actor = TestActorRef(DateConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          actor ! StartTransformation(List(), TransformerOptions(classOf[String], classOf[String]))
          val response = TransformerResponse(List(), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list with a timestamp") {
        it("should return a list with the correct DateTime") {
          val actor = TestActorRef(DateConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          actor ! StartTransformation(List(42L),
                                      TransformerOptions(classOf[String], classOf[String]))
          val ldt      = Timestamp.valueOf("1970-01-01 00:00:00.042")
          val response = TransformerResponse(List(ldt), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list with a DateTime as java.sql.Timestamp that is 0") {
        it("should return a list with the correct timestamp") {
          val actor = TestActorRef(DateConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          actor ! StartTransformation(List(0L),
                                      TransformerOptions(classOf[String], classOf[String]))
          val ldt      = Timestamp.valueOf("1970-01-01 00:00:00")
          val response = TransformerResponse(List(ldt), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list with a DateTime as java.sql.Timestamp") {
        it("should return a list with the correct timestamp") {
          val actor = TestActorRef(DateConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          actor ! StartTransformation(List(new Timestamp(42000L)),
                                      TransformerOptions(classOf[String], classOf[String]))
          val response = TransformerResponse(List(42000L), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list with a timestamp and a timezone") {
        it("should return a list with the correct datetime") {
          val actor = TestActorRef(DateConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("timezone", "+0200"))
          actor ! StartTransformation(List("42"),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val ldt      = Timestamp.valueOf("1970-01-01 02:00:00.042")
          val response = TransformerResponse(List(ldt), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list with a datetime and a timezone") {
        it("should return a list with the correct timestamp") {
          val actor = TestActorRef(DateConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("timezone", "-0800"))
          actor ! StartTransformation(List("1970-01-01 00:00:42"),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(List(28842000), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list with a date as java.sql.Date") {
        it("should return a list with the correct timestamp") {
          val actor = TestActorRef(DateConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          actor ! StartTransformation(List(new Date(86400000)),
                                      TransformerOptions(classOf[String], classOf[String]))
          val response = TransformerResponse(List(86400000), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list with a datetime and a numeric timezone") {
        it("should return a list with the correct timestamp") {
          val actor = TestActorRef(DateConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("timezone", "+0100"), ("format", "yyyy-MM-dd HH:mm:ss"))
          actor ! StartTransformation(List("1970-01-01 01:00:42"),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(List(42000), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list with a current timestamp without timezone") {
        it("should return a list with the correct datetime") {
          val actor = TestActorRef(DateConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          actor ! StartTransformation(List(1437725430000L),
                                      TransformerOptions(classOf[String], classOf[String]))
          val ldt = Timestamp.valueOf(
            ZonedDateTime
              .ofInstant(Instant.ofEpochMilli(1437725430000L), ZoneId.of("UTC"))
              .toLocalDateTime
          )
          val response = TransformerResponse(List(ldt), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list with a current timestamp and timezone") {
        it("should return a list with the correct datetime") {
          val actor = TestActorRef(DateConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("timezone", "+0200"))
          actor ! StartTransformation(List(1437725430000L),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val ldt = Timestamp.valueOf(
            ZonedDateTime
              .ofInstant(Instant.ofEpochMilli(1437725430000L), ZoneId.of("+0200"))
              .toLocalDateTime
          )
          val response = TransformerResponse(List(ldt), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list with a maximum timestamp") {
        it("should return a list with the correct datetime") {
          val actor = TestActorRef(DateConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          actor ! StartTransformation(List(2147483647000L),
                                      TransformerOptions(classOf[String], classOf[String]))
          val ldt = Timestamp.valueOf(
            ZonedDateTime
              .ofInstant(Instant.ofEpochMilli(2147483647000L), ZoneId.of("UTC"))
              .toLocalDateTime
          )
          val response = TransformerResponse(List(ldt), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }
    }
  }
}
