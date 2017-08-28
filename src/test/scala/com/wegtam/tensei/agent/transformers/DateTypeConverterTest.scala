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

import java.sql.{ Date, Time, Timestamp }

import java.text.SimpleDateFormat

import akka.testkit.TestActorRef
import com.wegtam.tensei.adt.TransformerOptions
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform,
  StartTransformation,
  TransformerResponse
}

class DateTypeConverterTest extends ActorSpec with DateTypeConverterFunctions {
  describe("DateTypeConverterFunctions") {
    describe("transformDate") {
      describe("using a correct Date without target") {
        it("should return the same Date") {
          val dateString = "2012-01-02"
          val dateParser = new SimpleDateFormat("yyyy-MM-dd")
          val date       = new Date(dateParser.parse(dateString).getTime)

          val transformedDate = transformDate("", date)

          transformedDate.getClass should be(classOf[Date])

          transformedDate.toString should be(date.toString)
        }
      }

      describe("using a correct Date with `date` target") {
        it("should return a Date") {
          val dateString = "2012-01-02"
          val dateParser = new SimpleDateFormat("yyyy-MM-dd")
          val date       = new Date(dateParser.parse(dateString).getTime)

          val transformedDate = transformDate("date", date)

          transformedDate.getClass should be(classOf[Date])

          transformedDate.toString should be(date.toString)
        }
      }

      describe("using a correct Date with `time` target") {
        it("should return a Time") {
          val dateString = "2012-01-02"
          val dateParser = new SimpleDateFormat("yyyy-MM-dd")
          val date       = new Date(dateParser.parse(dateString).getTime)

          val transformedDate = transformDate("time", date)

          transformedDate.getClass should be(classOf[Time])

          transformedDate.toString should be("00:00:00")
        }
      }

      describe("using a correct Date with `datetime` target") {
        it("should return a Timestamp") {
          val dateString = "2012-01-02"
          val dateParser = new SimpleDateFormat("yyyy-MM-dd")
          val date       = new Date(dateParser.parse(dateString).getTime)

          val transformedDate = transformDate("datetime", date)

          transformedDate.getClass should be(classOf[Timestamp])

          transformedDate.toString should be("2012-01-02 00:00:00.0")
        }
      }
    }

    describe("transformTime") {
      describe("using a correct Time without target") {
        it("should return the same Time") {
          val timeString = "12:12:12"
          val timeParser = new SimpleDateFormat("HH:mm:ss")
          val time       = new Time(timeParser.parse(timeString).getTime)

          val transformedTime = transformTime("", time)

          transformedTime.getClass should be(classOf[Time])

          transformedTime.toString should be(time.toString)
        }
      }

      describe("using a correct Date with `date` target") {
        it("should return a Date") {
          val timeString = "12:12:12"
          val timeParser = new SimpleDateFormat("HH:mm:ss")
          val time       = new Time(timeParser.parse(timeString).getTime)

          val transformedTime = transformTime("date", time)

          transformedTime.getClass should be(classOf[Date])

          transformedTime.toString should be("1970-01-01")
        }
      }

      describe("using a correct Date with `time` target") {
        it("should return a Time") {
          val timeString = "12:12:12"
          val timeParser = new SimpleDateFormat("HH:mm:ss")
          val time       = new Time(timeParser.parse(timeString).getTime)

          val transformedTime = transformTime("time", time)

          transformedTime.getClass should be(classOf[Time])

          transformedTime.toString should be("12:12:12")
        }
      }

      describe("using a correct Date with `datetime` target") {
        it("should return a Timestamp") {
          val timeString = "12:12:12"
          val timeParser = new SimpleDateFormat("HH:mm:ss")
          val time       = new Time(timeParser.parse(timeString).getTime)

          val transformedTime = transformTime("datetime", time)

          transformedTime.getClass should be(classOf[Timestamp])

          transformedTime.toString should be("1970-01-01 12:12:12.0")
        }
      }
    }

    describe("transformDateTime") {
      describe("using a correct Timestamp without target") {
        it("should return the same Timestamp") {
          val timeString = "2008-07-22 12:12:12"
          val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val timestamp  = new Timestamp(timeParser.parse(timeString).getTime)

          val transformedDateTime = transformDateTime("", timestamp)

          transformedDateTime.getClass should be(classOf[Timestamp])

          transformedDateTime.toString should be(timestamp.toString)
        }
      }

      describe("using a correct Date with `date` target") {
        it("should return the Date equivalent") {
          val timeString = "2008-07-22 12:12:12"
          val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val timestamp  = new Timestamp(timeParser.parse(timeString).getTime)

          val transformedDateTime = transformDateTime("date", timestamp)

          transformedDateTime.getClass should be(classOf[Date])

          transformedDateTime.toString should be("2008-07-22")
        }
      }

      describe("using a correct Date with `time` target") {
        it("should return the Time equivalent") {
          val timeString = "2008-07-22 12:12:12"
          val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val timestamp  = new Timestamp(timeParser.parse(timeString).getTime)

          val transformedDateTime = transformDateTime("time", timestamp)

          transformedDateTime.getClass should be(classOf[Time])

          transformedDateTime.toString should be("12:12:12")
        }
      }

      describe("using a correct Date with `datetime` target") {
        it("should return the Timestamp equivalent") {
          val timeString = "2008-07-22 12:12:12"
          val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val timestamp  = new Timestamp(timeParser.parse(timeString).getTime)

          val transformedDateTime = transformDateTime("datetime", timestamp)

          transformedDateTime.getClass should be(classOf[Timestamp])

          transformedDateTime.toString should be("2008-07-22 12:12:12.0")
        }
      }
    }
  }

  describe("DateTypeConverter") {
    describe("using an empty list") {
      it("should return an empty list") {
        val actor = TestActorRef(DateTypeConverter.props)
        actor ! PrepareForTransformation
        expectMsg(ReadyToTransform)
        actor ! StartTransformation(List(), TransformerOptions(classOf[String], classOf[String]))
        val response = TransformerResponse(List(), classOf[String])
        val d        = expectMsgType[TransformerResponse]
        d.data.mkString(",") should be(response.data.mkString(","))
      }
    }

    describe("when giving a list with one Date") {
      describe("transforming into Date") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("target", "date"))
          actor ! StartTransformation(List(new Date(0)),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(List(new Date(0)), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming into Time") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("target", "time"))
          actor ! StartTransformation(List(new Date(0)),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(List(new Time(0)), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming into Timestamp") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("target", "datetime"))
          actor ! StartTransformation(List(new Date(0)),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(List(new Timestamp(0)), classOf[String])
          val d        = expectMsgType[TransformerResponse]

          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
    }

    describe("when giving a list with three Date") {
      describe("transforming into Date") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("target", "date"))
          actor ! StartTransformation(
            List(new Date(0), new Date(1000), new Date(234567)),
            TransformerOptions(classOf[String], classOf[String], params)
          )
          val response = TransformerResponse(List(new Date(0), new Date(1000), new Date(234567)),
                                             classOf[String])
          val d = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming into Time") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("target", "time"))
          actor ! StartTransformation(
            List(new Date(0), new Date(1000), new Date(234567)),
            TransformerOptions(classOf[String], classOf[String], params)
          )
          val response = TransformerResponse(
            List(new Time(0), new Time(1000), new Time(234567)),
            classOf[String]
          )
          val d = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming into Timestamp") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("target", "datetime"))
          actor ! StartTransformation(
            List(new Date(0), new Date(1000), new Date(234567)),
            TransformerOptions(classOf[String], classOf[String], params)
          )
          val response =
            TransformerResponse(List(new Timestamp(0), new Timestamp(1000), new Timestamp(234567)),
                                classOf[String])
          val d = expectMsgType[TransformerResponse]

          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
    }

    describe("when giving a list with one Time") {
      describe("transforming into Date") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "date"))
          val time1      = "12:12:12"
          val timeParser = new SimpleDateFormat("HH:mm:ss")
          actor ! StartTransformation(List(new Time(timeParser.parse(time1).getTime)),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response =
            TransformerResponse(List(new Date(timeParser.parse(time1).getTime)), classOf[String])
          val d = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming into Time") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "time"))
          val time1      = "12:12:12"
          val timeParser = new SimpleDateFormat("HH:mm:ss")
          actor ! StartTransformation(List(new Time(timeParser.parse(time1).getTime)),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response =
            TransformerResponse(List(new Time(timeParser.parse(time1).getTime)), classOf[String])
          val d = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming into Timestamp") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "datetime"))
          val time1      = "12:12:12"
          val timeParser = new SimpleDateFormat("HH:mm:ss")
          actor ! StartTransformation(List(new Time(timeParser.parse(time1).getTime)),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(List(new Timestamp(timeParser.parse(time1).getTime)),
                                             classOf[String])
          val d = expectMsgType[TransformerResponse]

          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
    }

    describe("when giving a list with three Time") {
      describe("transforming into Date") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "date"))
          val time1      = "12:12:12"
          val time2      = "02:03:22"
          val time3      = "22:12:45"
          val timeParser = new SimpleDateFormat("HH:mm:ss")
          actor ! StartTransformation(
            List(new Time(timeParser.parse(time1).getTime),
                 new Time(timeParser.parse(time2).getTime),
                 new Time(timeParser.parse(time3).getTime)),
            TransformerOptions(classOf[String], classOf[String], params)
          )
          val response = TransformerResponse(
            List(new Date(timeParser.parse(time1).getTime),
                 new Date(new Time(timeParser.parse(time2).getTime).getTime),
                 new Date(timeParser.parse(time3).getTime)),
            classOf[String]
          )
          val d = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming into Time") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "time"))
          val time1      = "12:12:12"
          val time2      = "02:03:22"
          val time3      = "22:12:45"
          val timeParser = new SimpleDateFormat("HH:mm:ss")
          actor ! StartTransformation(
            List(new Time(timeParser.parse(time1).getTime),
                 new Time(timeParser.parse(time2).getTime),
                 new Time(timeParser.parse(time3).getTime)),
            TransformerOptions(classOf[String], classOf[String], params)
          )
          val response = TransformerResponse(
            List(new Time(timeParser.parse(time1).getTime),
                 new Time(timeParser.parse(time2).getTime),
                 new Time(timeParser.parse(time3).getTime)),
            classOf[String]
          )
          val d = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming into Timestamp") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "datetime"))
          val time1      = "12:12:12"
          val time2      = "02:03:22"
          val time3      = "22:12:45"
          val timeParser = new SimpleDateFormat("HH:mm:ss")
          actor ! StartTransformation(
            List(new Time(timeParser.parse(time1).getTime),
                 new Time(timeParser.parse(time2).getTime),
                 new Time(timeParser.parse(time3).getTime)),
            TransformerOptions(classOf[String], classOf[String], params)
          )
          val response = TransformerResponse(
            List(new Timestamp(timeParser.parse(time1).getTime),
                 new Timestamp(timeParser.parse(time2).getTime),
                 new Timestamp(timeParser.parse(time3).getTime)),
            classOf[String]
          )
          val d = expectMsgType[TransformerResponse]

          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
    }

    describe("when giving a list with one Timestamp") {
      describe("transforming into Date") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "date"))
          val time1      = "2016-01-22 12:12:12"
          val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          actor ! StartTransformation(List(new Timestamp(timeParser.parse(time1).getTime)),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response =
            TransformerResponse(List(new Date(timeParser.parse(time1).getTime)), classOf[String])
          val d = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming another into Date") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "date"))
          val time1      = "06.11.2009"
          val timeParser = new SimpleDateFormat("dd.MM.yyyy")
          actor ! StartTransformation(List(new Timestamp(timeParser.parse(time1).getTime)),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response =
            TransformerResponse(List(new Date(timeParser.parse(time1).getTime)), classOf[String])
          val d = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
          d.data.mkString should be("2009-11-06")
        }
      }
      describe("transforming into Time") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "time"))
          val time1      = "2016-01-22 12:12:12"
          val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          actor ! StartTransformation(List(new Timestamp(timeParser.parse(time1).getTime)),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response =
            TransformerResponse(List(new Time(timeParser.parse(time1).getTime)), classOf[String])
          val d = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming into Timestamp") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "datetime"))
          val time1      = "2016-01-22 12:12:12"
          val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          actor ! StartTransformation(List(new Timestamp(timeParser.parse(time1).getTime)),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(List(new Timestamp(timeParser.parse(time1).getTime)),
                                             classOf[String])
          val d = expectMsgType[TransformerResponse]

          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
    }

    describe("when giving a list with three Timestamp") {
      describe("transforming into Date") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "date"))
          val time1      = "2016-01-22 12:12:12"
          val time2      = "2001-12-22 14:13:12"
          val time3      = "1987-11-12 22:33:12"
          val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          actor ! StartTransformation(
            List(new Timestamp(timeParser.parse(time1).getTime),
                 new Timestamp(timeParser.parse(time2).getTime),
                 new Timestamp(timeParser.parse(time3).getTime)),
            TransformerOptions(classOf[String], classOf[String], params)
          )
          val response = TransformerResponse(List(
                                               new Date(timeParser.parse(time1).getTime),
                                               new Date(timeParser.parse(time2).getTime),
                                               new Date(timeParser.parse(time3).getTime)
                                             ),
                                             classOf[String])
          val d = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming into Time") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "time"))
          val time1      = "2016-01-22 12:12:12"
          val time2      = "2001-12-22 14:13:12"
          val time3      = "1987-11-12 22:33:12"
          val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          actor ! StartTransformation(
            List(new Timestamp(timeParser.parse(time1).getTime),
                 new Timestamp(timeParser.parse(time2).getTime),
                 new Timestamp(timeParser.parse(time3).getTime)),
            TransformerOptions(classOf[String], classOf[String], params)
          )
          val response = TransformerResponse(List(
                                               new Time(timeParser.parse(time1).getTime),
                                               new Time(timeParser.parse(time2).getTime),
                                               new Time(timeParser.parse(time3).getTime)
                                             ),
                                             classOf[String])
          val d = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming into Timestamp") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "datetime"))
          val time1      = "2016-01-22 12:12:12"
          val time2      = "2001-12-22 14:13:12"
          val time3      = "1987-11-12 22:33:12"
          val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          actor ! StartTransformation(
            List(new Timestamp(timeParser.parse(time1).getTime),
                 new Timestamp(timeParser.parse(time2).getTime),
                 new Timestamp(timeParser.parse(time3).getTime)),
            TransformerOptions(classOf[String], classOf[String], params)
          )
          val response = TransformerResponse(
            List(
              new Timestamp(timeParser.parse(time1).getTime),
              new Timestamp(timeParser.parse(time2).getTime),
              new Timestamp(timeParser.parse(time3).getTime)
            ),
            classOf[String]
          )
          val d = expectMsgType[TransformerResponse]

          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
    }

    describe("when giving a list with three different Types") {
      describe("transforming into Date") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "date"))
          val time1      = "2016-01-22 12:12:12"
          val time2      = "2001-12-22 14:13:12"
          val time3      = "1987-11-12 22:33:12"
          val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          actor ! StartTransformation(
            List(new Date(timeParser.parse(time1).getTime),
                 new Time(timeParser.parse(time2).getTime),
                 new Timestamp(timeParser.parse(time3).getTime)),
            TransformerOptions(classOf[String], classOf[String], params)
          )
          val response = TransformerResponse(List(
                                               new Date(timeParser.parse(time1).getTime),
                                               new Date(0),
                                               new Date(timeParser.parse(time3).getTime)
                                             ),
                                             classOf[String])
          val d = expectMsgType[TransformerResponse]

          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming into Time") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "time"))
          val time1      = "2016-01-22 12:12:12"
          val time2      = "2001-12-22 14:13:12"
          val time3      = "1987-11-12 22:33:12"
          val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          actor ! StartTransformation(
            List(new Date(timeParser.parse(time1).getTime),
                 new Time(timeParser.parse(time2).getTime),
                 new Timestamp(timeParser.parse(time3).getTime)),
            TransformerOptions(classOf[String], classOf[String], params)
          )
          val response = TransformerResponse(List(
                                               new Time(timeParser.parse(time1).getTime),
                                               new Time(timeParser.parse(time2).getTime),
                                               new Time(timeParser.parse(time3).getTime)
                                             ),
                                             classOf[String])
          val d = expectMsgType[TransformerResponse]
          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
      describe("transforming into Timestamp") {
        it("should return the correct values") {
          val actor = TestActorRef(DateTypeConverter.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params     = List(("target", "datetime"))
          val time1      = "2016-01-22 12:12:12"
          val time2      = "2001-12-22 14:13:12"
          val time3      = "1987-11-12 22:33:12"
          val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          actor ! StartTransformation(
            List(new Date(timeParser.parse(time1).getTime),
                 new Time(timeParser.parse(time2).getTime),
                 new Timestamp(timeParser.parse(time3).getTime)),
            TransformerOptions(classOf[String], classOf[String], params)
          )
          val response = TransformerResponse(
            List(
              new Timestamp(timeParser.parse(time1).getTime),
              new Timestamp(timeParser.parse(time2).getTime),
              new Timestamp(timeParser.parse(time3).getTime)
            ),
            classOf[String]
          )
          val d = expectMsgType[TransformerResponse]

          d.data.mkString(",") should be(response.data.mkString(","))
        }
      }
    }
  }
}
