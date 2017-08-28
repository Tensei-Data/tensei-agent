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

package com.wegtam.tensei.agent

import java.io.File

import akka.testkit.{ EventFilter, TestActorRef }
import com.wegtam.tensei.adt.GlobalMessages

import scala.collection.mutable.ListBuffer

class LogStreamerTest extends ActorSpec {

  describe("LogStreamer") {
    describe("on an empty file") {
      val path = {
        val f = new File(
          getClass.getResource("/com/wegtam/tensei/agent/LogStreamerTestFileEmpty.txt").toURI
        )
        f.toPath.toAbsolutePath
      }

      describe("given valid path") {
        it("should return nothing and stop") {
          EventFilter.debug(message = "LogStreamer done, stopping.", occurrences = 1) intercept {
            val a = TestActorRef(LogStreamer.props(self, "LOGSTREAMER-TEST"))
            a ! LogStreamer.StreamLog(path)
            expectNoMsg()
          }
        }
      }

      describe("given valid path and offset") {
        it("should return nothing and stop") {
          EventFilter.debug(message = "LogStreamer done, stopping.", occurrences = 1) intercept {
            val a = TestActorRef(LogStreamer.props(self, "LOGSTREAMER-TEST"))
            a ! LogStreamer.StreamLog(path, offset = Option(23L))
            expectNoMsg()
          }
        }
      }

      describe("given valid path and maximum size") {
        it("should return nothing and stop") {
          EventFilter.debug(message = "LogStreamer done, stopping.", occurrences = 1) intercept {
            val a = TestActorRef(LogStreamer.props(self, "LOGSTREAMER-TEST"))
            a ! LogStreamer.StreamLog(path, maxSize = Option(42L))
            expectNoMsg()
          }
        }
      }

      describe("given valid path, offset and maximum size") {
        it("should return nothing and stop") {
          EventFilter.debug(message = "LogStreamer done, stopping.", occurrences = 1) intercept {
            val a = TestActorRef(LogStreamer.props(self, "LOGSTREAMER-TEST"))
            a ! LogStreamer.StreamLog(path, offset = Option(23L), maxSize = Option(42L))
            expectNoMsg()
          }
        }
      }
    }

    describe("on a non empty file") {
      val path = {
        val f =
          new File(getClass.getResource("/com/wegtam/tensei/agent/LogStreamerTestFile.txt").toURI)
        f.toPath.toAbsolutePath
      }
      val logFileLines: List[String] = {
        val i = scala.io.Source.fromURI(path.toUri).getLines()
        val l = new ListBuffer[String]
        while (i.hasNext) {
          l += i.next()
        }
        l.toList
      }
      val linesInLogFile = logFileLines.length

      describe("given valid path") {
        it("should stream the whole file") {
          val a = TestActorRef(LogStreamer.props(self, "LOGSTREAMER-TEST"))
          a ! LogStreamer.StreamLog(path)

          for (l <- 1 to linesInLogFile) {
            val m = expectMsgType[GlobalMessages.ReportAgentRunLogLine]
            m.logLine should be(logFileLines(l - 1))
          }
        }
      }

      describe("given valid path and offset") {
        describe("using offset for line endings") {
          it("should skip bytes and stream the rest of the file") {
            val a = TestActorRef(LogStreamer.props(self, "LOGSTREAMER-TEST"))
            a ! LogStreamer.StreamLog(path, offset = Option(63L))

            for (l <- 2 to linesInLogFile) {
              val m = expectMsgType[GlobalMessages.ReportAgentRunLogLine]
              m.logLine should be(logFileLines(l - 1))
            }
          }
        }

        describe("using offset for line starts") {
          it("should skip bytes and stream the rest of the file") {
            val a = TestActorRef(LogStreamer.props(self, "LOGSTREAMER-TEST"))
            a ! LogStreamer.StreamLog(path, offset = Option(5467L))

            for (l <- 27 to linesInLogFile) {
              val m = expectMsgType[GlobalMessages.ReportAgentRunLogLine]
              println(m.offet)
              m.logLine should be(logFileLines(l - 1))
            }
          }
        }

        describe("using offset somewhere in between") {
          it("should skip bytes and stream the rest of the file") {
            val a = TestActorRef(LogStreamer.props(self, "LOGSTREAMER-TEST"))
            a ! LogStreamer.StreamLog(path, offset = Option(4000L))

            for (l <- 20 to linesInLogFile) {
              val m = expectMsgType[GlobalMessages.ReportAgentRunLogLine]
              m.logLine should be(logFileLines(l - 1))
            }
          }
        }
      }

      describe("given valid path and maximum size") {
        it("should stream all lines up to the maximum size of bytes") {
          val a = TestActorRef(LogStreamer.props(self, "LOGSTREAMER-TEST"))
          a ! LogStreamer.StreamLog(path, maxSize = Option(1024L))

          for (l <- 1 to 5) {
            val m = expectMsgType[GlobalMessages.ReportAgentRunLogLine]
            m.logLine should be(logFileLines(l - 1))
          }
        }
      }

      describe("given valid path, offset and maximum size") {
        it(
          "should skip bytes and stream lines from the rest of the file up to the maximum number of bytes"
        ) {
          val a = TestActorRef(LogStreamer.props(self, "LOGSTREAMER-TEST"))
          a ! LogStreamer.StreamLog(path, offset = Option(5467L), maxSize = Option(1024L))

          for (l <- 27 to 35) {
            val m = expectMsgType[GlobalMessages.ReportAgentRunLogLine]
            m.logLine should be(logFileLines(l - 1))
          }
        }
      }
    }
  }
}
