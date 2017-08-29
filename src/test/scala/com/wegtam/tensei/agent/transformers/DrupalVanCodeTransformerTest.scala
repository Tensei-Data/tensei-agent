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

import akka.actor.{ ActorRef, Terminated }
import akka.testkit.{ EventFilter, TestProbe }
import akka.util.ByteString
import com.wegtam.tensei.adt.TransformerOptions
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform,
  StartTransformation,
  TransformerResponse
}
import com.wegtam.tensei.agent.{ ActorSpec, DummyAgent, DummyProcessor }
import org.scalatest.BeforeAndAfterEach

class DrupalVanCodeTransformerTest extends ActorSpec with BeforeAndAfterEach {
  var agent: ActorRef = null

  override protected def beforeEach(): Unit =
    agent = createDummyAgent(Option("TEST"))

  override protected def afterEach(): Unit = {
    val p = TestProbe()
    p.watch(agent)
    stopDummyAgent()
    val t = p.expectMsgType[Terminated]
    t.actor shouldEqual agent
    ()
  }

  describe("Transformers") {
    describe("DrupalVanCodeTransformer") {
      describe("when given no parameters") {
        it("should log an error") {
          agent ! DummyAgent.CreateDummyProcessor(Option("TEST"))
          val p = expectMsgType[ActorRef]
          p ! DummyProcessor.CreateGenerator(
            "com.wegtam.tensei.agent.transformers.DrupalVanCodeTransformer"
          )
          expectMsgType[ActorRef]
          p ! DummyProcessor.CreateTransformer(
            "com.wegtam.tensei.agent.transformers.DrupalVanCodeTransformer"
          )
          val actor = expectMsgType[ActorRef]
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          EventFilter[IllegalArgumentException](source = actor.path.toString, occurrences = 1) intercept {
            actor ! StartTransformation(List(),
                                        TransformerOptions(classOf[String], classOf[String]))
          }
        }
      }

      describe("when given multiple comments") {
        it("should return the correct vancodes") {
          agent ! DummyAgent.CreateDummyProcessor(Option("TEST"))
          val p = expectMsgType[ActorRef]
          p ! DummyProcessor.CreateGenerator(
            "com.wegtam.tensei.agent.transformers.DrupalVanCodeTransformer"
          )
          expectMsgType[ActorRef]
          p ! DummyProcessor.CreateTransformer(
            "com.wegtam.tensei.agent.transformers.DrupalVanCodeTransformer"
          )
          val actor = expectMsgType[ActorRef]
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          actor ! StartTransformation(List(1, 1),
                                      TransformerOptions(classOf[String], classOf[String]))
          val response1 = TransformerResponse(List(ByteString("01/")), classOf[String])
          val d         = expectMsgType[TransformerResponse]
          d.data shouldEqual response1.data

          actor ! StartTransformation(List(2, 1),
                                      TransformerOptions(classOf[String], classOf[String]))
          val response2 = TransformerResponse(List(ByteString("02/")), classOf[String])
          val e         = expectMsgType[TransformerResponse]
          e.data shouldEqual response2.data

          actor ! StartTransformation(List(3, 1, 2),
                                      TransformerOptions(classOf[String], classOf[String]))
          val response3 = TransformerResponse(List(ByteString("02.00/")), classOf[String])
          val f         = expectMsgType[TransformerResponse]
          f.data shouldEqual response3.data

          actor ! StartTransformation(List(4, 2, 0),
                                      TransformerOptions(classOf[String], classOf[String]))
          val response4 = TransformerResponse(List(ByteString("01/")), classOf[String])
          val g         = expectMsgType[TransformerResponse]
          g.data shouldEqual response4.data
        }
      }
    }
  }
}
