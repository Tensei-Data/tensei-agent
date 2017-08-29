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
import akka.testkit.TestProbe
import com.wegtam.tensei.adt.TransformerOptions
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform,
  StartTransformation,
  TransformerResponse
}
import com.wegtam.tensei.agent.{ ActorSpec, DummyAgent, DummyProcessor }
import org.scalatest.BeforeAndAfterEach

class IDTransformerTest extends ActorSpec with BeforeAndAfterEach {
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
    describe("IDTransformer") {
      describe("when given no parameters") {
        it("should return 1") {
          agent ! DummyAgent.CreateDummyProcessor(Option("TEST"))
          val p = expectMsgType[ActorRef]
          p ! DummyProcessor.CreateGenerator("com.wegtam.tensei.agent.transformers.IDTransformer")
          expectMsgType[ActorRef]
          p ! DummyProcessor.CreateTransformer(
            "com.wegtam.tensei.agent.transformers.IDTransformer"
          )
          val actor = expectMsgType[ActorRef]
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          actor ! StartTransformation(List.empty,
                                      new TransformerOptions(classOf[String], classOf[String]))
          val response = TransformerResponse(List(1), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given an id") {
        it("should return this id + 1") {
          agent ! DummyAgent.CreateDummyProcessor(Option("TEST"))
          val p = expectMsgType[ActorRef]
          p ! DummyProcessor.CreateGenerator("com.wegtam.tensei.agent.transformers.IDTransformer")
          expectMsgType[ActorRef]
          p ! DummyProcessor.CreateTransformer(
            "com.wegtam.tensei.agent.transformers.IDTransformer"
          )
          val actor = expectMsgType[ActorRef]
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("start", "41"), ("field", "anyfield"), ("type", "long"))
          actor ! StartTransformation(List.empty,
                                      new TransformerOptions(classOf[String],
                                                             classOf[String],
                                                             params))
          val response = TransformerResponse(List(41), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }
    }
  }
}
