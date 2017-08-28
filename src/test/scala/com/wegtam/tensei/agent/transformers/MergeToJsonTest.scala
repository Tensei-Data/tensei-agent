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

import argonaut._, Argonaut._
import akka.testkit.TestActorRef
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform,
  StartTransformation,
  TransformerResponse
}
import com.wegtam.tensei.adt.TransformerOptions

class MergeToJsonTest extends ActorSpec {
  describe("Transformers") {
    describe("MergeToJson") {
      describe("when given an empty list") {
        it("should return an empty string") {
          val actor = TestActorRef(MergeToJson.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          actor ! new StartTransformation(List(),
                                          new TransformerOptions(classOf[String], classOf[String]))

          val response = new TransformerResponse(List(""), classOf[String])

          expectMsg(response)
        }
      }

      describe("when given a list and the appropriate options") {
        it("should return the proper json string") {
          val actor = TestActorRef(MergeToJson.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          val data =
            List("foo", None, new Integer(9), "fancy stuff", new java.lang.Double(2.71), "ending")
          val options = List(
            ("label1", "1"),
            ("label2", "2"),
            ("label3", "3"),
            ("label4", "4"),
            ("label5", "5"),
            ("label6", "6")
          )

          actor ! new StartTransformation(data,
                                          new TransformerOptions(classOf[String],
                                                                 classOf[String],
                                                                 options))

          val json =
            """
              |{
              |  "1": "foo",
              |  "2": null,
              |  "3": "9",
              |  "4": "fancy stuff",
              |  "5": 2.71,
              |  "6": "ending"
              |}
            """.stripMargin

          val expectedJson = json.parseOption.get
          val response     = new TransformerResponse(List(expectedJson.nospaces), classOf[String])

          expectMsg(response)
        }
      }
    }
  }
}
