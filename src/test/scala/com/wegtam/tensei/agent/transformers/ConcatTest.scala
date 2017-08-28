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

import akka.testkit.TestActorRef
import akka.util.ByteString
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform,
  StartTransformation,
  TransformerResponse
}
import com.wegtam.tensei.adt.TransformerOptions

class ConcatTest extends ActorSpec {
  describe("Transformers") {
    describe("Concat") {
      describe("when given an empty list") {
        it("should return an empty string") {
          val actor = TestActorRef(Concat.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          actor ! StartTransformation(List(),
                                      new TransformerOptions(classOf[String], classOf[String]))

          val response = TransformerResponse(List(ByteString("")), classOf[String])

          expectMsg(response)
        }

        describe("with a prefix") {
          it("should return a string that equals the prefix") {
            val actor = TestActorRef(Concat.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("prefix", "Fancy Prefix"))

            actor ! StartTransformation(List(),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))

            val response = TransformerResponse(List(ByteString("Fancy Prefix")), classOf[String])

            expectMsg(response)
          }
        }

        describe("with a suffix") {
          it("should return a string that equals the suffix") {
            val actor = TestActorRef(Concat.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("suffix", "Lame Suffix"))

            actor ! StartTransformation(List(),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))

            val response = TransformerResponse(List(ByteString("Lame Suffix")), classOf[String])

            expectMsg(response)
          }
        }

        describe("with a prefix and a suffix") {
          it("should return a string that equals the prefix + the suffix") {
            val actor = TestActorRef(Concat.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("prefix", "Fancy Prefix"), ("suffix", "Lame Suffix"))

            actor ! StartTransformation(List(),
                                        new TransformerOptions(classOf[String],
                                                               classOf[String],
                                                               params))

            val response =
              TransformerResponse(List(ByteString("Fancy PrefixLame Suffix")), classOf[String])

            expectMsg(response)
          }
        }
      }

      describe("when given a list of strings") {
        it("should return the concatenated strings") {
          val actor = TestActorRef(Concat.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          actor ! StartTransformation(List(ByteString("one"),
                                           ByteString("two"),
                                           ByteString("three")),
                                      new TransformerOptions(classOf[String], classOf[String]))

          val response = TransformerResponse(List(ByteString("onetwothree")), classOf[String])

          expectMsg(response)
        }

        describe("with a separator") {
          it("should return the concatenated strings separated by the given separator") {
            val actor = TestActorRef(Concat.props)

            actor ! PrepareForTransformation

            expectMsg(ReadyToTransform)

            val params = List(("separator", "<<I AM A SEPARATOR>>"))

            actor ! StartTransformation(
              List(ByteString("one"), ByteString("two"), ByteString("three")),
              new TransformerOptions(classOf[String], classOf[String], params)
            )

            val response = TransformerResponse(
              List(ByteString("one<<I AM A SEPARATOR>>two<<I AM A SEPARATOR>>three")),
              classOf[String]
            )

            expectMsg(response)
          }
        }
      }
    }
  }
}
