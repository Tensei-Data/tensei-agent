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
import com.wegtam.tensei.adt.TransformerOptions
import com.wegtam.tensei.agent.ActorSpec
import com.wegtam.tensei.agent.transformers.BaseTransformer.{
  PrepareForTransformation,
  ReadyToTransform,
  StartTransformation,
  TransformerResponse
}

class IfThenElseNumericTest extends ActorSpec {
  describe("Transformers") {
    describe("IfThenElse") {
      describe("when given an empty list") {
        it("should return an empty list") {
          val actor = TestActorRef(IfThenElseNumeric.props)

          actor ! PrepareForTransformation

          expectMsg(ReadyToTransform)

          actor ! StartTransformation(List(), TransformerOptions(classOf[String], classOf[String]))

          val response = TransformerResponse(List(), classOf[String])

          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list and no transformation function") {
        it("should return the given list") {
          val actor = TestActorRef(IfThenElseNumeric.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          actor ! StartTransformation(List(1, 2, 3, 4, 5),
                                      TransformerOptions(classOf[String], classOf[String]))
          val response = TransformerResponse(
            List(new java.math.BigDecimal("1"),
                 new java.math.BigDecimal("2"),
                 new java.math.BigDecimal("3"),
                 new java.math.BigDecimal("4"),
                 new java.math.BigDecimal("5")),
            classOf[String]
          )
          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list and condition evals to false") {
        it("should return the given list") {
          val actor = TestActorRef(IfThenElseNumeric.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("if", "x>6"), ("then", "0"))
          actor ! StartTransformation(List(1, 2, 3, 4, 5),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(
            List(new java.math.BigDecimal("1"),
                 new java.math.BigDecimal("2"),
                 new java.math.BigDecimal("3"),
                 new java.math.BigDecimal("4"),
                 new java.math.BigDecimal("5")),
            classOf[String]
          )
          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list, a condition and a transformation for the if-case") {
        it("should return a transformed list") {
          val actor = TestActorRef(IfThenElseNumeric.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("if", "x>3"), ("then", "x=x+2"))
          actor ! StartTransformation(List(1, 2, 3, 4, 5),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(
            List(new java.math.BigDecimal("1"),
                 new java.math.BigDecimal("2"),
                 new java.math.BigDecimal("3"),
                 new java.math.BigDecimal("6"),
                 new java.math.BigDecimal("7")),
            classOf[String]
          )
          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when given a list with ints, a condition and transformation for if and else case") {
        it("should return a transformed list") {
          val actor = TestActorRef(IfThenElseNumeric.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("if", "x<=2"), ("then", "x=x*3"), ("else", "x=2-x"))
          actor ! StartTransformation(List(1, 2, 3, 4, 5),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(
            List(new java.math.BigDecimal("3"),
                 new java.math.BigDecimal("6"),
                 new java.math.BigDecimal("-1"),
                 new java.math.BigDecimal("-2"),
                 new java.math.BigDecimal("-3")),
            classOf[String]
          )
          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe(
        "when given a list with doubles, a condition and transformation for if and else case"
      ) {
        it("should return a transformed list") {
          val actor = TestActorRef(IfThenElseNumeric.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("if", "x!=2"), ("then", "x=x/2"), ("else", "x=x*2"))
          actor ! StartTransformation(List(1.0, 2.0, 3.0, 4.0, 5.0),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(
            List(
              new java.math.BigDecimal("0.5"),
              new java.math.BigDecimal("4.0"),
              new java.math.BigDecimal("1.5"),
              new java.math.BigDecimal("2.0"),
              new java.math.BigDecimal("2.5")
            ),
            classOf[String]
          )
          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe(
        "when given a list with with ints, a condition, transformations for if and else case and format for long return values"
      ) {
        it("should return a transformed list with long values") {
          val actor = TestActorRef(IfThenElseNumeric.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("if", "x>2"), ("then", "x=x+1"), ("else", "x=x-1"), ("format", "num"))
          actor ! StartTransformation(List(1, 2, 3, 4, 5),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(List(0, 1, 4, 5, 6), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe(
        "when given a list with with doubles, a condition, transformations for if and else case and format for long return values"
      ) {
        it("should return a transformed list with long values") {
          val actor = TestActorRef(IfThenElseNumeric.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("if", "x>2"), ("then", "x=x+1"), ("else", "x=x-1"), ("format", "num"))
          actor ! StartTransformation(List(1.5, 2, 3, 4, 5),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(List(1, 1, 4, 5, 6), classOf[String])
          val d        = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when a specific decimal value is given, it should be doubled") {
        it("should return a transformed list with decimal values") {
          val actor = TestActorRef(IfThenElseNumeric.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("if", "x<1"), ("then", "x=x*2"), ("format", "dec"))
          actor ! StartTransformation(List(1.5, 0.4, 2.0, 20.21, 7.0),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(
            List(
              new java.math.BigDecimal("1.5"),
              new java.math.BigDecimal("0.8"),
              new java.math.BigDecimal("2.0"),
              new java.math.BigDecimal("20.21"),
              new java.math.BigDecimal("7.0")
            ),
            classOf[String]
          )
          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when a specific decimal value is given, it should be doubled with empy else") {
        it("should return a transformed list with decimal values") {
          val actor = TestActorRef(IfThenElseNumeric.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("if", "x<1"), ("then", "x=x*2"), ("else", ""), ("format", "dec"))
          actor ! StartTransformation(List(1.5, 0.4, 2.0, 20.21, 7.0),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(
            List(
              new java.math.BigDecimal("1.5"),
              new java.math.BigDecimal("0.8"),
              new java.math.BigDecimal("2.0"),
              new java.math.BigDecimal("20.21"),
              new java.math.BigDecimal("7.0")
            ),
            classOf[String]
          )
          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe("when a specific decimal value is given, it should be doubled with else branch") {
        it("should return a transformed list with decimal values") {
          val actor = TestActorRef(IfThenElseNumeric.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("if", "x==20.21"), ("then", "x=x*2"), ("format", "dec"))
          actor ! StartTransformation(List(1.5, 2.0, 20.21, 7.0),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(List(new java.math.BigDecimal("1.5"),
                                                  new java.math.BigDecimal("2.0"),
                                                  new java.math.BigDecimal("40.42"),
                                                  new java.math.BigDecimal("7.0")),
                                             classOf[String])
          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }

      describe(
        "when a specific decimal value is given, it should be doubled with else branch and with empty else"
      ) {
        it("should return a transformed list with decimal values") {
          val actor = TestActorRef(IfThenElseNumeric.props)
          actor ! PrepareForTransformation
          expectMsg(ReadyToTransform)
          val params = List(("if", "x==20.21"), ("then", "x=x*2"), ("else", ""), ("format", "dec"))
          actor ! StartTransformation(List(1.5, 2.0, 20.21, 7.0),
                                      TransformerOptions(classOf[String], classOf[String], params))
          val response = TransformerResponse(List(new java.math.BigDecimal("1.5"),
                                                  new java.math.BigDecimal("2.0"),
                                                  new java.math.BigDecimal("40.42"),
                                                  new java.math.BigDecimal("7.0")),
                                             classOf[String])
          val d = expectMsgType[TransformerResponse]
          d.data shouldEqual response.data
        }
      }
    }
  }
}
