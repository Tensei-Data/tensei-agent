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

import akka.actor.FSM.{ CurrentState, SubscribeTransitionCallBack, Transition }
import akka.actor.{ Actor, ActorRef, Props }
import akka.testkit.{ ImplicitSender, TestActorRef, TestFSMRef }
import com.wegtam.tensei.agent.DummyActor.DummyActorRelay
import com.wegtam.tensei.agent.ParserTest.FSMSpy
import com.wegtam.tensei.agent.parsers.BaseParserMessages

import scala.concurrent.duration._

object ParserTest {

  /**
    * A small helper actor that watches an fsm an relays the transition and state messages to
    * the given supervisor.
    *
    * @param fsmRef      The actor ref of the FSM.
    * @param supervisor  The actor ref of the supervisor.
    */
  class FSMSpy(fsmRef: ActorRef, supervisor: ActorRef) extends Actor {
    fsmRef ! SubscribeTransitionCallBack(self)

    override def receive: Receive = {
      case msg =>
        supervisor ! msg
    }
  }

  object FSMSpy {
    def props(fsmRef: ActorRef, supervisor: ActorRef): Props =
      Props(classOf[FSMSpy], fsmRef, supervisor)
  }
}

class ParserTest extends ActorSpec with ImplicitSender {
  val agentRunIdentifier = Option("ParserTest")

  describe("Parser") {
    describe("when created") {
      it("should go into Waiting state") {
        val parser = TestFSMRef(new Parser(agentRunIdentifier))
        parser.stateName should be(ParserState.Idle)
        parser.stateData should be(Parser.ParserData())
      }
    }

    describe("when waiting for checksum validation") {
      it("should change to idle mode after the timeout has been reached") {
        val checksumTimeout = FiniteDuration(
          system.settings.config.getDuration("tensei.agents.parser.checksum-validation-timeout",
                                             MILLISECONDS),
          MILLISECONDS
        )
        val waitForChecksumTimeout = checksumTimeout + FiniteDuration(2, SECONDS)

        val parser = TestFSMRef(new Parser(agentRunIdentifier))
        TestActorRef(FSMSpy.props(parser, self))

        val expectedState = CurrentState(parser, ParserState.Idle)
        expectMsg(expectedState)

        parser.setState(ParserState.ValidatingChecksums, Parser.ParserData(), checksumTimeout)
        parser.stateName should be(ParserState.ValidatingChecksums)

        expectMsg(Transition(parser, ParserState.Idle, ParserState.ValidatingChecksums))

        expectMsg(waitForChecksumTimeout,
                  Transition(parser, ParserState.ValidatingChecksums, ParserState.Idle))
      }
    }

    describe("when waiting for sub parsers") {
      it("should change to parsing mode if all parsers are initialized") {
        val dummy  = TestActorRef(DummyActor.props())
        val parser = TestFSMRef(new Parser(agentRunIdentifier))

        parser.setState(ParserState.InitializingSubParsers,
                        Parser.ParserData(uninitializedSubParsers = List(dummy)))
        parser.stateName should be(ParserState.InitializingSubParsers)

        dummy ! DummyActorRelay(BaseParserMessages.SubParserInitialized, parser)

        parser.stateName should be(ParserState.Parsing)
      }

      it("should change to parsing mode after the timeout has been reached") {
        val subParserInitializationTimeout = FiniteDuration(
          system.settings.config.getDuration("tensei.agents.parser.subparsers-init-timeout",
                                             MILLISECONDS),
          MILLISECONDS
        )
        val waitForsubParserInitializationTimeout = subParserInitializationTimeout + FiniteDuration(
          2,
          SECONDS
        )

        val dummy  = TestActorRef(DummyActor.props())
        val parser = TestFSMRef(new Parser(agentRunIdentifier))
        TestActorRef(FSMSpy.props(parser, self))

        val expectedState = CurrentState(parser, ParserState.Idle)
        expectMsg(expectedState)

        parser.setState(ParserState.InitializingSubParsers,
                        Parser.ParserData(uninitializedSubParsers = List(dummy)),
                        subParserInitializationTimeout)
        parser.stateName should be(ParserState.InitializingSubParsers)

        expectMsg(Transition(parser, ParserState.Idle, ParserState.InitializingSubParsers))

        expectMsg(waitForsubParserInitializationTimeout,
                  Transition(parser, ParserState.InitializingSubParsers, ParserState.Parsing))
      }
    }
  }
}
