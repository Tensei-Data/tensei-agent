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

import java.net.ServerSocket

import akka.actor._
import akka.testkit.{ ImplicitSender, TestActorRef, TestKit }
import com.typesafe.config.ConfigFactory
import com.wegtam.tensei.adt.ClusterConstants
import com.wegtam.tensei.agent.DummyProcessor.{ CreateGenerator, CreateTransformer }
import com.wegtam.tensei.agent.generators.BaseGenerator.BaseGeneratorMessages.PrepareToGenerate
import com.wegtam.tensei.agent.generators.{ DrupalVanCodeGenerator, IDGenerator }
import org.scalatest.{ BeforeAndAfterAll, FunSpecLike, Matchers }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

abstract class ActorSpec
    extends TestKit(ActorSystem("system", ConfigFactory.load()))
    with ImplicitSender
    with FunSpecLike
    with Matchers
    with BeforeAndAfterAll {

  lazy val testConfig = ConfigFactory.load("integration-test.conf")

  def createDummyAgent(id: Option[String] = None): ActorRef =
    TestActorRef(DummyAgent.props(id.getOrElse("TENSEI-AGENT-TEST")),
                 ClusterConstants.topLevelActorNameOnAgent)

  def stopDummyAgent(): Unit =
    system.actorSelection(s"/user/${ClusterConstants.topLevelActorNameOnAgent}") ! DummyAgent.Shutdown

  override protected def afterAll(): Unit = {
    val _ = Await.result(system.terminate(), Duration.Inf)
  }
}

object ActorSpec {

  /**
    * Start a server socket and close it. The port number used by
    * the socket is considered free and returned.
    *
    * @return A port number.
    */
  def findAvailablePort(): Int = {
    val serverSocket = new ServerSocket(0)
    val freePort     = serverSocket.getLocalPort
    serverSocket.setReuseAddress(true) // Allow instant rebinding of the socket.
    serverSocket.close()
    freePort
  }

}

object DummyAgent {

  def props(id: String): Props = Props(classOf[DummyAgent], id)

  case class CreateDummyProcessor(agentRunIdentifier: Option[String])

  case class CreateProcessor(agentRunIdentifier: Option[String])

  case object StopProcessor

  case object Shutdown

}

class DummyAgent(id: String) extends Actor with ActorLogging {
  var processor: Option[ActorRef] = None

  override def receive: Receive = {
    case DummyAgent.CreateDummyProcessor(agentRunIdentifier) =>
      if (processor.isDefined)
        context.stop(processor.get)

      processor = Option(context.actorOf(DummyProcessor.props(agentRunIdentifier), Processor.name))
      sender() ! processor.get
    case DummyAgent.CreateProcessor(agentRunIdentifier) =>
      if (processor.isDefined)
        context.stop(processor.get)

      processor = Option(context.actorOf(Processor.props(agentRunIdentifier), Processor.name))
      sender() ! processor.get
    case DummyAgent.StopProcessor =>
      if (processor.isDefined)
        context.stop(processor.get)
    case DummyAgent.Shutdown =>
      context.stop(self)
    case _ =>
  }
}

object DummyProcessor {

  def props(agentRunIdentifier: Option[String]): Props =
    Props(classOf[DummyProcessor], agentRunIdentifier)

  case class CreateGenerator(className: String)

  case class CreateTransformer(className: String)

}

class DummyProcessor(agentRunIdentifier: Option[String]) extends Actor with ActorLogging {
  override def receive: Actor.Receive = {
    case CreateGenerator(className) =>
      val g = className match {
        case "com.wegtam.tensei.agent.transformers.IDTransformer" =>
          context.actorOf(IDGenerator.props, IDGenerator.name)
        case "com.wegtam.tensei.agent.transformers.DrupalVanCodeTransformer" =>
          context.actorOf(DrupalVanCodeGenerator.props, DrupalVanCodeGenerator.name)
        case _ => throw new RuntimeException(s"Unknown generator name: $className!")
      }
      g ! PrepareToGenerate
      sender() ! g
    case CreateTransformer(className) =>
      val clazz       = Class.forName(className)
      val transformer = context.actorOf(Props(clazz))
      sender() ! transformer
  }
}
