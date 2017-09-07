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
import org.scalatest.{ BeforeAndAfterAll, FunSpecLike, Matchers }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

abstract class ActorSpecWithDebugLog
    extends TestKit(
      ActorSystem(
        "system",
        ConfigFactory.parseString("akka.loglevel=DEBUG").withFallback(ConfigFactory.load())
      )
    )
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

object ActorSpecWithDebugLog {

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
