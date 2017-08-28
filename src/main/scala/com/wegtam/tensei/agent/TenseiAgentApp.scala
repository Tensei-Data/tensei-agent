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

import java.io.{ FileWriter, InputStreamReader }
import java.nio.charset.StandardCharsets
import java.nio.file.{ FileSystems, Files }
import java.util.Properties

import akka.actor.{ ActorPath, ActorSelection, ActorSystem }
import akka.cluster.client.{ ClusterClient, ClusterClientSettings }
import akka.cluster.singleton.{ ClusterSingletonManager, ClusterSingletonManagerSettings }
import com.wegtam.tensei.adt.ClusterConstants
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Random, Success, Try }

/**
  * The main entry point for the Tensei agent component.
  *
  * It will try to start an actor system which represents an agent.
  * Connection to the server will be provided via a `ClusterClient`.
  */
object TenseiAgentApp {
  private final val SYSTEM_NAME = "tensei-agent"

  private final val log = LoggerFactory.getLogger(this.getClass)

  /**
    * Try to create a cluster client using the given actor system.
    *
    * @param system An actor system that must also contain the needed settings.
    * @return Either an error or the actor selection to the cluster client.
    */
  def createClusterClient(system: ActorSystem): Try[ActorSelection] =
    for {
      srvHost <- Try(system.settings.config.getString("tensei.server.hostname"))
      srvPort <- Try(system.settings.config.getInt("tensei.server.port"))
      srvPath <- Try(
        ActorPath.fromString(s"akka.tcp://tensei-system@$srvHost:$srvPort/system/concierge")
      )
      client <- Try(
        system.actorSelection(
          system
            .actorOf(ClusterClient
                       .props(ClusterClientSettings(system).withInitialContacts(Set(srvPath))),
                     "clusterClient")
            .path
        )
      )
    } yield client

  def main(args: Array[String]): Unit = {
    val r = for {
      system <- Try(ActorSystem(SYSTEM_NAME))
      client <- createClusterClient(system)
      singleton <- Try(
        system.actorOf(
          ClusterSingletonManager.props(
            singletonProps = TenseiAgent.props(checkAndCreateId(), client),
            terminationMessage = akka.actor.PoisonPill,
            settings = ClusterSingletonManagerSettings(system)
              .withRole(ClusterConstants.Roles.agent)
              .withSingletonName(ClusterConstants.topLevelActorNameOnAgent)
          ),
          name = "singleton"
        )
      )
      term <- Try {
        log.info("Started Tensei agent.")
        Await.result(system.whenTerminated, Duration.Inf)
      }
    } yield term
    r match {
      case Failure(error) =>
        log.error("An error occured!", error)
        sys.exit(1)
      case Success(_) =>
        sys.exit(0)
    }
  }

  /**
    * Try to load the ID of the agent from a java properties file with
    * the given filename.
    *
    * @param filename The name of the properties file.
    * @return An option to the ID string.
    */
  def loadIdFromProperties(filename: String): Option[String] =
    Try {
      val properties = new Properties()
      // Properties are loaded as ISO-8859-1 per default. Therefore we need to enforce utf-8.
      properties.load(
        new InputStreamReader(Files.newInputStream(FileSystems.getDefault.getPath(filename)),
                              StandardCharsets.UTF_8)
      )
      properties.getProperty("tensei.agent.id").trim
    }.toOption

  /**
    * Returns the ID or creates one if it doesn't exist.
    *
    * @return The ID of the agent.
    */
  def checkAndCreateId(): String = {
    val idFile = System.getProperty("user.dir") + System.getProperty("file.separator") + "tensei-agent-id.properties"
    loadIdFromProperties(idFile).getOrElse(createAndWriteId(idFile))
  }

  /**
    * Create an ID, write it to the given filename and return it.
    * If the file should already exist it will be overwritten!
    *
    * @param filename The file name of the properties file that should contain the ID. It will be overwritten!
    * @return The generated ID.
    */
  @throws[java.io.IOException]
  def createAndWriteId(filename: String): String = {
    val names = scala.io.Source
      .fromInputStream(getClass.getClassLoader.getResourceAsStream("names.txt"))
      .mkString
      .split(",")
    val counter = Random.nextInt(names.length)
    val id      = s"${names(counter)}"
    val writer  = new FileWriter(filename)
    writer.write(s"tensei.agent.id=$id")
    writer.flush()
    writer.close()
    id
  }
}
