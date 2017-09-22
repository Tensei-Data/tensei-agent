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

import akka.actor.{ Actor, ActorLogging, Props }
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.metrics.{ ClusterMetricsChanged, ClusterMetricsExtension }
import com.wegtam.tensei.adt.RuntimeStats
import akka.cluster.metrics.NodeMetrics
import akka.cluster.metrics.StandardMetrics.HeapMemory
import akka.cluster.metrics.StandardMetrics.Cpu
import com.wegtam.tensei.agent.ClusterMetricsListener.ClusterMetricsListenerMessages

object ClusterMetricsListener {

  /**
    * A sealed trait for cluster metrics listener messages.
    */
  sealed trait ClusterMetricsListenerMessages

  /**
    * A companion object to keep the namespace clean.
    */
  object ClusterMetricsListenerMessages {

    /**
      * Remove the metrics for the given agent hostname from the buffer.
      *
      * @param hostname The hostname of the agent node.
      */
    case class RemoveMetrics(hostname: String) extends ClusterMetricsListenerMessages

    /**
      * Report the collected metrics to the sender.
      */
    case object ReportMetrics extends ClusterMetricsListenerMessages

    /**
      * Empty the metrics buffer.
      */
    case object ResetMetrics extends ClusterMetricsListenerMessages

  }

  def props: Props = Props(new ClusterMetricsListener())

  val name = "ClusterMetricsListener"
}

/**
  * A simple actor that gets cluster metrics events.
  */
class ClusterMetricsListener extends Actor with ActorLogging {
  // A mutable map holding our stats.
  val metricsBuffer = scala.collection.mutable.Map.empty[String, RuntimeStats]
  val extension     = ClusterMetricsExtension(context.system)

  /**
    * Subscribe to the cluster metrics events upon start.
    */
  override def preStart(): Unit = extension.subscribe(self)

  /**
    * Unsubscribe from cluster events after stop.
    */
  override def postStop(): Unit = extension.unsubscribe(self)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override def receive: Receive = {
    case ClusterMetricsListenerMessages.RemoveMetrics(hostname) =>
      log.debug("Got request to remove metrics for {}.", hostname)
      val _ = metricsBuffer.remove(hostname)
    case ClusterMetricsListenerMessages.ReportMetrics =>
      log.debug("Reporting metrics to {}.", sender().path)
      sender() ! metricsBuffer.toMap // We must send an immutable map!
    case ClusterMetricsListenerMessages.ResetMetrics =>
      log.debug("Got request to reset metrics buffer.")
      metricsBuffer.clear()
    case ClusterMetricsChanged(clusterMetrics) =>
      clusterMetrics foreach { nodeMetrics =>
        saveHeapMetrics(nodeMetrics)
        saveCpuMetrics(nodeMetrics)
      }
    case state: CurrentClusterState => // We ignore the current cluster state.
  }

  /**
    * Report the metrics via the proxy.
    *
    * @param nodeMetrics The node metrics.
    */
  def saveHeapMetrics(nodeMetrics: NodeMetrics): Unit = nodeMetrics match {
    case HeapMemory(address, timestamp, used, committed, max) =>
      log.debug("HEAP on {}: {} / {}",
                address,
                used.doubleValue / 1024 / 1024,
                committed.doubleValue / 1024 / 1024)
      val nodeHostname = address.host.getOrElse("unknown-host")
      val metrics      = metricsBuffer.getOrElse(nodeHostname, RuntimeStats(0, 0, 0, 0, None))
      val _ = metricsBuffer.put(nodeHostname,
                                metrics.copy(freeMemory = committed - used,
                                             maxMemory = max.getOrElse(0),
                                             totalMemory = committed))
    case _ => // No heap info available.
  }

  def saveCpuMetrics(nodeMetrics: NodeMetrics): Unit = nodeMetrics match {
    case Cpu(address, timestamp, Some(systemLoadAverage), cpuCombined, cpuStolen, processors) =>
      log.debug("LOAD on {}: {} ({} processors)", address, systemLoadAverage, processors)
      val nodeHostname = address.host.getOrElse("unknown-host")
      val metrics      = metricsBuffer.getOrElse(nodeHostname, RuntimeStats(0, 0, 0, 0, None))
      val _ = metricsBuffer.put(
        nodeHostname,
        metrics.copy(processors = processors, systemLoad = Option(systemLoadAverage))
      )
    case _ => // No cpu info available.
  }
}
