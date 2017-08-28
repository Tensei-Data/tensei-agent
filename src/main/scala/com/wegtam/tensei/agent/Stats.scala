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

import akka.actor._
import com.wegtam.tensei.adt.StatsMessages.CalculateStatisticsResult
import com.wegtam.tensei.adt.{ ConnectionInformation, Cookbook, StatsResult }
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.Stats.StatsMessages.{
  FinishAnalysis,
  GetStatisticResult,
  SendStatisticResults
}
import com.wegtam.tensei.agent.analyzer.{ NumericAnalyzer, StringAnalyzer }
import com.wegtam.tensei.agent.analyzer.GenericAnalyzer.NumericAnalyzerMessages.AnalyzeData
import org.dfasdl.utils.{ DocumentHelpers, ElementNames }

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

import scalaz._, Scalaz._

object Stats {
  val name = "Stats"

  /**
    *
    * @param source      The source specifies the DFASDL that must be used from the cookbook for the analysis.
    * @param cookbook    The cookbook that holds the DFASDL for the analysis.
    * @param sourceIds   A list of IDs that are relevant for the analysis.
    * @param percent     Defines the amount of that data that should be used from the source for the analysis.
    * @return The props to generate this actor.
    */
  def props(source: ConnectionInformation,
            cookbook: Cookbook,
            sourceIds: List[String],
            percent: Int = 100): Props =
    Props(classOf[Stats], source, cookbook, sourceIds, percent)

  sealed trait StatsMessages

  object StatsMessages {
    case object FinishAnalysis extends StatsMessages

    case object GetStatisticResult extends StatsMessages

    case object SendStatisticResults extends StatsMessages
  }
}

/**
  * Receives data and calculates basic statistics depending on the type of the respective DFASDL element.
  */
class Stats(source: ConnectionInformation,
            cookbook: Cookbook,
            sourceIds: List[String],
            percent: Int = 100)
    extends Actor
    with ActorLogging
    with DocumentHelpers {
  val finishTimeout = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.analyzer.finish-timeout", MILLISECONDS),
    MILLISECONDS
  )

  val analyzers: scala.collection.mutable.HashMap[String, ActorRef] =
    new scala.collection.mutable.HashMap[String, ActorRef]()

  var results: ListBuffer[StatsResult] = new ListBuffer[StatsResult]()

  val dfasdlTree = createNormalizedDocument(cookbook.findDFASDL(source.dfasdlRef.get).get.content)

  var finishCaller: Option[ActorRef] = None

  import context.dispatcher
  var finishScheduler: Cancellable = null

  override def receive: Receive = {
    case DataTreeDocumentMessages.SaveData(elementData, elementDataHash) =>
      log.debug("Received new data for calculation!")
      if (sourceIds.contains(elementData.elementId)) {
        val dfasdlElement = dfasdlTree.getElementById(elementData.elementId)
        if (dfasdlElement != null) {
          dfasdlElement.getNodeName match {
            case ElementNames.NUMBER =>
              val analyzer: ActorRef =
                if (!analyzers.contains(elementData.elementId)) {
                  val element = dfasdlTree.getElementById(elementData.elementId)
                  val newAnalyzer =
                    context.actorOf(NumericAnalyzer.props(elementData.elementId, element, percent))
                  analyzers.put(elementData.elementId, newAnalyzer)
                  newAnalyzer
                } else
                  analyzers.get(elementData.elementId).get
              analyzer ! AnalyzeData(elementData)
            case ElementNames.STRING =>
              val analyzer: ActorRef =
                if (!analyzers.contains(elementData.elementId)) {
                  val element = dfasdlTree.getElementById(elementData.elementId)
                  val newAnalyzer =
                    context.actorOf(StringAnalyzer.props(elementData.elementId, element, percent))
                  analyzers.put(elementData.elementId, newAnalyzer)
                  newAnalyzer
                } else
                  analyzers.get(elementData.elementId).get
              analyzer ! AnalyzeData(elementData)
            case _ =>
              log.info("Statistics for element type {} not implemented yet!",
                       dfasdlElement.getNodeName)
          }
        }
      }
    case FinishAnalysis =>
      log.info("Received finish message from {}", sender())
      log.info("Send messages to the sub-analyzer to get the results.")
      finishCaller = Option(sender())
      analyzers.foreach(analyzer => {
        analyzer._2 ! GetStatisticResult
      })

      // start the scheduler
      finishScheduler =
        context.system.scheduler.scheduleOnce(finishTimeout, self, SendStatisticResults)
    case e: StatsResult =>
      results += e
      if (results.length == analyzers.size) {
        log.info("All sub-analyzers delivered their results, we send the result message!")
        val result = new CalculateStatisticsResult(results.toList.right[String],
                                                   source,
                                                   cookbook,
                                                   sourceIds,
                                                   percent)
        finishScheduler.cancel()
        if (finishCaller.isDefined)
          finishCaller.get ! result
        else
          log.error("StatsAnalyzer: No target actor for the statistical result is defined!")
      } else {
        // Reset the scheduler
        finishScheduler.cancel()
        finishScheduler =
          context.system.scheduler.scheduleOnce(finishTimeout, self, SendStatisticResults)
      }
    case SendStatisticResults =>
      log.info("Send the statistic results after timer interrupt!")
      val result = new CalculateStatisticsResult(results.toList.right[String],
                                                 source,
                                                 cookbook,
                                                 sourceIds,
                                                 percent)
      if (finishCaller.isDefined)
        finishCaller.get ! result
      else
        log.error("StatsAnalyzer: No target actor for the statistical result is defined!")
  }
}
