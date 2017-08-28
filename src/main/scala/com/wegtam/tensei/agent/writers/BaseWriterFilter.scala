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

package com.wegtam.tensei.agent.writers

import akka.actor.{ Actor, ActorLogging, ActorRef, Props, Terminated }
import com.wegtam.tensei.adt.DFASDL
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages
import com.wegtam.tensei.agent.writers.BaseWriterFilter.Filter
import org.dfasdl.utils.DocumentHelpers
import org.w3c.dom.traversal.{ DocumentTraversal, NodeFilter }
import org.w3c.dom.{ Document, Element }

import scala.collection.immutable.SortedSet

/**
  * This actor receives a write request for a batch of data from the database writer.
  * It then filters the received data for already written (unique) values and executes
  * the actual write operation.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  * @param dfasdl The dfasdl describing the target database.
  */
class BaseWriterFilter(
    agentRunIdentifier: Option[String],
    dfasdl: DFASDL
) extends Actor
    with ActorLogging
    with BaseWriterFunctions
    with DocumentHelpers {

  val dfasdlTree: Document = createNormalizedDocument(dfasdl.content)
  lazy val uniqueElements: Set[Element] = {
    log.debug("Starting to collect unique data elements from dfasdl tree {}.", dfasdl.id)
    val t = dfasdlTree.asInstanceOf[DocumentTraversal]
    val it = t.createNodeIterator(dfasdlTree.getDocumentElement,
                                  NodeFilter.SHOW_ELEMENT,
                                  new DataElementFilter(),
                                  true)
    var ues  = Set.empty[Element]
    var node = it.nextNode()
    while (node != null) {
      val e = node.asInstanceOf[Element]
      if (isUniqueDataElement(e))
        ues = ues + e
      node = it.nextNode()
    }
    log.debug("Extracted {} unique data element ids from dfasdl {}.", ues.size, dfasdl.id)
    ues
  }
  lazy val uniqueIds: Set[String]                = uniqueElements.map(_.getAttribute("id"))
  lazy val uniqueSequences: Map[String, Element] = getParentSequences(uniqueElements)
  // A buffer for unfiltered data.
  var unfilteredDataBuffer: SortedSet[BaseWriterMessages.WriteData] =
    SortedSet.empty[BaseWriterMessages.WriteData]
  // Buffer workers.
  val workers = scala.collection.mutable.Map.empty[String, ActorRef]

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    if (unfilteredDataBuffer.nonEmpty)
      log.error("Unfiltered data buffer contains still {} entries that were not written!",
                unfilteredDataBuffer.size)

    super.postStop()
  }

  override def receive: Receive = {
    case Filter(data, target) =>
      log.debug("Trying to filter and write {} data messages.", data.size)
      val tmpData = unfilteredDataBuffer ++ data
      val dataToFilter =
        if (messagesMissing(tmpData)) {
          var num: Long = tmpData.headOption.map(_.number).getOrElse(-1L)
          tmpData.foreach(d => if (d.number == num + 1) num = d.number)
          tmpData.takeWhile(_.number <= num)
        } else
          tmpData
      unfilteredDataBuffer = tmpData diff dataToFilter

      if (uniqueIds.nonEmpty && dataToFilter.exists(d => uniqueIds.contains(d.metaData.get.id))) {
        val seqs: Set[Element] = dataToFilter
          .filter(d => uniqueIds.contains(d.metaData.get.id))
          .map(e => uniqueSequences(e.metaData.get.id))
        val seqColumnIds: Map[String, List[String]] = seqs
          .map(
            s =>
              s.getAttribute("id") -> getChildDataElementsFromElement(s).map(_.getAttribute("id"))
          )
          .toMap
        val seqData: Map[String, SortedSet[BaseWriterMessages.WriteData]] = seqColumnIds.map(
          sc => sc._1 -> dataToFilter.filter(m => sc._2.contains(m.metaData.get.id))
        )
        var dataToProcess: SortedSet[BaseWriterMessages.WriteData] =
          SortedSet.empty[BaseWriterMessages.WriteData]
        seqData.values.foreach(ds => dataToProcess = dataToProcess ++ ds)
        log.debug("Data to process contains {} entries.", dataToProcess.size) // DEBUG
        val noNeedToFilterData
          : SortedSet[BaseWriterMessages.WriteData] = dataToFilter diff dataToProcess
        log.debug("Data that will not be filtered contains {} entries.", noNeedToFilterData.size) // DEBUG
        target.getOrElse(sender()) ! BaseWriterMessages.WriteBatchData(noNeedToFilterData.toList)
        seqs.foreach { seq =>
          val id   = seq.getAttribute("id")
          val name = s"FilterWorker-$id"
          val worker = workers.getOrElse(
            id,
            context.actorOf(BaseWriterFilterWorker.props(agentRunIdentifier, dfasdl), name)
          )
          workers.put(id, worker)
          worker forward BaseWriterFilterWorker.FilterSequenceRows(seqData(id), seq, target)
        }
      } else
        target.getOrElse(sender()) ! BaseWriterMessages.WriteBatchData(dataToFilter.toList) // There is no need to filter the data.
    case BaseWriterMessages.CloseWriter =>
      log.debug("Received close writer request from {}.", sender().path)
      if (workers.isEmpty)
        context stop self
      else
        workers.values.foreach { w =>
          context watch w
          w ! BaseWriterMessages.CloseWriter
        }
    case Terminated(ref) =>
      log.debug("Received terminated message for {}.", ref.path)
      workers.find(_._2 == ref).foreach(p => workers.remove(p._1))
      if (workers.isEmpty)
        context stop self

  }

  /**
    * Collect all parent sequences from the given set of unique data elements.
    * The returned map uses the ids of the data elements as keys that point to their parent sequence.
    *
    * @param es A set of unique data elements.
    * @return A map holding the collected parent sequences.
    */
  private def getParentSequences(es: Set[Element]): Map[String, Element] = {
    log.debug(
      "Starting to collect parent sequences for {} unique data elements from dfasdl tree {}.",
      es.size,
      dfasdl.id
    )
    var seqs = Map.empty[String, Element]
    es.foreach(
      e =>
        getParentSequence(e).fold(
          log.error("No parent sequence found for element {} in dfasdl tree {}!",
                    e.getAttribute("id"),
                    dfasdl.id)
        )(s => seqs = seqs + (e.getAttribute("id") -> s))
    )
    log.debug("Collected {} parent sequences for {} unique data elements from dfasdl tree {}.",
              seqs.size,
              es.size,
              dfasdl.id)
    seqs
  }

}

object BaseWriterFilter {

  /**
    * A factory method to create the actor.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @param dfasdl The dfasdl describing the target database.
    * @return The props to create the actor.
    */
  def props(agentRunIdentifier: Option[String], dfasdl: DFASDL): Props =
    Props(classOf[BaseWriterFilter], agentRunIdentifier, dfasdl)

  /**
    * Instruct the actor to filter and write the given batch of data.
    *
    * @param d A sorted set of writer messages.
    * @param target An option to an actor ref that should receive the result of the filter operation. If empty, the sender will receive the filtered results.
    */
  final case class Filter(d: SortedSet[BaseWriterMessages.WriteData], target: Option[ActorRef])

}
