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
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import com.wegtam.tensei.adt.GlobalMessages.{ ReportToCaller, ReportingTo }
import com.wegtam.tensei.adt.{ DFASDL, ElementReference }
import com.wegtam.tensei.agent.processor.UniqueValueBuffer
import com.wegtam.tensei.agent.processor.UniqueValueBuffer.UniqueValueBufferMessages
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages
import com.wegtam.tensei.agent.writers.BaseWriterFilterWorker.FilterSequenceRows
import org.dfasdl.utils.DocumentHelpers
import org.w3c.dom.Element

import scala.collection.immutable.SortedSet

/**
  * This actor filters writer messages regarding unique values for given sequences.
  *
  * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
  * @param dfasdl The dfasdl describing the target database.
  */
class BaseWriterFilterWorker(agentRunIdentifier: Option[String], dfasdl: DFASDL)
    extends Actor
    with ActorLogging
    with DocumentHelpers
    with BaseWriterFilterWorkerFunctions {
  // Create a distributed pub sub mediator.
  val mediator                            = DistributedPubSub(context.system).mediator
  var uniqueValueBuffer: Option[ActorRef] = None

  var columnIdsBuffer: List[String] = List.empty[String]
  var deduplicatedData: SortedSet[BaseWriterMessages.WriteData] =
    SortedSet.empty[BaseWriterMessages.WriteData]
  var filteredData: SortedSet[BaseWriterMessages.WriteData] =
    SortedSet.empty[BaseWriterMessages.WriteData]
  var receiver: Option[ActorRef] = None
  var requests: Set[UniqueValueBufferMessages.CheckIfValueExists] =
    Set.empty[UniqueValueBufferMessages.CheckIfValueExists]
  var rowsToRemove: SortedSet[Int] = SortedSet.empty[Int]
  var seqElement: Option[Element]  = None
  // A buffer for unfiltered data.
  var unfilteredDataBuffer: SortedSet[BaseWriterMessages.WriteData] =
    SortedSet.empty[BaseWriterMessages.WriteData]

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    mediator ! Publish(UniqueValueBuffer.UNIQUE_VALUE_BUFFER_CHANNEL, ReportToCaller)
    super.preStart()
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    if (unfilteredDataBuffer.nonEmpty)
      log.error("Unfiltered data buffer contains still {} entries that were not written!",
                unfilteredDataBuffer.size)

    uniqueValueBuffer.foreach(ref => context unwatch ref)
    super.postStop()
  }

  override def receive: Receive = {
    case ReportingTo(ref, _) =>
      log.debug("Received handshake from unique value buffer at {}.", ref.path)
      uniqueValueBuffer = Option(ref)
      val _ = context.watch(ref)

    case Terminated(ref) =>
      log.debug("Unique value buffer terminated at {}.", ref)
      uniqueValueBuffer = None
      mediator ! Publish(UniqueValueBuffer.UNIQUE_VALUE_BUFFER_CHANNEL, ReportToCaller)

    case FilterSequenceRows(data, sequence, target) =>
      log.debug("Received filter request for {} messages and sequence {}.",
                data.size,
                sequence.getAttribute("id"))
      val dataToFilter                  = unfilteredDataBuffer ++ data
      val columnElements: List[Element] = getChildDataElementsFromElement(sequence)
      val columnIds                     = columnElements.map(_.getAttribute("id"))
      val uniqueColumnIds: List[String] =
        columnElements.filter(e => isUniqueDataElement(e)).map(_.getAttribute("id"))
      if (uniqueColumnIds.isEmpty)
        target.getOrElse(sender()) ! BaseWriterMessages.WriteBatchData(dataToFilter.toList) // No need to filter anything.
      else {
        // Initialise state and continue.
        receiver = Option(target.getOrElse(sender()))
        columnIdsBuffer = columnIds
        seqElement = Option(sequence)
        deduplicatedData = removeDuplicateRows(dataToFilter, columnIds, uniqueColumnIds)
        log.debug("Deduplicated data contains {} entries.", deduplicatedData.size) // DEBUG
        val it: Iterator[BaseWriterMessages.WriteData] = deduplicatedData.iterator
          .sliding(columnIds.size, columnIds.size)
          .withPartial(false)
          .flatMap(row => row.filter(c => uniqueColumnIds.contains(c.metaData.get.id)))
        requests = it
          .map(
            c =>
              UniqueValueBufferMessages.CheckIfValueExists(
                ElementReference(dfasdlId = dfasdl.id, elementId = c.metaData.get.id),
                c.data
            )
          )
          .toSet
        if (requests.isEmpty)
          reportAndReset() // We don't need to query the unique value buffer.
        else {
          if (uniqueValueBuffer.isEmpty)
            log.warning("No reference found for unique value buffer! Using event channel.")
          // Send the messages for value checking either over the event channel or directly.
          uniqueValueBuffer.fold(
            requests.foreach(
              c => mediator ! Publish(UniqueValueBuffer.UNIQUE_VALUE_BUFFER_CHANNEL, c)
            )
          )(ref => requests.foreach(c => ref ! c))
        }
      }

    case UniqueValueBufferMessages.ValueDoesNotExist(ref, value) =>
      requests = requests - UniqueValueBufferMessages.CheckIfValueExists(ref, value) // Remove request
      if (requests.isEmpty)
        reportAndReset()

    case UniqueValueBufferMessages.ValueExists(ref, value) =>
      log.debug("A unique value for element {} already exists.", ref)
      findRowNumber(deduplicatedData, columnIdsBuffer, ref, value).fold(
        log.warning("Could not find row number for existing unique value of {}!", ref)
      )(n => rowsToRemove = rowsToRemove + n)
      requests = requests - UniqueValueBufferMessages.CheckIfValueExists(ref, value) // Remove request
      if (requests.isEmpty)
        reportAndReset()

    case BaseWriterMessages.CloseWriter =>
      log.debug("Received close writer request from {}.", sender().path)
      context stop self
  }

  /**
    * Report the filtered data and shutdown the actor.
    */
  private def reportAndReset(): Unit = {
    log.debug("Reporting results and resetting state.")
    filteredData = removeRowNumbers(deduplicatedData, columnIdsBuffer, rowsToRemove)
    // Now filter a maybe incomplete last row.
    var incompleteRow = SortedSet.empty[BaseWriterMessages.WriteData]
    if (columnIdsBuffer.nonEmpty) {
      val it =
        filteredData.iterator.sliding(columnIdsBuffer.size, columnIdsBuffer.size).withPartial(true)
      it.foreach(
        ds =>
          if (ds.size != columnIdsBuffer.size) ds.foreach(d => incompleteRow = incompleteRow + d)
      )
      val fd = filteredData diff incompleteRow
      unfilteredDataBuffer = incompleteRow
      receiver.fold(log.error("No receiver for filter unique value results!"))(
        r => r ! BaseWriterMessages.WriteBatchData(fd.toList)
      )
    }

    columnIdsBuffer = List.empty[String]
    deduplicatedData = SortedSet.empty[BaseWriterMessages.WriteData]
    filteredData = SortedSet.empty[BaseWriterMessages.WriteData]
    receiver = None
    requests = Set.empty[UniqueValueBufferMessages.CheckIfValueExists]
    rowsToRemove = SortedSet.empty[Int]
    seqElement = None
  }
}

/**
  * Functions put in a seperate trait to ease testing.
  */
trait BaseWriterFilterWorkerFunctions {

  /**
    * Return the row number that contains the specified element and value. It will report the first row number
    * that contains a matching element value!
    *
    * @param rows A sorted set of writer messages.
    * @param columnIds The ids of the column elements of the sequence.
    * @param ref An element reference for the element.
    * @param value The value of the element.
    * @return An option to the row number.
    */
  def findRowNumber(rows: SortedSet[BaseWriterMessages.WriteData],
                    columnIds: List[String],
                    ref: ElementReference,
                    value: Any): Option[Int] = {
    val it = rows.iterator.sliding(columnIds.size, columnIds.size).withPartial(true)
    it.zipWithIndex
      .find(p => p._1.exists(d => d.metaData.get.id == ref.elementId && d.data == value))
      .map(_._2)
  }

  /**
    * Return only the data rows from the given sorted set of writer messages that belong to the given list
    * of column ids.
    *
    * @param rows A sorted set of writer messages.
    * @param columnIds The ids of the column elements of the sequence.
    * @return A sorted set containing only data rows for the sequence columns.
    */
  def getSequenceRows(rows: SortedSet[BaseWriterMessages.WriteData],
                      columnIds: List[String]): SortedSet[BaseWriterMessages.WriteData] =
    rows.filter(m => columnIds.contains(m.metaData.get.id))

  /**
    * Remove all sequence rows from the given sorted set of writer messages.
    *
    * @param rows A sorted set of writer messages. <strong>Beware that these messages should only contain messages for the sequence!</strong>
    * @param columnIds The ids of the column elements of the sequence.
    * @param uniqueColumnIds The ids of the column elements that are unique columns.
    * @return A sorted set containing the deduplicated messages.
    */
  def removeDuplicateRows(
      rows: SortedSet[BaseWriterMessages.WriteData],
      columnIds: List[String],
      uniqueColumnIds: List[String]
  ): SortedSet[BaseWriterMessages.WriteData] = {
    var dedupedRows = SortedSet.empty[BaseWriterMessages.WriteData]
    val it          = rows.iterator.sliding(columnIds.size, columnIds.size).withPartial(true)
    it.foreach { row =>
      if (dedupedRows.isEmpty)
        dedupedRows = dedupedRows ++ row // Append current row because we have no previous values.
      else if (row.size < columnIds.size)
        dedupedRows = dedupedRows ++ row // Append current row because it has too few columns meaning it is a partial row at the end.
      else {
        val uniqueColumnValues = row.filter(c => uniqueColumnIds.contains(c.metaData.get.id))
        if (!uniqueColumnValues.exists(
              v =>
                dedupedRows.exists(c => c.metaData.get.id == v.metaData.get.id && c.data == v.data)
            ))
          dedupedRows = dedupedRows ++ row // Append current row because no unique column values matched the previous values.
      }
    }
    dedupedRows
  }

  /**
    * Remove the given "line numbers" from the provides sequence rows.
    *
    * @param rows A sorted set of writer messages. <strong>Beware that these messages should only contain messages for the sequence!</strong>
    * @param columnIds The ids of the column elements of the sequence.
    * @param removeLines A set of sequence line numbers. Note that the first line has to be 0.
    * @return A sorted set that does not contain the specified sequence line numbers.
    */
  def removeRowNumbers(rows: SortedSet[BaseWriterMessages.WriteData],
                       columnIds: List[String],
                       removeLines: SortedSet[Int]): SortedSet[BaseWriterMessages.WriteData] =
    if (removeLines.isEmpty)
      rows
    else {
      var cleanedRows = SortedSet.empty[BaseWriterMessages.WriteData]
      val it          = rows.iterator.sliding(columnIds.size, columnIds.size).withPartial(true)
      it.zipWithIndex.foreach(
        p => if (!removeLines.contains(p._2)) cleanedRows = cleanedRows ++ p._1
      )
      cleanedRows
    }

}

object BaseWriterFilterWorker {

  /**
    * A factory method to create the actor.
    *
    * @param agentRunIdentifier An optional agent run identifier which is usually an uuid.
    * @param dfasdl The dfasdl describing the target database.
    * @return The props to create the actor.
    */
  def props(agentRunIdentifier: Option[String], dfasdl: DFASDL): Props =
    Props(new BaseWriterFilterWorker(agentRunIdentifier, dfasdl))

  /**
    * Instruct the actor to filter the fiven data set for the given sequence.
    *
    * @param d A sorted set of writer messages.
    * @param s The dfasdl element describing the sequence.
    * @param target An option to an actor ref that should receive the result of the filter operation. If empty, the sender will receive the filtered results.
    */
  final case class FilterSequenceRows(
      d: SortedSet[BaseWriterMessages.WriteData],
      s: Element,
      target: Option[ActorRef]
  )

}
