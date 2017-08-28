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

import com.wegtam.tensei.adt.{ ConnectionInformation, DFASDL, ElementReference }
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages

import scala.collection.SortedSet
import scalaz._

object BaseWriter {
  // The default charset that will be used.
  val DEFAULT_CHARSET = "UTF-8"
  // The default stop-sign that will be used.
  val DEFAULT_STOP_SIGN = System.lineSeparator()
  // The name of the writer option that will ensure that no stop sign is written for the current writer message.
  val SKIP_STOP_SIGN_OPTION = "skip-stop-sign"

  /**
    * Contains meta data for the writer.
    *
    * @param id      The ID of the data element in the DFASDL.
    * @param charset The charset that should be used if the data is a string. In most cases this will be `UTF-8`.
    */
  final case class WriterMessageMetaData(id: String, charset: Option[String] = None)

  sealed trait BaseWriterMessages

  object BaseWriterMessages {
    case object AreYouReady extends BaseWriterMessages

    case object ReadyToWork extends BaseWriterMessages

    case object InitializeTarget extends BaseWriterMessages

    case object WriteBufferedData extends BaseWriterMessages

    case object CloseWriter extends BaseWriterMessages

    final case class WriterClosed(status: String \/ String) extends BaseWriterMessages

    /**
      * Holds data that is passed to a "writer".
      *
      * @param number    The number of the message. They are numbered upon create therefore they should be sortable and it is possible to check if messages are missing.
      * @param data      The actual data to be written.
      * @param options   A list of tuples that contains options for the writer.
      * @param metaData  Optional meta data for the writer.
      */
    final case class WriteData(
        number: Long,
        data: Any,
        options: List[(String, String)] = List(),
        metaData: Option[WriterMessageMetaData] = None
    ) extends Ordered[WriteData] {
      override def compare(that: WriteData): Int = number compare that.number
    }

    final case class WriteBatchData(batch: List[WriteData])

  }

  sealed trait State

  object State {
    case object Initializing extends State

    case object Working extends State

    case object Closing extends State
  }

}

trait BaseWriterFunctions {

  /**
    * Get all data values from the given messages that belong to unique data elements.
    *
    * @param dfasdl The dfasdl describing the data.
    * @param messages A set of writer messages holding the data.
    * @param uniqueElementIds A set of dfasdl element ids that point to unique data elements.
    * @return A map holding the values for each element reference of a unique data element.
    */
  def getUniqueMessageValues(dfasdl: DFASDL,
                             messages: SortedSet[BaseWriterMessages.WriteData],
                             uniqueElementIds: Set[String]): Map[ElementReference, Set[Any]] = {
    var buffer = Map.empty[ElementReference, Set[Any]]
    messages.foreach(
      m =>
        if (uniqueElementIds.contains(m.metaData.get.id)) {
          val r = ElementReference(dfasdlId = dfasdl.id, elementId = m.metaData.get.id)
          val e = buffer.getOrElse(r, Set.empty[Any]) + m.data
          buffer = buffer + (r -> e)
      }
    )
    buffer
  }

  /**
    * Check if messages are missing from the given sorted set of `WriteData` messages.
    * This function assumes that all messages include a properly generated message number.
    *
    * @param messages A list of messages.
    * @return Returns `true` if messages are missing and `false` otherwise.
    */
  def messagesMissing(messages: SortedSet[BaseWriterMessages.WriteData]): Boolean =
    messages.headOption.exists(
      head =>
        messages.lastOption.exists(last => (last.number - (head.number - 1)) != messages.size)
    )
}

/**
  * A base class for writers.
  *
  * @param target The connection information for the target data sink.
  */
abstract class BaseWriter(target: ConnectionInformation) {

  /**
    * Returns an option to the given parameter.
    *
    * @param name The name of the parameter.
    * @param options The list of parameters.
    * @return An option to the named parameter.
    */
  def getOption(name: String, options: List[(String, String)]): Option[String] = {
    val o = options.find(p => p._1 == name)
    if (o.isDefined)
      Option(o.get._2)
    else
      None
  }

  /**
    * Initialize the target.
    *
    * @return Returns `true` upon success and `false` if an error occurred.
    */
  def initializeTarget: Boolean

}
