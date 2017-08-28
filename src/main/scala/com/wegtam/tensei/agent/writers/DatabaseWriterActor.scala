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

import java.sql.{ PreparedStatement, Statement }
import java.util.Locale
import javax.xml.xpath.{ XPath, XPathConstants, XPathFactory }

import akka.actor._
import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import akka.util.ByteString
import com.wegtam.tensei.adt.{ ConnectionInformation, DFASDL, ElementReference }
import com.wegtam.tensei.agent.adt.TenseiForeignKeyValueType
import com.wegtam.tensei.agent.helpers._
import com.wegtam.tensei.agent.processor.{ AutoIncrementValueBuffer, UniqueValueBuffer }
import com.wegtam.tensei.agent.processor.AutoIncrementValueBuffer.{
  AutoIncrementValueBufferMessages,
  AutoIncrementValuePair
}
import com.wegtam.tensei.agent.processor.UniqueValueBuffer.UniqueValueBufferMessages
import com.wegtam.tensei.agent.writers.BaseWriter.BaseWriterMessages.{ AreYouReady, ReadyToWork }
import com.wegtam.tensei.agent.writers.BaseWriter.State.{ Closing, Initializing, Working }
import com.wegtam.tensei.agent.writers.BaseWriter._
import com.wegtam.tensei.agent.writers.DatabaseWriterActor.DatabaseWriterActorMessages.CloseResources
import com.wegtam.tensei.agent.writers.DatabaseWriterActor.{
  AutoIncrementValuePairBuffer,
  DatabaseQueryType,
  DatabaseSequenceBuffer,
  DatabaseWriterData
}
import org.dfasdl.utils.{ AttributeNames, DocumentHelpers, ElementNames }
import org.w3c.dom.{ Document, Element, NodeList }

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.collection.{ immutable, mutable }
import scala.concurrent.duration._
import scalaz._
import Scalaz._

/**
  * An actor that writes given informations into a database.
  * The dfasdl parameter is used to generate a database schema from it.
  *
  * @param target              The connection information for the target data sink.
  * @param dfasdl              The dfasdl describing the target database. It is needed to create tables and sql statements.
  * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
  */
class DatabaseWriterActor(
    target: ConnectionInformation,
    dfasdl: DFASDL,
    agentRunIdentifier: Option[String]
) extends BaseWriter(target = target)
    with Actor
    with FSM[BaseWriter.State, DatabaseWriterActor.DatabaseWriterData]
    with ActorLogging
    with BaseWriterFunctions
    with DatabaseWriterFunctions
    with DatabaseHelpers {
  // Create a distributed pub sub mediator.
  import DistributedPubSubMediator.Publish
  val mediator: ActorRef = DistributedPubSub(context.system).mediator

  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.clearMDC()
    cancelTimer("writeTrigger")
    super.postStop()
  }

  implicit val databaseDriver
    : SupportedDatabase = extractSupportedDatabaseFromUri(target.uri) match {
    case -\/(failure) => throw failure
    case \/-(success) => success
  }

  val dfasdlTree: Document = createNormalizedDocument(dfasdl.content)
  val defaultEncoding: String = {
    val root = dfasdlTree.getDocumentElement
    if (root.hasAttribute(AttributeNames.DEFAULT_ENCODING))
      root.getAttribute(AttributeNames.DEFAULT_ENCODING)
    else
      "UTF-8"
  }

  val writeTriggerInterval = FiniteDuration(
    context.system.settings.config
      .getDuration("tensei.agents.writers.database.write-interval", MILLISECONDS),
    MILLISECONDS
  )
  setTimer("writeTrigger",
           BaseWriterMessages.WriteBufferedData,
           writeTriggerInterval,
           repeat = true)

  val uniqueDataElementIds: Set[String] =
    getUniqueDataElements(dfasdlTree).map(_.getAttribute("id"))

  startWith(Initializing, DatabaseWriterData())

  when(Initializing) {
    case Event(BaseWriterMessages.InitializeTarget, data) =>
      connect(target) match {
        case -\/(failure) =>
          log.error(failure, "Could not connect to database!")
          goto(Closing) using DatabaseWriterData()
        case \/-(success) =>
          goto(Working) using data.copy(connection = Option(success),
                                        sequenceBuffer = createSchemaAndStatements(success))
      }
    case Event(AreYouReady, data) =>
      stay() using data.copy(readyRequests = sender() :: data.readyRequests)
  }

  when(Working) {
    case Event(msg: BaseWriterMessages.WriteData, data) =>
      log.debug("Got write request.")
      val messageBuffer = data.messages + msg
      stay() using data.copy(messages = messageBuffer)
    case Event(msg: BaseWriterMessages.WriteBatchData, data) =>
      log.debug("Got bulk write request containing {} messages.", msg.batch.size)
      val messageBuffer = data.messages ++ msg.batch
      stay() using data.copy(messages = messageBuffer)
    case Event(BaseWriterMessages.WriteBufferedData, data) =>
      log.debug("Got WriteData request.")
      val unwrittenMessages = writeData(connection = data.connection,
                                        messages = data.messages,
                                        sequenceBuffer = data.sequenceBuffer)
      stay() using data.copy(messages = unwrittenMessages)
    case Event(BaseWriterMessages.CloseWriter, data) =>
      log.debug("Got CloseWriter request.")
      // We only try to write data, if the `data.messages` is not empty!
      val unwrittenMessages =
        if (data.messages.isEmpty)
          data.messages
        else
          writeData(connection = data.connection,
                    messages = data.messages,
                    sequenceBuffer = data.sequenceBuffer)
      if (unwrittenMessages.nonEmpty)
        log.warning("Could not write {} messages upon close request!", unwrittenMessages.size)
      self ! CloseResources
      goto(Closing) using data.copy(messages = unwrittenMessages,
                                    closeRequester = Option(sender()))
    case Event(AreYouReady, data) =>
      sender() ! ReadyToWork
      stay() using data
  }

  when(Closing) {
    case Event(CloseResources, data) =>
      data.connection.foreach(c => c.close())
      if (data.closeRequester.isDefined)
        data.closeRequester.get ! BaseWriterMessages.WriterClosed("".right[String])
      stay() using data
  }

  onTransition {
    case _ -> Working => nextStateData.readyRequests foreach (a => a ! ReadyToWork)
    case _ -> Closing => cancelTimer("writeTrigger") // Avoid unhandled timer messages.
  }

  initialize()

  /**
    * Write the list of messages to the database.
    * If there is data that can't be written (We need complete sql statements!) then it is returned as list.
    *
    * @param connection      An option to the database connection.
    * @param messages        A list of writer messages e.g. data to be written.
    * @param sequenceBuffer  The buffer holding the statement stuff.
    * @return A list of messages e.g. data that couldn't be written.
    */
  @tailrec
  private def writeData(
      connection: Option[java.sql.Connection],
      messages: immutable.SortedSet[BaseWriterMessages.WriteData],
      sequenceBuffer: Map[String, DatabaseSequenceBuffer]
  ): immutable.SortedSet[BaseWriterMessages.WriteData] =
    if (messages.headOption.isEmpty)
      messages
    else {
      val firstElementId  = messages.head.metaData.get.id
      val firstSequenceId = sequenceBuffer.find(_._2.columnIds.contains(firstElementId)).get._1

      if (firstElementId != sequenceBuffer(firstSequenceId).columnIds.head) {
        log.warning(
          "Retrieved unordered data. The first element id '{}' should also be the first data column in insert statement for '{}'!",
          firstElementId,
          firstSequenceId
        )
      }

      //      if (messagesMissing(messages)) {
      //        log.error("Writer is missing messages!")
      //        messages
      //      }
      //      else {
      writeSequence(connection, firstSequenceId, messages, sequenceBuffer) match {
        case -\/(failure) =>
          log.debug("Unable to write complete message batch. {} messages left.", failure.size)
          failure
        case \/-(success) =>
          if (success.nonEmpty) {
            log.debug("{} messages not written.", success.size)
            if (success.size == messages.size) {
              log.error("No messages could be written!")
              success
            } else
              writeData(connection, success, sequenceBuffer)
          } else
            success
      }
      //      }
    }

  /**
    * Write as much data from the given sequence as possible and return the still unwritten messages.
    *
    * @param connection      An option to the database connection.
    * @param sequenceId      The ID of the sequence.
    * @param messages        A list of writer messages e.g. data to be written.
    * @param sequenceBuffer  The buffer holding the statement stuff.
    * @return A validation holding a list of unwritten messages.
    */
  def writeSequence(
      connection: Option[java.sql.Connection],
      sequenceId: String,
      messages: immutable.SortedSet[BaseWriterMessages.WriteData],
      sequenceBuffer: Map[String, DatabaseSequenceBuffer]
  ): immutable.SortedSet[BaseWriterMessages.WriteData] \/ immutable.SortedSet[
    BaseWriterMessages.WriteData
  ] = {
    val sequenceColumnIds = sequenceBuffer(sequenceId).columnIds

    if (messages.size < sequenceColumnIds.size) {
      log.debug("Not enough data collected to write sequence row. Got {} but need {}.",
                messages.size,
                sequenceColumnIds.size)
      log.debug("Needed column ids: {}", sequenceColumnIds.sorted.mkString(", "))
      log.debug("Got column ids: {}",
                messages.map(_.metaData.fold("NO METADATA!")(_.id)).toList.sorted.mkString(", "))
      messages.left
    } else {
      connection
        .map { c =>
          // If we have primary key columns we need to check if the entry exists.
          // We build a list of query types for each row.
          val queryTypesToUse: List[DatabaseQueryType] =
            if (sequenceBuffer(sequenceId).primaryKeyColumnNames.nonEmpty) {
              if (sequenceBuffer(sequenceId).primaryKeyColumnIds.nonEmpty && sequenceBuffer(
                    sequenceId
                  ).updateStatement.isDefined && sequenceBuffer(sequenceId).countStatement.isDefined) {
                val uniqueColumnIds =
                  sequenceColumnIds.filter(c => uniqueDataElementIds.contains(c))
                val primaryKeyColumnIds = sequenceBuffer(sequenceId).primaryKeyColumnIds
                val check               = sequenceBuffer(sequenceId).countStatement.get
                // Slide through the column data ignoring a maybe incomplete last row.
                messages.iterator
                  .sliding(sequenceColumnIds.size, sequenceColumnIds.size)
                  .withPartial(false)
                  .map { columnData =>
                    if (uniqueColumnIds.isEmpty) {
                      checkIfPrimaryKeyExists(columnData, primaryKeyColumnIds, check)
                    } else {
                      val columnSpecificChecks: Seq[DatabaseQueryType] =
                        checkIfUniqueColumnsExist(columnData,
                                                  uniqueColumnIds,
                                                  sequenceBuffer(sequenceId).uniqueSelects).map(
                          r =>
                            if (r.exists) {
                              // Publish the unique value to the event channel because it already exists in the database.
                              val eRef =
                                ElementReference(dfasdlId = dfasdl.id, elementId = r.elementId)
                              mediator ! Publish(UniqueValueBuffer.UNIQUE_VALUE_BUFFER_CHANNEL,
                                                 UniqueValueBufferMessages.Store(eRef,
                                                                                 r.elementValue))
                              // Publish the fetched primary key value to the autoincrementer event channel.
                              r.relatedPrimaryKeyValue.foreach {
                                pkValue =>
                                  val pkRef =
                                    ElementReference(dfasdlId = dfasdl.id,
                                                     elementId = primaryKeyColumnIds.head)
                                  val oldValue: TenseiForeignKeyValueType = r.elementValue match {
                                    case d: ByteString =>
                                      TenseiForeignKeyValueType.FkString(Option(d.utf8String))
                                    case d: java.sql.Date =>
                                      TenseiForeignKeyValueType.FkDate(Option(d))
                                    case d: Long   => TenseiForeignKeyValueType.FkLong(Option(d))
                                    case d: String => TenseiForeignKeyValueType.FkString(Option(d))
                                  }
                                  val newValue = TenseiForeignKeyValueType.FkLong(Option(pkValue))
                                  mediator ! Publish(
                                    AutoIncrementValueBuffer.AUTO_INCREMENT_BUFFER_CHANNEL,
                                    AutoIncrementValueBufferMessages.Store(
                                      pkRef,
                                      Vector(AutoIncrementValuePair(oldValue, newValue))
                                    )
                                  )
                              }
                              DatabaseQueryType.Ignore
                            } else
                              DatabaseQueryType.Insert
                        )
                      if (columnSpecificChecks.contains(DatabaseQueryType.Ignore))
                        DatabaseQueryType.Ignore // At least one unique column was present in the database.
                      else
                        checkIfPrimaryKeyExists(columnData, primaryKeyColumnIds, check) // The primary key must be checked because we're about to insert the row.
                    }
                  }
                  .toList
              } else {
                log.warning(
                  "Primary key defined but no key columns ids, update or count query for {}! This may lead to data loss!",
                  sequenceId
                )
                messages.iterator
                  .sliding(sequenceColumnIds.size, sequenceColumnIds.size)
                  .withPartial(false)
                  .map(m => DatabaseQueryType.Insert)
                  .toList
              }
            } else
              messages.iterator
                .sliding(sequenceColumnIds.size, sequenceColumnIds.size)
                .withPartial(false)
                .map(m => DatabaseQueryType.Insert)
                .toList

          c.setAutoCommit(false) // Disable auto-commit for performance reasons.

          log.debug(
            "Write batch counts {} rows (including {} updates).",
            queryTypesToUse.size,
            queryTypesToUse.count(_ == DatabaseQueryType.Update)
          ) // FIXME Logging for internal information (Protokollierung)

          // Slide through the messages including a potential incomplete last row.
          // The data is zipped with it's index to be able to extract the proper
          // query type from `queryTypesToUse`.

          // We need to buffer the values separated by insert and update. Update values should actually never change!
          val autoIncrementBuffer: scala.collection.mutable.Map[DatabaseQueryType, ListBuffer[
            AutoIncrementValuePairBuffer
          ]] = scala.collection.mutable.Map
            .empty[DatabaseQueryType, ListBuffer[AutoIncrementValuePairBuffer]]

          // Just take the messages with IDs that are within the sequenceColumnIds list
          val dataToProcess =
            messages.takeWhile(p => sequenceColumnIds.contains(p.metaData.get.id))
          val unprocessedMessages = try {
            val uniqueValueBuffer = scala.collection.mutable.Map.empty[ElementReference, Set[Any]]
            val unwrittenMessages = dataToProcess.iterator
              .sliding(sequenceColumnIds.size, sequenceColumnIds.size)
              .withPartial(true)
              .zipWithIndex
              .map { pair =>
                val writeCandidates = pair._1
                val rowIndex        = pair._2

                if (writeCandidates.size == sequenceColumnIds.size) {
                  val primaryKeyColumnIds    = sequenceBuffer(sequenceId).primaryKeyColumnIds
                  val autoIncrementColumnIds = sequenceBuffer(sequenceId).autoIncrementColumnIds
                  // We check if the "sequence row" of unwritten messages is complete.
                  // First we get all needed column ids from the "sequence row" of messages to be written.
                  val orderedWriteCandidates = sequenceColumnIds.map { id =>
                    val candidate = writeCandidates.find(_.metaData.get.id == id)
                    if (candidate.isEmpty)
                      log.error("Could not find data message for column {} in sequence {}!",
                                id,
                                sequenceId)
                    candidate
                  }
                  if (orderedWriteCandidates.contains(None)) {
                    log.error("Data writer messages out of sync with expected data columns!")
                    writeCandidates.toSet
                  } else {
                    // Store our values into the buffer.
                    val b =
                      autoIncrementBuffer.getOrElse(queryTypesToUse(rowIndex),
                                                    new ListBuffer[AutoIncrementValuePairBuffer])
                    orderedWriteCandidates
                      .map(_.get)
                      .filter(m => autoIncrementColumnIds.contains(m.metaData.get.id))
                      .foreach { a =>
                        val r =
                          ElementReference(dfasdlId = dfasdl.id, elementId = a.metaData.get.id)
                        if (a.data != None) {
                          a.data match {
                            case d: java.sql.Date =>
                              b += AutoIncrementValuePairBuffer(r,
                                                                TenseiForeignKeyValueType
                                                                  .FkDate(Option(d)))
                            case d: Long =>
                              b += AutoIncrementValuePairBuffer(r,
                                                                TenseiForeignKeyValueType
                                                                  .FkLong(Option(d)))
                            case d: String =>
                              b += AutoIncrementValuePairBuffer(r,
                                                                TenseiForeignKeyValueType
                                                                  .FkString(Option(d)))
                          }
                        } else {
                          // Find the first data column within our sequence that has the unique attribute set.
                          val relatedColumnId: Option[String] = sequenceColumnIds.find(
                            id => isUniqueDataElement(dfasdlTree.getElementById(id))
                          )
                          relatedColumnId.fold(
                            log.error(
                              "No related unique column id found for auto increment element {}!",
                              r
                            )
                          )(
                            id =>
                              orderedWriteCandidates
                                .map(_.get)
                                .find(_.metaData.get.id == id)
                                .fold(
                                  log.error("No related unique column data found for element {}!",
                                            r)
                                )(
                                  a =>
                                    a.data match {
                                      case d: ByteString =>
                                        val _ = b += AutoIncrementValuePairBuffer(
                                          r,
                                          TenseiForeignKeyValueType.FkString(Option(d.utf8String))
                                        )
                                      case d: java.sql.Date =>
                                        val _ = b += AutoIncrementValuePairBuffer(
                                          r,
                                          TenseiForeignKeyValueType
                                            .FkDate(Option(d))
                                        )
                                      case d: Long =>
                                        val _ = b += AutoIncrementValuePairBuffer(
                                          r,
                                          TenseiForeignKeyValueType
                                            .FkLong(Option(d))
                                        )
                                      case d: String =>
                                        val _ = b += AutoIncrementValuePairBuffer(
                                          r,
                                          TenseiForeignKeyValueType
                                            .FkString(Option(d))
                                        )
                                  }
                              )
                          )
                        }
                      }
                    autoIncrementBuffer.put(queryTypesToUse(rowIndex), b)

                    queryTypesToUse(rowIndex) match {
                      case DatabaseQueryType.Ignore =>
                        log.debug("Ignoring complete sequence row for write operation: {}",
                                  orderedWriteCandidates)

                      case DatabaseQueryType.Insert =>
                        val statement: PreparedStatement =
                          sequenceBuffer(sequenceId).insertStatement
                        // Remove auto-increment column elements from the insert statement.
                        // This will break oracle databases <12c!
                        val messagesToBeWritten = orderedWriteCandidates
                          .map(_.get)
                          .filterNot(m => autoIncrementColumnIds.contains(m.metaData.get.id))
                        // Set the column data for the prepared statement.
                        messagesToBeWritten.zipWithIndex.foreach { entry =>
                          val index   = entry._2 + 1 // The jdbc column index starts at "1".
                          val message = entry._1

                          setStatementParameter(statement, index, message)
                          // Buffer the value of a unique element.
                          if (uniqueDataElementIds.contains(message.metaData.get.id)) {
                            val r = ElementReference(dfasdlId = dfasdl.id,
                                                     elementId = message.metaData.get.id)
                            val vs: Set[Any] = uniqueValueBuffer.getOrElse(r, Set.empty[Any])
                            uniqueValueBuffer.put(r, vs + message.data)
                          }
                        }
                        // Add the parameters to a batch run.
                        statement.addBatch()

                      case DatabaseQueryType.Update =>
                        val statement: PreparedStatement =
                          sequenceBuffer(sequenceId).updateStatement.getOrElse(
                            throw new RuntimeException("No update statement defined!")
                          )
                        val messagesToBeWritten = databaseDriver match {
                          case Derby | Firebird | HyperSql | SqlServer =>
                            orderedWriteCandidates
                              .map(_.get)
                              .filterNot(m => primaryKeyColumnIds.contains(m.metaData.get.id))
                          case _ => orderedWriteCandidates.map(_.get)
                        }

                        // Set the column data for the prepared statement.
                        messagesToBeWritten.zipWithIndex.foreach { entry =>
                          val index   = entry._2 + 1 // The jdbc column index starts at "1".
                          val message = entry._1

                          setStatementParameter(statement, index, message)
                          // Buffer the value of a unique element.
                          if (uniqueDataElementIds.contains(message.metaData.get.id)) {
                            val r = ElementReference(dfasdlId = dfasdl.id,
                                                     elementId = message.metaData.get.id)
                            val vs: Set[Any] = uniqueValueBuffer.getOrElse(r, Set.empty[Any])
                            uniqueValueBuffer.put(r, vs + message.data)
                          }
                        }
                        // If we are using an update query we must use the primary key columns for the where clause.
                        val cols = orderedWriteCandidates
                          .map(_.get)
                          .filter(e => primaryKeyColumnIds.contains(e.metaData.get.id))
                        val startIndex = messagesToBeWritten.size + 1 // The number of already added columns.
                        cols.zipWithIndex.foreach { colEntry =>
                          val colData  = colEntry._1
                          val colIndex = colEntry._2 + startIndex // Add the already added number of columns.
                          setStatementParameter(statement, colIndex, colData)
                        }

                        // Add the parameters to a batch run.
                        statement.addBatch()
                    }
                    // TODO
                    Set.empty[BaseWriterMessages.WriteData]
                  }
                } else
                  writeCandidates.toSet // Not enough data to fill a sequence row.
              }
              .toSet
              .flatten

            // Execute the batches and determine the generated auto-increment values.
            val generatedInsertKey: Option[Long] =
              if (queryTypesToUse.contains(DatabaseQueryType.Insert)) {
                val is = sequenceBuffer(sequenceId).insertStatement.executeBatch()
                is.zipWithIndex.foreach(
                  n =>
                    n._1 match {
                      case Statement.EXECUTE_FAILED =>
                        log.error("Execution of insert statement {} in batch failed!", n._2)
                      case _ => // The statement was successful.
                  }
                )
                if (databaseDriver != SqlServer && databaseDriver != Firebird) {
                  val ks = sequenceBuffer(sequenceId).insertStatement.getGeneratedKeys
                  if (ks != null && ks.next()) {
                    databaseDriver match {
                      case PostgreSql =>
                        // PostgreSQL returns all columns.
                        sequenceBuffer(sequenceId).autoIncrementColumnNames.headOption
                          .map { c =>
                            var k: Long = ks.getLong(c)
                            while (ks.next()) {
                              k = ks.getLong(c)
                            }
                            Option(k)
                          }
                          .getOrElse(None)
                      case Oracle =>
                        None // FIXME There is a way to circumvent the oracle issues but it will break stuff for all other databases!
                      case _ =>
                        var k: Long = ks.getLong(1)
                        while (ks.next()) {
                          k = ks.getLong(1)
                        }
                        Option(k)
                    }
                  } else
                    None
                } else {
                  sequenceBuffer(sequenceId).maxStatement
                    .map { s =>
                      val r = s.executeQuery()
                      if (r != null && r.next())
                        Option(r.getLong(1))
                      else
                        None
                    }
                    .getOrElse(None)
                }
              } else
                None
            val generatedUpdateKey: Option[Long] =
              if (queryTypesToUse.contains(DatabaseQueryType.Update)) {
                val us = sequenceBuffer(sequenceId).updateStatement.get.executeBatch()
                us.zipWithIndex.foreach(
                  n =>
                    n._1 match {
                      case Statement.EXECUTE_FAILED =>
                        log.error("Execution of update statement {} in batch failed!", n._2)
                      case _ => // The statement was successful.
                  }
                )
                if (databaseDriver != SqlServer) {
                  val ks = sequenceBuffer(sequenceId).updateStatement.get.getGeneratedKeys
                  if (ks != null && ks.next()) {
                    databaseDriver match {
                      case PostgreSql =>
                        // PostgreSQL returns all columns.
                        sequenceBuffer(sequenceId).autoIncrementColumnNames.headOption
                          .map { c =>
                            var k: Long = ks.getLong(c)
                            while (ks.next()) {
                              k = ks.getLong(c)
                            }
                            Option(k)
                          }
                          .getOrElse(None)
                      case Oracle =>
                        None // Oracle does not support the return of auto generated keys on statements other than INSERT.
                      case _ =>
                        var k: Long = ks.getLong(1)
                        while (ks.next()) {
                          k = ks.getLong(1)
                        }
                        Option(k)
                    }
                  } else
                    None
                } else {
                  sequenceBuffer(sequenceId).maxStatement
                    .map { s =>
                      val r = s.executeQuery()
                      if (r != null && r.next())
                        Option(r.getLong(1))
                      else
                        None
                    }
                    .getOrElse(None)
                }
              } else
                None

            generatedInsertKey.foreach { k =>
              log.debug("GENERATED INSERT KEY: {}", k)
              val b = autoIncrementBuffer(DatabaseQueryType.Insert)
              // The following is needed because hsqldb starts auto increment values at 0 per default.
              val sizeCheck: Int = if (databaseDriver != HyperSql) b.size else b.size - 1
              if (b.nonEmpty && sizeCheck <= k) {
                // We only do this if we have values at all and the number of inserts is equal of less than the retrieved last auto-increment value.
                val autoIncrements = for (c <- k until (k - b.size) by -1) yield c
                val newBuffer = b.reverse zip autoIncrements map (
                    p => p._1.copy(newValue = Option(p._2))
                )
                val ref    = newBuffer.head.r
                val values = Vector.newBuilder[AutoIncrementValuePair]
                newBuffer.foreach(
                  v =>
                    v.newValue.map(
                      nv =>
                        values += AutoIncrementValuePair(oldValue = v.oldValue,
                                                         newValue = TenseiForeignKeyValueType
                                                           .FkLong(Option(nv)))
                  )
                )
                val msg = AutoIncrementValueBufferMessages.Store(
                  ref = ref,
                  values = values.result()
                )
                mediator ! Publish(AutoIncrementValueBuffer.AUTO_INCREMENT_BUFFER_CHANNEL, msg) // Publish the results to the event channel.
              }
            }
            generatedUpdateKey.foreach { k =>
              log.debug("GENERATED UPDATE KEY: {}", k)
              val b = autoIncrementBuffer(DatabaseQueryType.Update)
              if (b.nonEmpty) {
                // We only do this if we have values at all.
                val ref    = b.head.r
                val values = Vector.newBuilder[AutoIncrementValuePair]
                b.foreach(
                  v =>
                    v.newValue.map(
                      nv =>
                        values += AutoIncrementValuePair(oldValue = v.oldValue,
                                                         newValue = TenseiForeignKeyValueType
                                                           .FkLong(Option(nv)))
                  )
                )
                val msg = AutoIncrementValueBufferMessages.Store(
                  ref = ref,
                  values = values.result()
                )
                mediator ! Publish(AutoIncrementValueBuffer.AUTO_INCREMENT_BUFFER_CHANNEL, msg) // Publish the results to the event channel.
              }
            }

            c.commit() // Finish the transaction.

            uniqueValueBuffer.foreach { p =>
              val r: ElementReference = p._1
              val vs: Set[Any]        = p._2
              log.debug(s"BUFFERED UNIQUE VALUES: $r -> $vs")
              mediator ! Publish(
                UniqueValueBuffer.UNIQUE_VALUE_BUFFER_CHANNEL,
                UniqueValueBufferMessages.StoreS(r, vs)
              ) // Publish the stored unique values to the event channel.
            }

            unwrittenMessages
          } catch {
            case e: java.sql.SQLException =>
              log.error(e, s"SQL error in sequence '$sequenceId'!")
              var ne = e.getNextException
              while (ne != null) {
                log.error(ne, "Next nested JDBC exception.")
                ne = ne.getNextException
              }
              log.error("Rolling back transaction.")
              c.rollback() // Rollback transaction...

              throw new RuntimeException(
                s"SQL exception while writing sequence '$sequenceId'! See log for details."
              )
          }
          // Drop the processed messages from the list of total messages and add the
          // remaining messages to the sortedUnwrittenMessages variable
          val nMessages = messages.drop(dataToProcess.size)

          // TODO Check the response and return a message list that includes also the failed messages.
          c.setAutoCommit(true)
          val sortedUnwrittenMessages = immutable
            .SortedSet[BaseWriterMessages.WriteData]() ++ unprocessedMessages ++ nMessages
          sortedUnwrittenMessages.right
        }
        .getOrElse(messages.left)
    }
  }

  /**
    * Use the given data columns and the prepared statement to check if a
    * primary key already exists in the database.
    *
    * Depending on it's existence the function returns either an `Insert`
    * or an `Update` query type.
    *
    * @param columnData The data columns for a table row.
    * @param primaryKeyColumnIds The ids of the dfasdl elements that are the primary keys of the table.
    * @param statement A prepared statement that must return a `COUNT` and provide parameters for all primary key columns.
    * @return If the key does not exist or the statement returns no result then a [[DatabaseQueryType.Insert]] is returned, otherwise a [[DatabaseQueryType.Update]].
    */
  private def checkIfPrimaryKeyExists(columnData: Seq[BaseWriterMessages.WriteData],
                                      primaryKeyColumnIds: Seq[String],
                                      statement: PreparedStatement): DatabaseQueryType = {
    // Find the columns that belong to the primary key.
    val cols = columnData.filter(e => primaryKeyColumnIds.contains(e.metaData.get.id))
    cols.zipWithIndex.foreach { entry =>
      val msg = entry._1
      val idx = entry._2 + 1 // JDBC column index starts at `1`!
      setStatementParameter(statement, idx, msg)
    }
    val result = statement.executeQuery()
    if (result.next() && result.getInt(1) > 0)
      DatabaseQueryType.Update
    else
      DatabaseQueryType.Insert
  }

  /**
    * Check if the unique column data given already exists in the database table
    * using the provided prepared statement.
    *
    * @param columnData A writer message containing the needed data and meta data.
    * @param statement A prepared statement the checks the data and returns either a count or a related primary key value.
    * @return An instance of [[UniqueColumnExistCheckResult]] that holds the appropriate data and flags.
    */
  private def checkIfUniqueColumnExists(
      columnData: BaseWriterMessages.WriteData,
      statement: PreparedStatement
  ): UniqueColumnExistCheckResult = {
    val checkResult = UniqueColumnExistCheckResult(
      elementId = columnData.metaData.get.id,
      elementValue = columnData.data,
      exists = false,
      relatedPrimaryKeyValue = None
    )
    setStatementParameter(statement, 1, columnData)
    val result = statement.executeQuery()
    if (result.next()) {
      if (result.getMetaData.getColumnName(1) == DatabaseWriterActor.UNIQUE_COLUMN_CHECK_NAME) {
        // The query did not return primary key data.
        checkResult.copy(exists = true)
      } else {
        // The query returned the matching primary key data of the queried table.
        if (result.getMetaData.getColumnCount > 1) {
          // Multiple column primary keys are not supported for possible foreign key relations.
          log.debug("Primary key for existing unique column {} has more than one column!",
                    columnData.metaData.get.id)
          checkResult.copy(exists = true)
        } else {
          // Return the primary key value for the existing unique column.
          val pkValue: Option[Long] =
            \/.fromTryCatch(result.getLong(1)) match {
              case -\/(failure) =>
                log.debug("Unable to extract long value from primary key column: {}",
                          failure.getMessage)
                None
              case \/-(success) =>
                Option(success) // TODO For values of `NULL` the jdbc method returns `0`, but `0` might be a valid primary key value!
            }
          checkResult.copy(exists = true, relatedPrimaryKeyValue = pkValue)
        }
      }
    } else
      checkResult
  }

  /**
    * Check the unique columns in the given table row of data for existing values.
    *
    * @param columnData The data for a database table row.
    * @param uniqueColumnIds The ids of the dfasdl elements describing the unique columns.
    * @param uniqueSelects A map holding prepared statements for checking the unique values mapped to their element ids.
    * @return A list of [[UniqueColumnExistCheckResult]] that hold the gathered results.
    */
  private def checkIfUniqueColumnsExist(
      columnData: Seq[BaseWriterMessages.WriteData],
      uniqueColumnIds: Seq[String],
      uniqueSelects: Map[String, PreparedStatement]
  ): Seq[UniqueColumnExistCheckResult] =
    uniqueColumnIds.map(
      id =>
        columnData
          .find(_.metaData.get.id == id)
          .fold {
            // TODO Check if it may lead to problems up the chain if we return a value of `None` for missing data!
            log.debug("No data for unique column check of element {}!", id)
            UniqueColumnExistCheckResult(elementId = id,
                                         elementValue = None,
                                         exists = false,
                                         relatedPrimaryKeyValue = None)
          }(d => checkIfUniqueColumnExists(columnData = d, statement = uniqueSelects(id)))
    )

  /**
    * Creates the database schema and returns a map that holds the insert statements and the column data for each sequence.
    *
    * @param connection The database connection.
    * @return A map (sequence id -> statement + column ids).
    */
  private def createSchemaAndStatements(
      connection: java.sql.Connection
  ): Map[String, DatabaseSequenceBuffer] = {
    val statements: mutable.Map[String, DatabaseSequenceBuffer] =
      new mutable.HashMap[String, DatabaseSequenceBuffer]()

    try {
      val statement     = connection.createStatement()
      val xpath: XPath  = XPathFactory.newInstance().newXPath()
      val xml           = createNormalizedDocument(dfasdl.content, useSchema = false) // Disable the schema to avoid confusing xpath.
      val xmlWithSchema = createNormalizedDocument(dfasdl.content)
      val nodes = xpath
        .evaluate(
          s"/${ElementNames.ROOT}/${ElementNames.SEQUENCE} | /${ElementNames.ROOT}/${ElementNames.FIXED_SEQUENCE}",
          xml.getDocumentElement,
          XPathConstants.NODESET
        )
        .asInstanceOf[NodeList]
      val allTables = for (idx <- 0 until nodes.getLength)
        yield nodes.item(idx).asInstanceOf[Element]
      val tables = sortTables(allTables)(xmlWithSchema)

      if (tables.isEmpty)
        log.warning("No table definitions found in DFASDL {}!", dfasdl.id)
      else
        log.debug("Found {} table definitions in DFASDL {}.", tables.length, dfasdl.id)

      val driver: SupportedDatabase = extractSupportedDatabaseFromUri(target.uri) match {
        case -\/(failure) => throw failure
        case \/-(success) => success
      }
      val databaseName = extractDatabaseNameFromURI(target.uri) match {
        case -\/(failure) => throw failure
        case \/-(success) => success
      }

      tables.foreach { table =>
        val tableName = table.getAttribute("id")
        val columns   = getChildDataElementsFromElement(table)

        // Activate foreign keys for SQLite, if possible
        if (databaseDriver == SQLite) {
          val r = statement.executeQuery("PRAGMA foreign_keys")
          // If the query did not return an Integer, the SQLite version does not support foreign keys
          if (r != null) {
            val status = r.getInt(1)
            // Activate foreign keys for SQLite
            if (status == 0) {
              log.debug("Activate Foreign Keys in SQLite.")
              statement.execute("PRAGMA foreign_keys = ON;")
            } else
              log.debug("Foreign Keys already activated for SQLite.")
          } else
            log.error("This version of SQLite does not support foreign keys!")
        }

        val foreignKeyStatements =
          if (databaseDriver != SQLite)
            createForeignKeyStatements(table)(xmlWithSchema)
          else
            Seq.empty[String]
        val primaryKeyColumnNames: List[String] =
          if (table.hasAttribute(AttributeNames.DB_PRIMARY_KEY))
            table.getAttribute(AttributeNames.DB_PRIMARY_KEY).split(",").toList
          else
            List.empty[String]
        // Find the column ids from the given column names.
        val primaryKeyColumnIds =
          if (primaryKeyColumnNames.nonEmpty) {
            primaryKeyColumnNames.map { name =>
              columns
                .find(
                  e =>
                    if (e.hasAttribute(AttributeNames.DB_COLUMN_NAME))
                      e.getAttribute(AttributeNames.DB_COLUMN_NAME) == name
                    else
                      e.getAttribute("id") == name
                )
                .get
                .getAttribute("id")
            }
          } else
            List.empty[String]
        val autoIncrementColumnIds = columns
          .filter(
            e =>
              e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e
                .getAttribute(AttributeNames.DB_AUTO_INCREMENT) == "true"
          )
          .map(_.getAttribute("id"))
        val autoIncrementColumnNames = columns
          .filter(
            e =>
              e.hasAttribute(AttributeNames.DB_AUTO_INCREMENT) && e
                .getAttribute(AttributeNames.DB_AUTO_INCREMENT) == "true"
          )
          .map(
            e =>
              if (e.hasAttribute(AttributeNames.DB_COLUMN_NAME))
                e.getAttribute(AttributeNames.DB_COLUMN_NAME)
              else e.getAttribute("id")
          )
        val uniqueTableColumns = columns.filter(e => isUniqueDataElement(e))

        val createColumns = columns map { c =>
          val columnName =
            if (c.hasAttribute(AttributeNames.DB_COLUMN_NAME))
              c.getAttribute(AttributeNames.DB_COLUMN_NAME)
            else
              c.getAttribute("id")
          val unique =
            if (isUniqueDataElement(c))
              " UNIQUE "
            else
              ""
          if (primaryKeyColumnNames.contains(columnName))
            s"$columnName ${getDatabaseColumnType(c, driver, defaultEncoding)} $unique NOT NULL"
          else
            s"$columnName ${getDatabaseColumnType(c, driver, defaultEncoding)} $unique"
        }

        // Some databases should also check for the database name when checking the existence
        // of a table.
        val metaResultsStatement = driver match {
          case Derby =>
            s"SELECT TABLENAME AS TABLE_NAME FROM SYS.SYSTABLES WHERE TABLETYPE='T' AND UPPER(TABLENAME) = UPPER('$tableName')"
          case Firebird =>
            s"SELECT rdb$$relation_name AS TABLE_NAME FROM rdb$$relations WHERE rdb$$view_blr IS NULL AND(rdb$$system_flag IS NULL OR rdb$$system_flag = 0) AND UPPER(rdb$$relation_name) = UPPER('$tableName')"
          case H2 =>
            s"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME) = UPPER('$tableName')"
          case HyperSql =>
            s"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME) = UPPER('$tableName')"
          case MariaDb =>
            s"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER('$databaseName') AND UPPER(TABLE_NAME) = UPPER('$tableName')"
          case MySql =>
            s"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER('$databaseName') AND UPPER(TABLE_NAME) = UPPER('$tableName')"
          case Oracle =>
            s"SELECT table_name FROM all_tables WHERE UPPER(table_name) = UPPER('$tableName')"
          case PostgreSql =>
            s"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME) = UPPER('$tableName')"
          case SQLite =>
            s"SELECT name AS TABLE_NAME FROM sqlite_master WHERE type = 'table' AND UPPER(name) = UPPER('$tableName')"
          case SqlServer =>
            s"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_CATALOG) = UPPER('$databaseName') AND UPPER(TABLE_NAME) = UPPER('$tableName')"
          case _ =>
            s"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME) = UPPER('$tableName')"
        }

        val metaResults = statement.executeQuery(metaResultsStatement)
        if (metaResults.next()) {
          log.warning("Table '{}' already exists in database!", tableName)
          // FIXME We should check if the columns for the table are correct and bail out if they are not!
        } else {
          val primaryKeyString =
            if (primaryKeyColumnNames.nonEmpty)
              if (databaseDriver == SQLite && autoIncrementColumnNames.nonEmpty)
                ""
              else
                s"""
                     |,
                     |PRIMARY KEY (${primaryKeyColumnNames.mkString(", ")})""".stripMargin
            else
              ""
          val foreignKeyString =
            if (databaseDriver == SQLite) {
              val fk = createForeignKeyStringSQLite(table)(xmlWithSchema)
              if (fk.nonEmpty)
                s"""
                     |,
                     |${fk.mkString(",")}""".stripMargin
              else
                ""
            } else
              ""
          val createTable =
            s"""
                 |CREATE TABLE $tableName (
                 |  ${createColumns.mkString(", ")}$primaryKeyString$foreignKeyString
                 |)
             """.stripMargin.trim
          log.info("Creating table {}.", tableName)
          log.debug("Executing sql statement: {}", createTable)
          try {
            statement.execute(createTable)
            if (foreignKeyStatements.nonEmpty) {
              log.info("Trying to create foreign keys for table {}.", tableName)
              foreignKeyStatements.foreach(fstm => statement.execute(fstm))
            }
          } catch {
            case sqlException: java.sql.SQLException =>
              log.error(sqlException, "SQL statement execution failed!")
              throw new RuntimeException("Unable to create table!", sqlException) // Escalate the error!
          }
        }

        // Create Generators and Triggers for Auto-Increment columns
        if (databaseDriver == Firebird && autoIncrementColumnNames.nonEmpty) {
          val results = statement.executeQuery(
            "SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS WHERE RDB$SYSTEM_FLAG=0;"
          )
          val existingSequences = new ListBuffer[String]
          while (results.next()) {
            existingSequences += results.getString(1).trim
          }
          results.close()

          val resultsTriggers = statement.executeQuery(
            s"SELECT rdb$$trigger_name FROM RDB$$TRIGGERS WHERE RDB$$SYSTEM_FLAG = 0 AND RDB$$RELATION_NAME='${tableName
              .toUpperCase(Locale.ROOT)}';"
          )
          val existingTriggers = new ListBuffer[String]
          while (resultsTriggers.next()) {
            existingTriggers += resultsTriggers.getString(1).trim
          }
          resultsTriggers.close()

          autoIncrementColumnNames.foreach(column => {
            val sequenceName = s"SEQUENCE_${tableName.toUpperCase(Locale.ROOT)}_WORKER"
            val triggerName  = s"TRIGGER_${tableName.toUpperCase(Locale.ROOT)}_WORKER"
            if (!existingSequences.contains(sequenceName) && !existingTriggers
                  .contains(triggerName)) {
              val sequence_create = s"CREATE SEQUENCE $sequenceName"
              val sequence_set    = s"ALTER SEQUENCE $sequenceName RESTART WITH 0"
              val sequence_trigger =
                s"""
                   |CREATE TRIGGER $triggerName FOR ${tableName.toUpperCase(Locale.ROOT)}
                   |ACTIVE BEFORE INSERT POSITION 0
                   |AS
                   |BEGIN
                   |  IF ((new.${column.toLowerCase(Locale.ROOT)} is null) or (new.${column
                     .toLowerCase(Locale.ROOT)} = 0)) then
                   |  begin
                   |    new.${column.toLowerCase(Locale.ROOT)} = gen_id($sequenceName , 1 );
                   |  end
                   |end
                                     """.stripMargin
              statement.execute(sequence_create)
              statement.execute(sequence_set)
              statement.execute(sequence_trigger)
            }
          })
        }

        val columnNames = columns map (
            c =>
              if (c.hasAttribute(AttributeNames.DB_COLUMN_NAME))
                c.getAttribute(AttributeNames.DB_COLUMN_NAME)
              else
                c.getAttribute("id")
        )

        val insertTableRow =
          if (table.hasAttribute(AttributeNames.DB_INSERT))
            table.getAttribute(AttributeNames.DB_INSERT)
          else {
            // We remove all auto-increment columns from the statement.
            // This may break oracle databases <12c!
            val autoIncrementColumns     = autoIncrementColumnNames
            val keyColumns: List[String] = autoIncrementColumns.distinct
            createInsertStatement(tableName, columnNames.diff(keyColumns))
          }

        log.debug("Creating INSERT statement: {}", insertTableRow)
        val preparedInsertStatement: PreparedStatement =
          if (databaseDriver != Firebird)
            connection.prepareStatement(insertTableRow, Statement.RETURN_GENERATED_KEYS)
          else
            connection.prepareStatement(insertTableRow, Statement.NO_GENERATED_KEYS)
        val preparedUpdateStatement: Option[PreparedStatement] =
          if (table.hasAttribute(AttributeNames.DB_UPDATE)) {
            val s = table.getAttribute(AttributeNames.DB_UPDATE)
            if (databaseDriver != Firebird)
              Option(connection.prepareStatement(s, Statement.RETURN_GENERATED_KEYS))
            else
              Option(connection.prepareStatement(s, Statement.NO_GENERATED_KEYS))
          } else {
            if (primaryKeyColumnNames.nonEmpty) {
              val s = driver match {
                case Derby | Firebird | HyperSql | SqlServer =>
                  val autoIncrementColumns = autoIncrementColumnNames
                  val keyColumns: List[String] =
                    (autoIncrementColumns ::: primaryKeyColumnNames).distinct
                  createUpdateStatement(tableName,
                                        columnNames.diff(keyColumns),
                                        primaryKeyColumnNames)
                case _ =>
                  createUpdateStatement(tableName, columnNames, primaryKeyColumnNames)
              }
              log.debug("Creating UPDATE statement: {}", s)
              if (databaseDriver != Firebird)
                Option(connection.prepareStatement(s, Statement.RETURN_GENERATED_KEYS))
              else
                Option(connection.prepareStatement(s, Statement.NO_GENERATED_KEYS))
            } else
              None
          }
        val preparedCountStatement: Option[PreparedStatement] =
          if (preparedUpdateStatement.isDefined) {
            val s = createCountStatement(tableName, primaryKeyColumnNames)
            log.debug("Creating COUNT statement: {}", s)
            Option(connection.prepareStatement(s))
          } else
            None
        val preparedMaxStatement: Option[PreparedStatement] =
          if (autoIncrementColumnNames.nonEmpty)
            Option(
              connection
                .prepareStatement(s"SELECT MAX(${autoIncrementColumnNames.head}) FROM $tableName")
            )
          else
            None

        val uniqueSelects: Map[String, PreparedStatement] =
          if (uniqueTableColumns.nonEmpty) {
            if (primaryKeyColumnNames.isEmpty) {
              log.info(
                "{} unique columns defined in table {} without primary key column. This may lead to unresolveable foreign key relations!",
                uniqueTableColumns.size,
                tableName
              )
              uniqueTableColumns
                .map(
                  c =>
                    c.getAttribute("id") -> connection.prepareStatement(
                      s"SELECT 1 AS ${DatabaseWriterActor.UNIQUE_COLUMN_CHECK_NAME} FROM $tableName WHERE ${getDatabaseColumnName(c)} = ?"
                  )
                )
                .toMap
            } else
              uniqueTableColumns
                .map(
                  c =>
                    c.getAttribute("id") -> connection.prepareStatement(
                      s"SELECT ${primaryKeyColumnNames.mkString(", ")} FROM $tableName WHERE ${getDatabaseColumnName(c)} = ?"
                  )
                )
                .toMap
          } else
            Map.empty[String, PreparedStatement]

        statements.put(
          table.getAttribute("id"),
          DatabaseSequenceBuffer(
            columnIds = columns.map(_.getAttribute("id")),
            insertStatement = preparedInsertStatement,
            updateStatement = preparedUpdateStatement,
            countStatement = preparedCountStatement,
            maxStatement = preparedMaxStatement,
            primaryKeyColumnIds = primaryKeyColumnIds,
            primaryKeyColumnNames = primaryKeyColumnNames,
            autoIncrementColumnIds = autoIncrementColumnIds,
            autoIncrementColumnNames = autoIncrementColumnNames,
            uniqueSelects = uniqueSelects
          )
        )
      }
    } catch {
      case e: Throwable =>
        log.error(e, "An error occurred while creating database schema and prepared statements.")
    }

    statements.toMap
  }

  /**
    * This function is meaningless for us therefore it returns always `true`.
    *
    * @return Returns `true` upon success and `false` if an error occurred.
    */
  override def initializeTarget: Boolean =
    true
}

/**
  * A trait that holds several functions for the database writer.
  *
  */
trait DatabaseWriterFunctions extends XmlHelpers with DocumentHelpers {

  /**
    * Create all the ALTER TABLE statements that are needed to add defined
    * foreign keys to a database table.
    *
    * @param table The element that describes the database table.
    * @param d The document containing the DFASDL description of the database.
    * @return A list of sql statements that may be empty.
    */
  def createForeignKeyStatements(table: Element)(d: Document): Seq[String] = {
    val tableName = table.getAttribute("id")
    val columns   = getChildDataElementsFromElement(table)
    val keys =
      columns.flatMap(c => if (c.hasAttribute(AttributeNames.DB_FOREIGN_KEY)) Option(c) else None)
    keys map { k =>
      val columnName =
        if (k.hasAttribute(AttributeNames.DB_COLUMN_NAME))
          k.getAttribute(AttributeNames.DB_COLUMN_NAME)
        else
          k.getAttribute("id")
      val referencedColumns = k
        .getAttribute(AttributeNames.DB_FOREIGN_KEY)
        .split(",")
        .map(_.trim)
        .map(id => d.getElementById(id))
      val keyColumns = referencedColumns.map(
        c =>
          if (c.hasAttribute(AttributeNames.DB_COLUMN_NAME))
            c.getAttribute(AttributeNames.DB_COLUMN_NAME)
          else
            c.getAttribute("id")
      )
      val referencedTable = getParentSequence(referencedColumns.head).get
      val keyTableName    = referencedTable.getAttribute("id")

      s"""ALTER TABLE $tableName ADD FOREIGN KEY ($columnName) REFERENCES $keyTableName(${keyColumns
        .mkString(", ")})"""
    }
  }

  /**
    * Create the ForeignKey definition strings for SQLite which must be added to the CREATE TABLE statement.
    *
    * @param table The element that describes the database table.
    * @param d The document containing the DFASDL description of the database.
    * @return A list of sql statements that may be empty.
    */
  def createForeignKeyStringSQLite(table: Element)(d: Document): Seq[String] = {
    val columns = getChildDataElementsFromElement(table)
    val keys =
      columns.flatMap(c => if (c.hasAttribute(AttributeNames.DB_FOREIGN_KEY)) Option(c) else None)
    keys map { k =>
      val columnName =
        if (k.hasAttribute(AttributeNames.DB_COLUMN_NAME))
          k.getAttribute(AttributeNames.DB_COLUMN_NAME)
        else
          k.getAttribute("id")
      val referencedColumns = k
        .getAttribute(AttributeNames.DB_FOREIGN_KEY)
        .split(",")
        .map(_.trim)
        .map(id => d.getElementById(id))
      val keyColumns = referencedColumns.map(
        c =>
          if (c.hasAttribute(AttributeNames.DB_COLUMN_NAME))
            c.getAttribute(AttributeNames.DB_COLUMN_NAME)
          else
            c.getAttribute("id")
      )
      val referencedTable = getParentSequence(referencedColumns.head).get
      val keyTableName    = referencedTable.getAttribute("id")

      s"""FOREIGN KEY($columnName) REFERENCES $keyTableName(${keyColumns.mkString(", ")})"""
    }
  }

  /**
    * Sort the given list of tables regarding their foreign key relationships.
    *
    * @param ts A list of elements that each describe a table e.g. seq and fixseq elements.
    * @param d The document of the DFASDL that contains all the given elements.
    * @return A sorted list of tables.
    */
  def sortTables(ts: Seq[Element])(d: Document): Seq[Element] =
    if (ts.isEmpty)
      ts
    else {
      val sortedTables = new ListBuffer[Element]
      sortedTables ++= ts

      val foreignKeyElements = ts.flatMap(
        t =>
          getChildDataElementsFromElement(t)
            .map(c => if (c.hasAttribute(AttributeNames.DB_FOREIGN_KEY)) Option(c) else None)
            .filter(_.isDefined)
            .map(_.get)
      )
      foreignKeyElements.foreach { fe =>
        val refs = fe
          .getAttribute(AttributeNames.DB_FOREIGN_KEY)
          .split(",")
          .map(_.trim)
          .distinct
          .map(id => d.getElementById(id))
        val allRefTables  = refs.flatMap(r => getParentSequence(r))
        val dedupedTables = new ListBuffer[Element]
        allRefTables.foreach(
          t =>
            if (!dedupedTables.exists(_.getAttribute("id") == t.getAttribute("id")))
              dedupedTables += t
        )
        val refTables = dedupedTables.result()
        getParentSequence(fe).foreach(
          t =>
            refTables.foreach(
              rt =>
                if (getIndexOf(rt)(sortedTables) > getIndexOf(t)(sortedTables)) {
                  if (sortedTables.head == t) {
                    // Special case: If the right element is the head of the list we just need to prepend the left to the cleaned up list.
                    sortedTables.remove(getIndexOf(rt)(sortedTables))
                    sortedTables.prepend(rt)
                  } else {
                    // Split the list at the index of the right element.
                    val s     = sortedTables.splitAt(getIndexOf(t)(sortedTables))
                    val left  = s._1
                    val right = s._2
                    // Remove the left element.
                    right.remove(getIndexOf(rt)(right))
                    // Clear our buffer and construct the new sorted list.
                    sortedTables.clear()
                    sortedTables ++= left
                    sortedTables += rt
                    sortedTables ++= right
                  }
              }
          )
        )
      }

      sortedTables.result()
    }

  /**
    * Return the index of the element within the given sequence.
    *
    * @param e An element.
    * @param l A list of elements.
    * @return The index of the element or `-1` if doesn't exist within the sequence.
    */
  def getIndexOf(e: Element)(l: Seq[Element]): Int =
    l.zipWithIndex
      .find(p => p._1.getAttribute("id") == e.getAttribute("id"))
      .map(_._2)
      .getOrElse(-1)

}

object DatabaseWriterActor {
  val UNIQUE_COLUMN_CHECK_NAME = "TENSEI_COUNTER_CHECK"

  /**
    * Helper method to create a database writer actor.
    *
    * @param target              The connection information for the target data sink.
    * @param dfasdl              The dfasdl describing the target database. It is needed to create tables and sql statements.
    * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
    * @return The props to generate the actor.
    */
  def props(target: ConnectionInformation,
            dfasdl: DFASDL,
            agentRunIdentifier: Option[String]): Props =
    Props(classOf[DatabaseWriterActor], target, dfasdl, agentRunIdentifier)

  sealed trait DatabaseWriterActorMessages

  object DatabaseWriterActorMessages {
    case object CloseResources extends DatabaseWriterActorMessages
  }

  /**
    * A helper trait for choosing the correct database query type.
    */
  sealed trait DatabaseQueryType

  /**
    * A companion object that holds all the trait's members.
    */
  object DatabaseQueryType {

    /**
      * No data modification is executed. The data (row) is ignored by the writer.
      */
    case object Ignore extends DatabaseQueryType

    /**
      * A SQL INSERT query.
      */
    case object Insert extends DatabaseQueryType

    /**
      * A SQL UPDATE query.
      */
    case object Update extends DatabaseQueryType

  }

  /**
    * Holds the list of column element ids and the insert statement for the table (sequence).
    *
    * @param columnIds             A list of the ids of the columns (data elements).
    * @param insertStatement       The SQL statement that should be used to insert data into the database.
    * @param updateStatement       An option to an update statement that should be used to update data in the database.
    * @param countStatement        An option to a count statement that should be used to check if an entry already exists in a database table.
    * @param maxStatement          An option to a statement that selects the maximum from the auto-increment column.
    * @param primaryKeyColumnIds   A list of column ids that belong to the primary key. This list is empty by default.
    * @param primaryKeyColumnNames A list of column names that belong to the primary key. This list is empty by default.
    * @param autoIncrementColumnIds A list of column ids that are auto-increment columns in the database.
    * @param autoIncrementColumnNames A list of the actual column names that are auto-increment columns.
    * @param uniqueSelects A map holding a select statement for each unique column of the table. The statement returns either the value of the primary key columns or a placeholder if no primary key is defined on the table.
    */
  final case class DatabaseSequenceBuffer(
      columnIds: List[String],
      insertStatement: PreparedStatement,
      updateStatement: Option[PreparedStatement],
      countStatement: Option[PreparedStatement],
      maxStatement: Option[PreparedStatement],
      primaryKeyColumnIds: List[String],
      primaryKeyColumnNames: List[String],
      autoIncrementColumnIds: List[String],
      autoIncrementColumnNames: List[String],
      uniqueSelects: Map[String, PreparedStatement]
  )

  /**
    * Our own FSM state.
    *
    * @param connection      An option to the database connection.
    * @param messages        The message buffer with the already received writer messages.
    * @param sequenceBuffer  Buffer for the sql insert statements (sequence id -> statement and column ids)
    * @param closeRequester  An option to the actor ref that requested the closing of the writer.
    * @param readyRequests   A list of actor refs that have asked if we are ready to work.
    */
  final case class DatabaseWriterData(
      connection: Option[java.sql.Connection] = None,
      messages: immutable.SortedSet[BaseWriterMessages.WriteData] =
        immutable.SortedSet.empty[BaseWriterMessages.WriteData],
      sequenceBuffer: Map[String, DatabaseSequenceBuffer] =
        HashMap[String, DatabaseSequenceBuffer](),
      closeRequester: Option[ActorRef] = None,
      readyRequests: List[ActorRef] = List.empty[ActorRef]
  )

  final case class AutoIncrementValuePairBuffer(
      r: ElementReference,
      oldValue: TenseiForeignKeyValueType,
      newValue: Option[Long] = None
  )

}

/**
  * The result of a check if a given unique column value does already exist
  * in the database table.
  *
  * @param elementId The id of the dfasdl element describing the unique column.
  * @param elementValue The actual data value that was used to check.
  * @param exists A flag that indicates if the value does already exist (`true`) or not (`false`).
  * @param relatedPrimaryKeyValue An optional long value of a related primary key column.
  */
final case class UniqueColumnExistCheckResult(
    elementId: String,
    elementValue: Any,
    exists: Boolean,
    relatedPrimaryKeyValue: Option[Long]
)
