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

package com.wegtam.tensei.agent.parsers

import java.nio.charset.Charset
import java.time.{ OffsetDateTime, ZoneOffset }
import javax.xml.xpath.{ XPath, XPathConstants, XPathFactory }

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.event.{ DiagnosticLoggingAdapter, Logging }
import com.google.common.base.Charsets
import com.wegtam.tensei.adt.{ ConnectionInformation, Cookbook, DFASDLReference }
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.adt._
import com.wegtam.tensei.agent.helpers.{ DatabaseHelpers, LoggingHelpers }
import com.wegtam.tensei.agent.parsers.DatabaseParser.DatabaseParserCursorState
import org.dfasdl.utils.{ AttributeNames, DataElementType, ElementNames, StructureElementType }
import org.w3c.dom.{ Element, NodeList }

import scala.collection.mutable
import scalaz.Scalaz._
import scalaz._

object DatabaseParser {

  /**
    * Helper method to create an actor for database parsing.
    *
    * @param source              The source connection to retrieve the data from.
    * @param cookbook            The cookbook holding the source dfasdl.
    * @param dataTreeRef         The actor ref to the data tree e.g. where to put the parsed data.
    * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
    * @return The props to generate the actor.
    */
  def props(source: ConnectionInformation,
            cookbook: Cookbook,
            dataTreeRef: ActorRef,
            agentRunIdentifier: Option[String]): Props =
    Props(new DatabaseParser(source, cookbook, dataTreeRef, agentRunIdentifier))

  /**
    * A sealed trait for the different states of a cursor.
    */
  sealed trait DatabaseParserCursorState

  object DatabaseParserCursorState {
    final case class Active(cursor: java.sql.ResultSet) extends DatabaseParserCursorState

    case object Done extends DatabaseParserCursorState

    case object Uninitialized extends DatabaseParserCursorState
  }
}

/**
  * A simple database parser.
  *
  * @param source              The source connection to retrieve the data from.
  * @param cookbook            The cookbook holding the source dfasdl.
  * @param dataTreeRef         The actor ref to the data tree e.g. where to put the parsed data.
  * @param agentRunIdentifier  An optional agent run identifier which is usually an uuid.
  */
class DatabaseParser(source: ConnectionInformation,
                     cookbook: Cookbook,
                     dataTreeRef: ActorRef,
                     agentRunIdentifier: Option[String])
    extends Actor
    with ActorLogging
    with BaseParser
    with DatabaseHelpers {
  override val log
    : DiagnosticLoggingAdapter = Logging(this) // Override the standard logger to be able to add stuff via MDC.
  log.mdc(LoggingHelpers.generateMdcEntryForRunIdentifier(agentRunIdentifier))

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    connection match {
      case -\/(failure) => log.error(failure, "Could not close database connection!")
      case \/-(success) => success.close()
    }
    log.clearMDC()
  }

  // Try to establish a database connection.
  val connection = connect(source)
  val statement: Option[java.sql.Statement] = connection match {
    case -\/(failure) =>
      log.error(failure, "Could not create statement from db connection!")
      None
    case \/-(success) =>
      val stm = success.createStatement()
      log.debug("Created statement from db connection: {}", stm)
      Option(stm)
  }
  // Holds the database cursors for the sequences (e.g. the tables).
  val cursors: mutable.Map[String, DatabaseParserCursorState] =
    initializeCursorMap(cookbook, source.dfasdlRef.get)
  var defaultEncoding: Charset = Charsets.UTF_8

  override def receive: Receive = {
    case BaseParserMessages.SubParserInitialize =>
      sender() ! BaseParserMessages.SubParserInitialized
    case BaseParserMessages.Start =>
      log.debug("Starting database parser.")
      parseDb()
      sender() ! ParserStatusMessage(ParserStatus.COMPLETED, Option(context.self))
    case BaseParserMessages.Stop =>
      log.debug("Stopping database parser.")
      context stop self
    case BaseParserMessages.Status =>
      log.error("Status request not yet implemented!")
  }

  /**
    * If we reach a sequence and there exists no database cursor then we create one using a generated query
    * and store it in the cursors map.
    *
    * @param e The element discovered.
    */
  override def parserStructuralElementHandler(e: Element): Unit =
    if (StructureElementType.isSequence(getStructureElementType(e.getNodeName))) {
      val id = e.getAttribute("id")
      if (cursors
            .get(id)
            .isDefined && cursors(id) == DatabaseParserCursorState.Uninitialized && statement.isDefined) {
        generateSqlSelect(e) match {
          case Success(query) =>
            log.debug("Generated SQL for sequence {}: {}", id, query)
            val results = statement.get.executeQuery(query)
            if (results.next())
              cursors.update(id, DatabaseParserCursorState.Active(results))
            else
              cursors.update(id, DatabaseParserCursorState.Done) // The query returned no results, therefore we are done with it.
          case Failure(errors) =>
            log.error("SQL query could not be created: {}", errors.toList.mkString)
        }
      }
    }

  /**
    * We try to move the database cursor to next row. If the cursors returns `false` there is no more data
    * and we remove it from the map.
    *
    * @param s The element that describes the sequence.
    */
  override def parserFinishSequenceRowHandler(s: Element): Unit = {
    val id = s.getAttribute("id")
    if (cursors.get(id).isDefined) {
      log.debug("Checking cursor for sequence {}.", id)
      cursors(id) match {
        case DatabaseParserCursorState.Active(cursor) =>
          if (!cursor.next()) {
            log.debug("No more data from cursor after {} rows.", state.getSequenceRowCount(id))
            cursors.update(id, DatabaseParserCursorState.Done)
            log.info("Table '{}' has '{}' rows.", id, state.getSequenceRowCount(id)) // FIXME Logging for internal information (Protokollierung)
            val _ = state.resetSequenceRowCount(id)
          }
        case DatabaseParserCursorState.Done => log.debug("Cursor already done.")
        case DatabaseParserCursorState.Uninitialized =>
          log.warning("Cursor for sequence {} is uninitialized! This should never happen!", id)
      }
    }
  }

  override def save(data: ParserDataContainer,
                    dataHash: Long,
                    referenceId: Option[String] = None): Unit = {
    val sourceSequenceRow =
      if (state.isInSequence)
        Option(state.getCurrentSequenceRowCount)
      else
        None
    // Is the sequence `Done`? -> We do not have to save anymore!
    val sequenceDone: Boolean =
      if (state.isInSequence)
        cursors(state.getCurrentSequence.asInstanceOf[Element].getAttribute("id")) match {
          case DatabaseParserCursorState.Done =>
            true
          case _ =>
            false
        } else
        false
    if (!sequenceDone) {
      if (referenceId.isDefined) {
        dataTreeRef ! DataTreeDocumentMessages.SaveReferenceData(data,
                                                                 dataHash,
                                                                 referenceId.get,
                                                                 sourceSequenceRow)
      } else
        dataTreeRef ! DataTreeDocumentMessages.SaveData(data, dataHash)
    }
  }

  override def readDataElement(structureElement: Element,
                               useOffset: Long,
                               isInChoice: Boolean): BaseParserResponse = {
    val parentSequence = getParentSequence(structureElement)
    val results: Option[java.sql.ResultSet] =
      if (parentSequence.isDefined && cursors.get(parentSequence.get.getAttribute("id")).isDefined)
        cursors(parentSequence.get.getAttribute("id")) match {
          case DatabaseParserCursorState.Active(cursor) => Option(cursor)
          case DatabaseParserCursorState.Done =>
            log.debug("Database cursor already done for {} in {}.",
                      structureElement.getAttribute("id"),
                      parentSequence.get.getAttribute("id"))
            None
          case DatabaseParserCursorState.Uninitialized =>
            log.warning("Database cursor uninitialized for {} in {}!",
                        structureElement.getAttribute("id"),
                        parentSequence.get.getAttribute("id"))
            None
        } else
        None
    val response =
      if (results.isDefined) {
        // Get encoding or set it to default.
        val encoding = if (structureElement.hasAttribute(AttributeNames.ENCODING)) {
          Charset.forName(structureElement.getAttribute(AttributeNames.ENCODING))
        } else
          defaultEncoding
        val format: String =
          if (structureElement.hasAttribute(AttributeNames.FORMAT))
            structureElement.getAttribute(AttributeNames.FORMAT)
          else
            ""

        val columnName =
          if (structureElement.hasAttribute(AttributeNames.DB_COLUMN_NAME))
            structureElement.getAttribute(AttributeNames.DB_COLUMN_NAME)
          else
            structureElement.getAttribute("id")

        /**
          * If a formatted number has the attributes max-digits and max-precision
          * set to a value other than "0" then the JDBC driver can use a
          * `getBigDecimal` operation to receive the value. Otherwise the `getDouble`
          * method is used.
          *
          * @return Returns only `true` if the `getBigDecimal` method can be used to retrieve the element from JDBC.
          */
        def canUseFormatNumAsDecimalExtractor: Boolean = {
          val digits    = structureElement.getAttribute(AttributeNames.MAX_DIGITS)
          val precision = structureElement.getAttribute(AttributeNames.MAX_PRECISION)
          digits != null && digits != "0" && precision != null && precision != "0"
        }

        val rawStringRepresentation: String = structureElement.getTagName match {
          case ElementNames.NUMBER =>
            val c = results.get.getLong(columnName).toString
            // Extra check because "0" is returned if the column is 0 but also if it is NULL. :-(
            if (c == "0" && results.get.getString(columnName) == null)
              null
            else
              c
          case ElementNames.FORMATTED_NUMBER =>
            \/.fromTryCatch {
              if (canUseFormatNumAsDecimalExtractor) {
                val c = results.get.getBigDecimal(columnName)
                if (c != null)
                  c.toPlainString
                else
                  null
              } else {
                val c = results.get.getDouble(columnName).toString
                // Extra check because "0" is returned if the column is 0 but also if it is NULL. :-(
                if (c == "0" && results.get.getString(columnName) == null)
                  null
                else
                  c
              }
            } match {
              case -\/(failure) =>
                results.get.getString(columnName) // Fall back to string parsing because of errors which might result from fancy things like parsing a varchar column with a formatnum element.
              case \/-(success) => success
            }
          case ElementNames.DATETIME =>
            \/.fromTryCatch {
              OffsetDateTime
                .of(results.get.getTimestamp(columnName).toLocalDateTime, ZoneOffset.UTC)
                .toString
            } match {
              case -\/(failure) =>
                results.get.getString(columnName) // Fall back to string parsing because of errors which might result from fancy things like parsing a varchar column with a formatnum element.
              case \/-(success) => success
            }
          case _ => results.get.getString(columnName)
        }
        val bytes =
          if (rawStringRepresentation == null)
            null
          else
            rawStringRepresentation.getBytes(encoding)

        val data =
          if (bytes == null)
            None
          else {
            if (format.isEmpty)
              Option(new String(bytes, encoding))
            else {
              val response: Option[String] = getDataElementType(structureElement.getTagName) match {
                case DataElementType.BinaryDataElement => Option(new String(bytes, encoding))
                case DataElementType.StringDataElement =>
                  structureElement.getTagName match {
                    case ElementNames.FORMATTED_STRING | ElementNames.FORMATTED_NUMBER =>
                      val tmpString = new String(bytes, encoding)
                      val pattern   = s"(?s)$format".r
                      val m         = pattern.findFirstMatchIn(tmpString)
                      if (m.isDefined)
                        Option(m.get.group(1))
                      else {
                        log.warning("Could not apply format of element {}!",
                                    structureElement.getAttribute("id"))
                        log.debug("Element input was: {}", rawStringRepresentation)
                        None
                      }
                    case _ =>
                      Option(new String(bytes, encoding))
                  }
                case DataElementType.UnknownElement =>
                  throw new RuntimeException(
                    s"Unknown element ${structureElement.getTagName} (${structureElement.getAttribute("id")})!"
                  )
              }
              response
            }
          }
        log.debug("Parsed element {} with data: >{}<.", structureElement.getAttribute("id"), data)
        BaseParserResponse(data, DataElementType.StringDataElement)
      } else {
        val doneCursors = cursors.count(_._2 == DatabaseParserCursorState.Done)
        if (doneCursors == cursors.size)
          BaseParserResponse(data = None,
                             elementType = DataElementType.StringDataElement,
                             status = BaseParserResponseStatus.END_OF_DATA)
        else
          BaseParserResponse(data = None,
                             elementType = DataElementType.StringDataElement,
                             status = BaseParserResponseStatus.END_OF_SEQUENCE)
      }

    response
  }

  private def parseDb(): Unit =
    if (source.dfasdlRef.isDefined && cookbook.findDFASDL(source.dfasdlRef.get).isDefined) {
      val xml  = createNormalizedDocument(cookbook.findDFASDL(source.dfasdlRef.get).get.content)
      val root = xml.getDocumentElement
      if (root.hasAttribute(AttributeNames.DEFAULT_ENCODING))
        defaultEncoding = Charset.forName(root.getAttribute(AttributeNames.DEFAULT_ENCODING))
      else
        defaultEncoding = Charsets.UTF_8
      traverseTree(xml, log)
    } else
      log.error("No DFASDL defined for {} in cookbook {}", source.uri, cookbook.id)

  /**
    * Generates the SQL SELECT statement for the given `seq` or `fixseq` element.
    *
    * @param e A DFASDL element which must be a `seq` or a `fixseq`.
    * @return A validation holding the SQL string or an error message.
    */
  private def generateSqlSelect(e: Element): ValidationNel[String, String] =
    if (isStructuralElement(e.getNodeName) && StructureElementType.isSequence(
          getStructureElementType(e.getNodeName)
        )) {
      if (e.hasAttribute(AttributeNames.DB_SELECT) && e
            .getAttribute(AttributeNames.DB_SELECT)
            .length > 0)
        e.getAttribute(AttributeNames.DB_SELECT).successNel[String]
      else {
        // FIXME What do we do if we have stacked sequences?!? (see #T-126)
        val dataElements = getChildDataElementsFromElement(e)
        val sql = s"SELECT ${dataElements
          .map(
            e =>
              if (e.hasAttribute(AttributeNames.DB_COLUMN_NAME))
                e.getAttribute(AttributeNames.DB_COLUMN_NAME)
              else
                e.getAttribute("id")
          )
          .mkString(",")} FROM ${e.getAttribute("id")}"

        val sqlWhere =
          if (e.hasAttribute(AttributeNames.FILTER))
            s"$sql WHERE ${e.getAttribute(AttributeNames.FILTER)}"
          else
            sql

        if (getStructureElementType(e.getNodeName) == StructureElementType.FixedSequence)
          s"$sqlWhere LIMIT ${e.getAttribute(AttributeNames.FIXED_SEQUENCE_COUNT)}}"
            .successNel[String]
        else if (e.hasAttribute(AttributeNames.SEQUENCE_MAX))
          s"$sqlWhere LIMIT ${e.getAttribute(AttributeNames.SEQUENCE_MAX)}".successNel[String]
        else
          sqlWhere.successNel[String]
      }
    } else {
      s"Element ${e.getNodeName} is no sequence!".failNel[String]
    }

  /**
    * Find all sequences within the given DFASDL and prepare a "cursor map" for them.
    *
    * @param c A cookbook.
    * @param r A reference to a DFASDL.
    * @return A Map that has a key entry for each sequence.
    */
  private def initializeCursorMap(
      c: Cookbook,
      r: DFASDLReference
  ): mutable.Map[String, DatabaseParserCursorState] = {
    val whitelist = getSourceParentSequences(c, r)
    c.findDFASDL(r)
      .map { dfasdl =>
        val cursors      = new mutable.HashMap[String, DatabaseParserCursorState]()
        val xpath: XPath = XPathFactory.newInstance().newXPath()
        val xml          = createNormalizedDocument(dfasdl.content, useSchema = false) // Disable the schema to avoid confusing xpath.
        val tables = xpath
          .evaluate(
            s"/${ElementNames.ROOT}/${ElementNames.SEQUENCE} | /${ElementNames.ROOT}/${ElementNames.FIXED_SEQUENCE}",
            xml.getDocumentElement,
            XPathConstants.NODESET
          )
          .asInstanceOf[NodeList]

        if (tables.getLength == 0)
          log.warning("No table definitions found in DFASDL {}!", dfasdl.id)
        else {
          log.debug("Found {} table definitions in DFASDL {}.", tables.getLength, dfasdl.id)

          for (count <- 0 until tables.getLength) {
            val table = tables.item(count).asInstanceOf[Element]
            val id    = table.getAttribute("id")
            if (whitelist.contains(id))
              cursors.update(table.getAttribute("id"), DatabaseParserCursorState.Uninitialized)
            else {
              log.debug("Marking unused database table {} as done.", id)
              cursors.update(table.getAttribute("id"), DatabaseParserCursorState.Done)
            }
          }
        }

        cursors
      }
      .getOrElse(mutable.Map.empty[String, DatabaseParserCursorState])
  }
}
