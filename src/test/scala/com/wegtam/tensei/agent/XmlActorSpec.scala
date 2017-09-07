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

import java.io.{ File, InputStream, StringReader }
import java.net.URI
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.{ XPathConstants, XPathFactory }

import akka.actor.ActorRef
import akka.testkit.{ ImplicitSender, TestActorRef, TestFSMRef }
import akka.util.ByteString
import com.wegtam.tensei.adt.Recipe.{ MapAllToAll, RecipeMode }
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages
import com.wegtam.tensei.agent.DataTreeDocument.DataTreeDocumentMessages.ReturnXmlStructure
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.Parser.ParserMessages.StartParsing
import com.wegtam.tensei.agent.Parser.{ ParserCompletedStatus, ParserMessages }
import com.wegtam.tensei.agent.Processor.ProcessorMessages.StartProcessingMessage
import com.wegtam.tensei.agent.adt._
import com.wegtam.tensei.agent.helpers.GenericHelpers
import org.dfasdl.utils._
import org.scalatest.{ BeforeAndAfterAll, FunSpecLike, Matchers }
import org.w3c.dom.{ Document, Element, NodeList }
import org.xml.sax.InputSource

import scala.concurrent.duration._

abstract class XmlActorSpec
    extends ActorSpec
    with ImplicitSender
    with FunSpecLike
    with Matchers
    with BeforeAndAfterAll
    with DocumentHelpers
    with XmlTestHelpers
    with GenericHelpers {

  /**
    * Helper function to compare the given data.
    *
    * @param actual Data to test.
    * @param expected The string representation of the given data must match the expected given here.
    */
  private def compareData(actual: Any, expected: String): Unit = {
    val _ = actual match {
      case bs: ByteString => bs.utf8String should be(expected)
      case _              => actual.toString should be(expected)
    }
  }

  /**
    * Compare the stored data for the specified choice in the data tree with the one
    * in the given expected data tree. The choice has to be the direct child of a sequence.
    *
    * @param sequenceId       The ID of the sequence that holds the choice.
    * @param expectedDataTree An xml tree holding the expected data.
    * @param dataTree         The actual data tree that holds the parsed data.
    */
  def compareChoiceInSequence(sequenceId: String,
                              expectedDataTree: Document,
                              dataTree: ActorRef): Unit = {
    val choiceSeq    = expectedDataTree.getElementById(sequenceId)
    var child        = choiceSeq.getFirstChild
    var childCounter = 0L
    while (child.getNextSibling != null) {
      if (child.getNodeName == ElementNames.CHOICE) {
        val dataElements = getChildDataElementsFromElement(child.asInstanceOf[Element])
        dataElements foreach { e =>
          val eId = e.getAttribute("class").substring(3)
          withClue(s"Data element $eId should be stored correctly!") {
            dataTree ! DataTreeDocumentMessages.ReturnData(eId, Option(childCounter))
            val d = expectMsgType[DataTreeNodeMessages.Content]
            withClue("Data tree should return exactly one cell!")(d.data.size should be(1))
            d.data.head.elementId should be(eId)
            if (e.getTextContent == "None")
              d.data.head.data should be(None)
            else
              compareData(d.data.head.data, e.getTextContent)
          }
        }
        childCounter += 1
      }
      if (child.getNextSibling != null)
        child = child.getNextSibling
    }
  }

  /**
    * Traverse through the given expected data tree and compare each data node with the corresponding
    * data node from the data tree.
    *
    * @param expectedDataTree An xml tree holding the expected data.
    * @param dataTree         The actual data tree that holds the parsed data.
    */
  def compareSimpleDataNodes(expectedDataTree: Document, dataTree: ActorRef): Unit = {
    val xpath = XPathFactory.newInstance().newXPath()
    val candidates =
      xpath.evaluate("//*[@id]", expectedDataTree, XPathConstants.NODESET).asInstanceOf[NodeList]
    if (candidates != null && candidates.getLength > 0) {
      val nodes = Vector.newBuilder[Element]
      var i     = 0
      while (i < candidates.getLength) {
        val node = candidates.item(i)
        if (isDataElement(node.getNodeName) && getParentSequence(node).isEmpty)
          nodes += node
            .asInstanceOf[Element] // Buffer all data nodes that are not the child of a sequence.
        i += 1
      }
      nodes.result().foreach { n =>
        val elementId = n.getAttribute("id")
        if (elementId == null || elementId.isEmpty) fail("ID Attribute empty!")
        dataTree ! DataTreeDocumentMessages.ReturnData(elementId)
        val data = expectMsgType[DataTreeNodeMessages.Content]
        withClue("A simple data element should not contain more than one data element!")(
          data.data.size should be(1)
        )
        data.data.head.elementId should be(elementId)
        compareData(data.data.head.data, n.getTextContent)
      }
    } else
      fail("No data nodes with id attribute found in expected data tree!")
  }

  /**
    * Compares the stored data values of the element with the given id from the data tree
    * with the expected values stored within the `expectedDataTree` xml tree.
    *
    * @param elementId         The ID of the element we need to check.
    * @param expectedDataTree  An xml tree holding the expected data.
    * @param dataTree          The actual data tree that holds the parsed data.
    */
  def compareSequenceData(elementId: String,
                          expectedDataTree: Document,
                          dataTree: ActorRef): Unit = {
    val xpath = XPathFactory.newInstance().newXPath()

    val labels = xpath
      .evaluate(s"""//*[@class="id:$elementId"]""", expectedDataTree, XPathConstants.NODESET)
      .asInstanceOf[NodeList]
    var i = 0
    while (i < labels.getLength) {
      val label = labels.item(i)
      dataTree ! DataTreeDocumentMessages.ReturnData(elementId, Option(i.toLong))
      val labelData = expectMsgType[DataTreeNodeMessages.Content]
      withClue(s"The data tree node should only return one cell ($elementId, $i)!")(
        labelData.data.size should be(1)
      )
      labelData.data.head.elementId should be(elementId)
      labelData.data.head.data match {
        case None =>
          withClue(s"Expected data for $elementId does not match!")(label.getTextContent.isEmpty)
        case _ =>
          withClue(s"Expected data for $elementId does not match!")(
            compareData(labelData.data.head.data, label.getTextContent)
          )
      }

      i += 1
    }
  }

  /**
    * Compare data elements from stacked sequences.
    *
    * @param elementId         The data element id.
    * @param parentSequenceId  The id of the parent sequence which is also the child of a sequence.
    * @param expectedDataTree  An xml tree holding the expected data.
    * @param dataTree          The actual data tree that holds the parsed data.
    */
  def compareStackedSequenceData(elementId: String,
                                 parentSequenceId: String,
                                 expectedDataTree: Document,
                                 dataTree: ActorRef): Unit = {
    val xpath = XPathFactory.newInstance().newXPath()

    dataTree ! DataTreeDocumentMessages.ReturnXmlStructure
    val structure = expectMsgType[DataTreeDocumentMessages.XmlStructure]
    val doc       = structure.document
    val gramps    = getParentSequence(doc.getElementById(parentSequenceId))

    val labels = xpath
      .evaluate(s"""//*[@class="id:$parentSequenceId"]""", expectedDataTree, XPathConstants.NODESET)
      .asInstanceOf[NodeList]
    var parentSequenceRows = 0
    while (parentSequenceRows < labels.getLength) {
      val currentRow = labels.item(parentSequenceRows)
      val childlabels = xpath
        .evaluate(s"""descendant::*[@class="id:$elementId"]""", currentRow, XPathConstants.NODESET)
        .asInstanceOf[NodeList]
      withClue("Sequence element has no children!")(childlabels.getLength should be > 0)
      var childSequenceColumns = 0
      while (childSequenceColumns < childlabels.getLength) {
        val currentHash = calculateDataElementStorageHash(
          elementId,
          List((parentSequenceId, childSequenceColumns.toLong),
               (gramps.get.getAttribute("id"), parentSequenceRows.toLong))
        )
        val label = childlabels.item(childSequenceColumns)
        withClue(s"It should return the data for ($elementId, $parentSequenceRows, $currentHash)!") {
          dataTree ! DataTreeDocumentMessages.ReturnHashedData(elementId,
                                                               currentHash,
                                                               Option(childSequenceColumns.toLong))
          val labelData =
            fishForMessage(FiniteDuration(3, SECONDS)) {
              case _ => true
            }
          labelData match {
            case msg: StatusMessage =>
              fail(msg.message)
            case msg: DataTreeNodeMessages.Content =>
              msg.data.size should be > 0
              withClue("The saved data should be correct!") {
                if (msg.data.size > parentSequenceRows)
                  compareData(msg.data(parentSequenceRows).data, label.getTextContent)
                else
                  compareData(msg.data.head.data, label.getTextContent)
              }
          }
        }
        childSequenceColumns += 1
      }
      parentSequenceRows += 1
    }
  }

  /**
    * Helper function for the grunt work of comparing a generated with an expected data tree.
    *
    * @param dataFilePath   Path to the data file within the project (e.g. /foo/bar/fancy.csv).
    * @param dfasdlFilePath Path to the dfasdl file within the project.
    * @param max            The maximum duration that the function should wait for the parser to complete.
    * @return The actor ref of the data tree.
    */
  def prepareFileParserDataComparison(
      dataFilePath: String,
      dfasdlFilePath: String,
      max: FiniteDuration = FiniteDuration(30, SECONDS)
  ): ActorRef = {
    val sourceFilePath = getClass.getResource(dataFilePath).toURI
    fileParserDataComparison(sourceFilePath, dfasdlFilePath, max)
  }

  /**
    * Helper function for the grunt work of comparing a generated with an expected data tree.
    *
    * @param dataFilePath   Path to the temporary data file on the system.
    * @param dfasdlFilePath Path to the dfasdl file within the project.
    * @param max            The maximum duration that the function should wait for the parser to complete.
    * @return The actor ref of the data tree.
    */
  def prepareFileParserDataComparisonForTempFile(
      dataFilePath: String,
      dfasdlFilePath: String,
      max: FiniteDuration = FiniteDuration(30, SECONDS)
  ): ActorRef = {
    val sourceFilePath = new File(dataFilePath).toURI
    fileParserDataComparison(sourceFilePath, dfasdlFilePath, max)
  }

  /**
    * Helper function for the concrete comparison of a generated with an expected data tree.
    *
    * @param sourceFilePath URI to the data file within the project (e.g. /foo/bar/fancy.csv).
    * @param dfasdlFilePath Path to the dfasdl file within the project.
    * @param max            The maximum duration that the function should wait for the parser to complete.
    * @return The actor ref of the data tree.
    */
  def fileParserDataComparison(sourceFilePath: URI,
                               dfasdlFilePath: String,
                               max: FiniteDuration = FiniteDuration(30, SECONDS)): ActorRef = {
    val targetFilePath = File.createTempFile("xmlActorTest", "test").toURI
    val xml =
      scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFilePath)).mkString
    val dfasdl = DFASDL("MY-DFASDL", xml)
    val cookbook = Cookbook(
      "MY-COOKBOOK",
      List(dfasdl),
      Option(dfasdl),
      List(
        Recipe
          .createAllToAllRecipe("ID",
                                List(
                                  MappingTransformation(List(ElementReference(dfasdl.id, "sid")),
                                                        List(ElementReference(dfasdl.id, "tid")),
                                                        List())
                                ))
      )
    )
    val source = ConnectionInformation(new URI(s"$sourceFilePath"),
                                       Option(DFASDLReference(cookbook.id, dfasdl.id)))
    val target = ConnectionInformation(new URI(s"$targetFilePath"),
                                       Option(DFASDLReference(cookbook.id, dfasdl.id)))

    val agentStartTransformationMessage =
      AgentStartTransformationMessage(List(source), target, cookbook)

    val dataTree = TestActorRef(
      DataTreeDocument.props(dfasdl, Option("XmlActorSpec"), Set.empty[String])
    )
    val parser = TestFSMRef(new Parser(Option("XmlActorSpec")))
    parser ! ParserMessages.StartParsing(agentStartTransformationMessage,
                                         Map(dfasdl.hashCode() -> dataTree))

    val parserResponse = expectMsgType[ParserCompletedStatus](max)
    parserResponse.statusMessages.foreach(status => status should be(ParserStatus.COMPLETED))

    dataTree
  }

  /**
    * Helper function for the grunt work of comparing a generated with an expected structure tree.
    *
    * @param dataFilePath              Path to the data file within the project (e.g. /foo/bar/fancy.csv).
    * @param dfasdlFilePath            Path to the dfasdl file within the project.
    * @param expectedStructureFilePath Path to the file holding the expected structure tree within the project.
    * @param printResultTree           Print the result tree to stdout.
    * @param max                       The maximum duration that the function should wait for the parser to complete.
    * @return A tuple-3 containing the expected nodes, the actual nodes and the destination file.
    */
  def prepareFileParserStructureComparison(
      dataFilePath: String,
      dfasdlFilePath: String,
      expectedStructureFilePath: String,
      printResultTree: Boolean = false,
      max: FiniteDuration = FiniteDuration(30, SECONDS)
  ): (List[Element], List[Element], String) = {
    val sourceFilePath = getClass.getResource(dataFilePath).toURI
    fileParserStructureComparison(sourceFilePath,
                                  dfasdlFilePath,
                                  expectedStructureFilePath,
                                  printResultTree,
                                  max)
  }

  /**
    * Helper function for the grunt work of comparing a generated with an expected structure tree.
    *
    * @param dataFilePath              URI to the data file on the system.
    * @param dfasdlFilePath            Path to the dfasdl file within the project.
    * @param expectedStructureFilePath Path to the file holding the expected structure tree within the project.
    * @param printResultTree           Print the result tree to stdout.
    * @param max                       The maximum duration that the function should wait for the parser to complete.
    * @return A tuple-3 containing the expected nodes, the actual nodes and the destination file.
    */
  def prepareFileParserStructureComparisonForTempFile(
      dataFilePath: String,
      dfasdlFilePath: String,
      expectedStructureFilePath: String,
      printResultTree: Boolean = false,
      max: FiniteDuration = FiniteDuration(30, SECONDS)
  ): (List[Element], List[Element], String) = {
    val sourceFilePath = new File(dataFilePath).toURI
    fileParserStructureComparison(sourceFilePath,
                                  dfasdlFilePath,
                                  expectedStructureFilePath,
                                  printResultTree,
                                  max)
  }

  /**
    * Helper function for the concrete comparison of a generated with an expected structure tree.
    *
    * @param sourceFilePath            URI to the data file.
    * @param dfasdlFilePath            Path to the dfasdl file within the project.
    * @param expectedStructureFilePath Path to the file holding the expected structure tree within the project.
    * @param printResultTree           Print the result tree to stdout.
    * @param max                       The maximum duration that the function should wait for the parser to complete.
    * @return A tuple-3 containing the expected nodes, the actual nodes and the destination file.
    */
  def fileParserStructureComparison(
      sourceFilePath: URI,
      dfasdlFilePath: String,
      expectedStructureFilePath: String,
      printResultTree: Boolean = false,
      max: FiniteDuration = FiniteDuration(30, SECONDS)
  ): (List[Element], List[Element], String) = {
    val targetFilePath = File.createTempFile("xmlActorTest", "test").toURI
    val xml =
      scala.io.Source.fromInputStream(getClass.getResourceAsStream(dfasdlFilePath)).mkString
    val dfasdl = DFASDL("MY-DFASDL", xml)
    val cookbook = Cookbook(
      "MY-COOKBOOK",
      List(dfasdl),
      Option(dfasdl),
      List(
        Recipe
          .createAllToAllRecipe("ID",
                                List(
                                  MappingTransformation(List(ElementReference(dfasdl.id, "sid")),
                                                        List(ElementReference(dfasdl.id, "tid")),
                                                        List())
                                ))
      )
    )
    val source = ConnectionInformation(new URI(s"$sourceFilePath"),
                                       Option(DFASDLReference(cookbook.id, dfasdl.id)))
    val target = ConnectionInformation(new URI(s"$targetFilePath"),
                                       Option(DFASDLReference(cookbook.id, dfasdl.id)))

    val agentStartTransformationMessage =
      AgentStartTransformationMessage(List(source), target, cookbook)

    val dataTree = TestActorRef(
      DataTreeDocument.props(dfasdl, Option("XmlActorSpec"), Set.empty[String])
    )
    val parser = TestFSMRef(new Parser(Option("XmlActorSpec")))
    parser ! ParserMessages.StartParsing(agentStartTransformationMessage,
                                         Map(dfasdl.hashCode() -> dataTree))

    val parserResponse = expectMsgType[ParserCompletedStatus](max)
    parserResponse.statusMessages.foreach(status => status should be(ParserStatus.COMPLETED))

    dataTree ! ReturnXmlStructure

    val structure        = expectMsgType[DataTreeDocumentMessages.XmlStructure]
    val dataTreeDocument = structure.document

    if (printResultTree) {
      println("[WARN] --- DEBUG ResultTree ---")
      println(xmlToPrettyString(dataTreeDocument))
      println("--- DEBUG ResultTree ---")
    }

    val inExpectedXml: InputStream = getClass.getResourceAsStream(expectedStructureFilePath)
    val expectedXml                = scala.io.Source.fromInputStream(inExpectedXml).mkString
    val documentBuilder            = DocumentBuilderFactory.newInstance().newDocumentBuilder()
    val expectedTree               = documentBuilder.parse(new InputSource(new StringReader(expectedXml)))
    expectedTree.getDocumentElement.normalize()
    val expectedNodes = getNodeList(expectedTree)
    val actualNodes   = getNodeList(dataTreeDocument)

    (expectedNodes, actualNodes, targetFilePath.getSchemeSpecificPart)
  }

  /**
    * Compare two lists of xml elements.
    *
    * @param expected The list of expected elements.
    * @param actual The list of actual elements.
    * @return The number of data elements that occurred.
    */
  def compareXmlDataNodes(expected: List[Element], actual: List[Element]): Int = {
    var dataElementsCounted = 0

    withClue("The lists should have the same length.") {
      actual.size should be(expected.size)
    }

    (expected, actual).zipped.map {
      case (e, a: Element) =>
        withClue(s"Comparing ${xmlToPrettyString(e)} to ${xmlToPrettyString(a)}: ") {
          withClue("The type (tag-name) should be equal.") {
            a.getTagName should be(e.getTagName)
          }
          withClue(
            s"The number of attributes should be equal (without ${AttributeNames.STORAGE_PATH}!)."
          ) {
            if (a.hasAttribute(AttributeNames.STORAGE_PATH))
              a.getAttributes.getLength should be(e.getAttributes.getLength + 1)
            else
              a.getAttributes.getLength should be(e.getAttributes.getLength)
          }
          // Check attribute presence and values.
          var i = 0
          while (i < e.getAttributes.getLength) {
            val attrName = e.getAttributes.item(i).getNodeName
            withClue(s"Attribute $attrName should be present") {
              a.hasAttribute(attrName) should be(true)
            }
            if (attrName != AttributeNames.STORAGE_PATH)
              withClue(s"Value of attribute $attrName should be equal") {
                a.getAttribute(attrName) should be(e.getAttribute(attrName))
              }
            i += 1
          }
          // Check data.
          getElementType(a.getTagName) match {
            case ElementType.DataElement =>
              dataElementsCounted += 1
              getDataElementType(a.getTagName) match {
                case DataElementType.BinaryDataElement =>
                  fail("Comparison of binary elements not implemented!")
                case DataElementType.StringDataElement =>
                  a.getTextContent should be(e.getTextContent)
                case DataElementType.UnknownElement =>
                  fail(s"Unknown data element (${a.getTagName})!")
              }
            case _ => // We don't need to compare data of non data elements.
          }
        }
    }
    dataElementsCounted
  }

  /**
    * Helper function to compare xml structure trees.
    *
    * @param expectedNodes List of expected nodes.
    * @param actualNodes List of actual nodes.
    * @return Returns `true` if there are no differences.
    */
  def compareXmlStructureNodes(expectedNodes: List[Element],
                               actualNodes: List[Element]): Boolean = {
    (expectedNodes, actualNodes).zipped.map {
      case (e, a: Element) =>
        withClue(s"Comparing ${xmlToPrettyString(e)} to ${xmlToPrettyString(a)}: ") {
          withClue(s"Number of attributes for ${a.getTagName} (${a.getAttribute("id")})") {
            e.getAttributes.getLength should be(a.getAttributes.getLength)
          }
          var i = 0
          while (i < e.getAttributes.getLength) {
            val attrName = e.getAttributes.item(i).getNodeName
            withClue(s"Attribute $attrName should be present") {
              a.hasAttribute(attrName) should be(true)
            }
            if (attrName != AttributeNames.STORAGE_PATH)
              withClue(s"Value of attribute $attrName should be equal") {
                a.getAttribute(attrName) should be(e.getAttribute(attrName))
              }
            i += 1
          }
        }
    }
    true // If there was no test error until here we should be able to safely return `true`.
  }

  /**
    * Create a StartParsingMessage for multiple data sources using the given parameters.
    * **ATTENTION!** The order of the `sourceDataPaths` and their corresponding `sourceDFASDLPaths`
    * must be correct! Otherwise a wrong DFASDL will be assigned to a source file!
    *
    * @todo Allow multiple recipes?!?
    *
    * @param sourceDataPaths   A list of source data file paths.
    * @param sourceDFASDLs A list of source DFASDLs.
    * @param targetDFASDL  The target DFASDL.
    * @param mappings          A list of mapping transformations.
    * @param recipeMode        The recipe mode.
    * @return A created StartParsingMessage.
    */
  def createStartParsingMessageForMultipleSources(
      sourceDataPaths: List[String],
      sourceDFASDLs: List[DFASDL],
      targetDFASDL: DFASDL,
      mappings: List[MappingTransformation],
      recipeMode: RecipeMode = MapAllToAll
  ): StartParsing = {
    val sources = for (fileName <- sourceDataPaths) yield {
      val sourceFilePath = getClass.getResource(fileName)
      ConnectionInformation(
        new URI(s"$sourceFilePath"),
        Option(DFASDLReference("MY-COOKBOOK", s"SOURCE-${sourceDataPaths.indexOf(fileName)}"))
      )
    }
    val target = ConnectionInformation(
      new URI(
        s"file:${File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")}"
      ),
      Option(DFASDLReference("MY-COOKBOOK", "TARGET"))
    )

    val dataTreeDocs = scala.collection.mutable.Map.empty[Int, ActorRef]
    sourceDFASDLs.foreach(
      dfasdl => dataTreeDocs.put(dfasdl.hashCode(), TestActorRef(new DataTreeDocument(dfasdl)))
    )

    val recipe   = new Recipe("RECIPE-01", recipeMode, mappings)
    val cookbook = Cookbook("MY-COOKBOOK", sourceDFASDLs, Option(targetDFASDL), List(recipe))

    val msg = AgentStartTransformationMessage(sources, target, cookbook)

    new StartParsing(msg, dataTreeDocs.toMap)
  }

  /**
    * Create a StartProcessingMessage with the given parameters. If the target DFASDL string is empty, the source
    * DFASDL is used for the target.
    *
    * @param sourceDataPath    A string with the source data file path.
    * @param sourceDFASDLPath  A string with the file path to the source DFASDL.
    * @param targetDFASDLPath  A string with the file path to the target DFASDL.
    * @param mappings          A list of mapping transformations.
    * @param recipeMode        The recipe mode.
    * @return A created StartProcessing message.
    */
  def createStartProcessingMessage(
      sourceDataPath: String,
      sourceDFASDLPath: String,
      targetDFASDLPath: String = "",
      mappings: List[MappingTransformation] = List(),
      recipeMode: RecipeMode = MapAllToAll
  ): StartProcessingMessage = {
    val sourceFilePath = getClass.getResource(sourceDataPath)
    val targetFilePath =
      File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")
    val in: InputStream = getClass.getResourceAsStream(sourceDFASDLPath)
    val inXml           = scala.io.Source.fromInputStream(in).mkString
    val outXml =
      if (targetDFASDLPath.isEmpty)
        inXml
      else {
        val out: InputStream = getClass.getResourceAsStream(targetDFASDLPath)
        scala.io.Source.fromInputStream(out).mkString
      }

    val sourceDFASDL = DFASDL("SOURCE", inXml)
    val targetDFASDL = Option(DFASDL("TARGET", outXml))

    val source = ConnectionInformation(new URI(s"$sourceFilePath"),
                                       Option(DFASDLReference("MY-COOKBOOK", "SOURCE")))
    val target = ConnectionInformation(new URI(s"file:$targetFilePath"),
                                       Option(DFASDLReference("MY-COOKBOOK", "TARGET")))

    val transformations = if (mappings.isEmpty) {
      List(
        MappingTransformation(List(ElementReference(sourceDFASDL.id, "sid")),
                              List(ElementReference(targetDFASDL.get.id, "tid")),
                              List())
      )
    } else {
      mappings
    }

    val recipe   = new Recipe("ID", recipeMode, transformations)
    val cookbook = Cookbook("MY-COOKBOOK", List(sourceDFASDL), targetDFASDL, List(recipe))
    val msg      = AgentStartTransformationMessage(List(source), target, cookbook)

    val dataTreeDocument = TestActorRef(new DataTreeDocument(sourceDFASDL))
    new StartProcessingMessage(msg, List(dataTreeDocument))
  }

  /**
    * Create a StartProcessingMessage with the given parameters. If the target DFASDL string is empty, the source
    * DFASDL is used for the target.
    *
    * @param sourceDataPath    A string with the source data file path.
    * @param sourceDFASDL      The source DFASDL.
    * @param targetDFASDL      An option to the target DFASDL.
    * @return A created StartProcessing message.
    */
  def createStartProcessingMessageWithRecipes(sourceDataPath: String,
                                              sourceDFASDL: DFASDL,
                                              targetDFASDL: Option[DFASDL] = None,
                                              recipes: List[Recipe]): StartProcessingMessage = {
    val targetFilePath =
      File.createTempFile("xmlActorTest", "test").getAbsolutePath.replace("\\", "/")

    val msg = createStartTransformationMessage(sourceDataPath,
                                               targetFilePath,
                                               sourceDFASDL,
                                               targetDFASDL,
                                               recipes)

    val dataTreeDocument = TestActorRef(new DataTreeDocument(sourceDFASDL))

    new StartProcessingMessage(msg, List(dataTreeDocument))
  }

  /**
    * Create an agent start transformation message.
    *
    * @param sourceDataPath    A string with the source data file path.
    * @param targetFilePath    A string with the target data file path.
    * @param sourceDFASDL      The source dfasdl.
    * @param targetDFASDL      An option to the target dfasdl. If empty the source dfasdl will be used.
    * @param recipes           A list of recipe for the cookbook.
    * @return An agent start transformation message.
    */
  def createStartTransformationMessage(sourceDataPath: String,
                                       targetFilePath: String,
                                       sourceDFASDL: DFASDL,
                                       targetDFASDL: Option[DFASDL] = None,
                                       recipes: List[Recipe]): AgentStartTransformationMessage = {
    val sourceFilePath = getClass.getResource(sourceDataPath).toURI

    val myTargetDfasdl = targetDFASDL.map(d => d).getOrElse(sourceDFASDL.copy(id = "TARGET"))

    val cookbook = Cookbook("MY-COOKBOOK", List(sourceDFASDL), Option(myTargetDfasdl), recipes)

    val source =
      ConnectionInformation(sourceFilePath, Option(DFASDLReference(cookbook.id, sourceDFASDL.id)))
    val target = ConnectionInformation(new URI(s"file:$targetFilePath"),
                                       Option(DFASDLReference(cookbook.id, myTargetDfasdl.id)))

    AgentStartTransformationMessage(List(source), target, cookbook)
  }

  /**
    * A helper function that creates a list of element references from a given dfasdl and
    * a list of element ids.
    *
    * @param dfasdl The dfasdl that contains the elements.
    * @param ids A list of element ids from the given dfasdl.
    * @return A list of element references that may be empty.
    */
  def createElementReferenceList(dfasdl: DFASDL, ids: List[String]): List[ElementReference] =
    ids.map(id => ElementReference(dfasdlId = dfasdl.id, elementId = id))

  /**
    * Load a dfasdl xml file from the given path and create a DFASDL object with the given name.
    *
    * @param name The name/id of the DFASDL.
    * @param path The path to the xml file.
    * @return A DFASDL.
    */
  def loadDfasdl(name: String, path: String): DFASDL = {
    val in: InputStream = getClass.getResourceAsStream(path)
    val xml             = scala.io.Source.fromInputStream(in).mkString
    DFASDL(name, xml)
  }

}
