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

package usecases.merge

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import akka.actor.ActorRef
import akka.testkit.{ TestActorRef, TestFSMRef }
import com.wegtam.tensei.adt.Recipe.MapOneToOne
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.Parser.ParserCompletedStatus
import com.wegtam.tensei.agent.Parser.ParserMessages.StartParsing
import com.wegtam.tensei.agent.Processor.ProcessorMessages.{ Completed, StartProcessingMessage }
import com.wegtam.tensei.agent._
import com.wegtam.tensei.agent.adt.ParserStatus
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks

import scala.concurrent.duration._

class MergeTwoCSV extends XmlActorSpec with XmlTestHelpers with PropertyChecks {
  val agentRunIdentifier = Option("MergeTwoCSVTest")

  case class Product(
      id: String,
      link: String,
      title: String,
      beschreibung: String,
      bildlink: String,
      marke: String,
      preis: java.math.BigDecimal,
      ean: String,
      gewicht: java.math.BigDecimal,
      googleCategory: String,
      verfuegbarkeit: String,
      produktTyp: String,
      zustand: String,
      versand: java.math.BigDecimal
  ) {
    def columns: List[String] = List(
      id,
      link,
      title,
      beschreibung,
      bildlink,
      marke,
      preis.toPlainString,
      ean,
      gewicht.toPlainString,
      googleCategory,
      verfuegbarkeit,
      produktTyp,
      zustand,
      versand.toPlainString
    )
  }

  private val productGen = for {
    path  <- Gen.alphaNumStr
    title <- Gen.alphaNumStr
    descr <- Gen.alphaNumStr
    ppath <- Gen.alphaNumStr
    brand <- Gen.oneOf(List("BrandA", "BrandB", "BrandC"))
    price <- arbitrary[BigDecimal]
    uid = java.util.UUID.randomUUID().toString
    ean    <- Gen.alphaNumStr
    weight <- arbitrary[BigDecimal]
    gcat   <- Gen.alphaNumStr
    avail  <- Gen.oneOf(List("in stock", "out of stock"))
    ptype  <- Gen.alphaNumStr
    state  <- Gen.oneOf(List("new", "used", "broken"))
    delvr  <- arbitrary[BigDecimal]
  } yield
    Product(
      id = uid,
      link = s"http://www.example.com/$path",
      title = title,
      beschreibung = descr,
      bildlink = s"http://img.example.com/$ppath.jpg",
      marke = brand,
      preis = new java.math.BigDecimal(price.toString()),
      ean = ean,
      gewicht = new java.math.BigDecimal(weight.toString()),
      googleCategory = gcat,
      verfuegbarkeit = avail,
      produktTyp = ptype,
      zustand = state,
      versand = new java.math.BigDecimal(delvr.toString())
    )

  private val productListGen = Gen.containerOf[List, Product](productGen)

  describe("Use cases") {
    describe("merging 2 ordered csv files") {
      it("should produce correct results") {
        forAll(productListGen) { ps =>
          val sourceA = Files.createTempFile("source-01-a-", ".tsv")
          val sourceB = Files.createTempFile("source-01-b-", ".tsv")
          val target  = Files.createTempFile("target-01-", ".tsv")

          val sourceDfasdlA = DFASDL(
            id = "SOURCE-0",
            content = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/usecases/merge/source-01-a.xml"),
                StandardCharsets.UTF_8.name()
              )
              .mkString
          )
          val sourceDfasdlB = DFASDL(
            id = "SOURCE-1",
            content = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/usecases/merge/source-01-b.xml"),
                StandardCharsets.UTF_8.name()
              )
              .mkString
          )
          val targetDfasdl = DFASDL(
            id = "TARGET",
            content = scala.io.Source
              .fromInputStream(
                getClass.getResourceAsStream("/usecases/merge/target-01.xml"),
                StandardCharsets.UTF_8.name()
              )
              .mkString
          )

          val sas = Files.newOutputStream(sourceA)
          val sbs = Files.newOutputStream(sourceB)
          ps.foreach { p =>
            val cr      = "\n".getBytes(StandardCharsets.UTF_8)
            val columns = p.columns
            sas.write(columns.take(7).mkString("\t").getBytes(StandardCharsets.UTF_8))
            sas.write(cr)
            val cbs = List(p.ean,
                           p.id,
                           p.gewicht.toPlainString,
                           p.googleCategory,
                           p.verfuegbarkeit,
                           p.produktTyp,
                           p.zustand,
                           p.versand.toPlainString)
            sbs.write(cbs.mkString("\t").getBytes(StandardCharsets.UTF_8))
            sbs.write(cr)
          }
          sas.close()
          sbs.close()

          val mappings = List(
            MappingTransformation(
              List(
                ElementReference(sourceDfasdlA.id, "link"),
                ElementReference(sourceDfasdlA.id, "title"),
                ElementReference(sourceDfasdlA.id, "beschreibung"),
                ElementReference(sourceDfasdlA.id, "bildlink"),
                ElementReference(sourceDfasdlA.id, "marke"),
                ElementReference(sourceDfasdlA.id, "preis"),
                ElementReference(sourceDfasdlA.id, "id"),
                ElementReference(sourceDfasdlB.id, "ean"),
                ElementReference(sourceDfasdlB.id, "gewicht"),
                ElementReference(sourceDfasdlB.id, "google_cat"),
                ElementReference(sourceDfasdlB.id, "verfuegbarkeit"),
                ElementReference(sourceDfasdlB.id, "produkttyp"),
                ElementReference(sourceDfasdlB.id, "zustand"),
                ElementReference(sourceDfasdlB.id, "versand")
              ),
              List(
                ElementReference(targetDfasdl.id, "link"),
                ElementReference(targetDfasdl.id, "title"),
                ElementReference(targetDfasdl.id, "beschreibung"),
                ElementReference(targetDfasdl.id, "bildlink"),
                ElementReference(targetDfasdl.id, "marke"),
                ElementReference(targetDfasdl.id, "preis"),
                ElementReference(targetDfasdl.id, "id"),
                ElementReference(targetDfasdl.id, "ean"),
                ElementReference(targetDfasdl.id, "gewicht"),
                ElementReference(targetDfasdl.id, "google_cat"),
                ElementReference(targetDfasdl.id, "verfuegbarkeit"),
                ElementReference(targetDfasdl.id, "produkttyp"),
                ElementReference(targetDfasdl.id, "zustand"),
                ElementReference(targetDfasdl.id, "versand")
              ),
              List(),
              List(),
              Option(MappingKeyFieldDefinition("id"))
            )
          )
          val recipe = Recipe(
            id = "RECIPE-01",
            mode = MapOneToOne,
            mappings = mappings
          )
          val cookbook = Cookbook(
            id = "MY-COOKBOOK",
            sources = List(sourceDfasdlA, sourceDfasdlB),
            target = Option(targetDfasdl),
            recipes = List(recipe)
          )

          val startTransformationMessage = AgentStartTransformationMessage(
            sources = List(
              ConnectionInformation(
                uri = sourceA.toUri,
                dfasdlRef = Option(DFASDLReference(cookbook.id, sourceDfasdlA.id))
              ),
              ConnectionInformation(
                uri = sourceB.toUri,
                dfasdlRef = Option(DFASDLReference(cookbook.id, sourceDfasdlB.id))
              )
            ),
            target = ConnectionInformation(
              uri = target.toUri,
              dfasdlRef = Option(DFASDLReference(cookbook.id, targetDfasdl.id))
            ),
            cookbook = cookbook
          )
          val dataTreeDocs: Map[Int, ActorRef] = Map(
            sourceDfasdlA.hashCode() -> TestActorRef(new DataTreeDocument(sourceDfasdlA)),
            sourceDfasdlB.hashCode() -> TestActorRef(new DataTreeDocument(sourceDfasdlB))
          )

          val startParsingCommand =
            StartParsing(startTransformationMessage, dataTreeDocs)

          val parser    = TestFSMRef(new Parser(agentRunIdentifier))
          val processor = TestFSMRef(new Processor(agentRunIdentifier))

          parser ! startParsingCommand

          val response = expectMsgType[ParserCompletedStatus](60.seconds)
          response.statusMessages foreach (st => st should be(ParserStatus.COMPLETED))

          processor ! StartProcessingMessage(stm = startTransformationMessage,
                                             dataTreeDocs = response.dataTreeDocs)

          expectMsg(5.seconds, Completed)

          val expectedTarget: String = ps
            .map(
              p =>
                List(
                  p.link,
                  p.title,
                  p.beschreibung,
                  p.bildlink,
                  p.marke,
                  p.preis.toPlainString,
                  p.id,
                  p.ean,
                  p.gewicht.toPlainString,
                  p.googleCategory,
                  p.verfuegbarkeit,
                  p.produktTyp,
                  p.zustand,
                  p.versand.toPlainString
                ).mkString("\t")
            )
            .mkString("\n")
          val producedTarget: String = scala.io.Source
            .fromInputStream(
              Files.newInputStream(target),
              StandardCharsets.UTF_8.name()
            )
            .mkString

          withClue(s"Merging $sourceA and $sourceB into $target failed!") {
            producedTarget should be(expectedTarget)
          }

          Files.deleteIfExists(sourceA)
          Files.deleteIfExists(sourceB)
          Files.deleteIfExists(target)
        }
      }

    }
  }
}
