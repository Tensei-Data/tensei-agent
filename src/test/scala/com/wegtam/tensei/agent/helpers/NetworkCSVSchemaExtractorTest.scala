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

package com.wegtam.tensei.agent.helpers

import java.io.InputStream

import akka.testkit.TestActorRef
import com.wegtam.tensei.adt._
import com.wegtam.tensei.agent.{ ActorSpec, SchemaExtractor, XmlTestHelpers }
import org.eclipse.jetty.server.{ Handler, Server }
import org.eclipse.jetty.server.handler.{
  ContextHandler,
  ContextHandlerCollection,
  ResourceHandler
}
import org.eclipse.jetty.toolchain.test.MavenTestingUtils
import org.eclipse.jetty.util.resource.Resource
import org.scalatest.Matchers

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scalaz._

class NetworkCSVSchemaExtractorTest
    extends ActorSpec
    with Matchers
    with CSVSchemaExtractor
    with XmlHelpers
    with XmlTestHelpers {
  val port: Int      = 1024 + scala.util.Random.nextInt(60000)
  val server: Server = new Server(port)

  override def beforeAll(): Unit = {
    val context0: ContextHandler = new ContextHandler()
    context0.setContextPath("/")
    val rh0 = new ResourceHandler()
    val dir0 = MavenTestingUtils.getTestResourceDir(
      "com/wegtam/tensei/agent/helpers/networkCSVSchemaExtractor"
    )
    rh0.setBaseResource(Resource.newResource(dir0))
    context0.setHandler(rh0)

    val contexts          = new ContextHandlerCollection()
    val c: Array[Handler] = new Array[Handler](1)
    c.update(0, context0)
    contexts.setHandlers(c)

    server.setHandler(contexts)
    server.start()
    Thread.sleep(3000)
    val _ = server.isStarted should be(true)
  }

  override def afterAll(): Unit = {
    server.stop()
    Thread.sleep(3000)
    server.isStopped should be(true)
    val _ = Await.result(system.terminate(), Duration.Inf)
  }

  describe("Test a network CSV file") {
    val data   = new java.net.URI(s"http://localhost:$port/simple-with-comma.csv")
    val source = ConnectionInformation(data, None)

    it("should work") {
      val schemaExtractor = TestActorRef(SchemaExtractor.props())
      schemaExtractor ! GlobalMessages.ExtractSchema(
        source,
        ExtractSchemaOptions.createCsvOptions(hasHeaderLine = false, "", "")
      )
      val in: InputStream = getClass.getResourceAsStream(
        "/com/wegtam/tensei/agent/helpers/networkCSVSchemaExtractor/simple-with-comma.xml"
      )
      val xmlExpected = scala.io.Source.fromInputStream(in).mkString

      val msg = expectMsgType[GlobalMessages.ExtractSchemaResult]
      msg.result match {
        case \/-(result) =>
          result.content.replaceAll("\\s+", "") should be(xmlExpected.replaceAll("\\s+", ""))
        case -\/(error) =>
          error should be("")
      }
    }
  }
}
