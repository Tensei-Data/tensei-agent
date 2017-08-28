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

import akka.actor.ActorSystem
import com.wegtam.tensei.agent.DataTreeNode.DataTreeNodeMessages
import com.wegtam.tensei.agent.adt.ParserDataContainer
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration._

@State(Scope.Benchmark)
@Fork(3)
@Warmup(iterations = 4)
@Measurement(iterations = 10)
@BenchmarkMode(Array(Mode.Throughput))
class DataTreeNodeAppendDataBenchmark {
  implicit val system: ActorSystem = ActorSystem("DataTreeNodeAppendDataBenchmark")
  val data = ParserDataContainer(
    data = "Lorem ipsum dolor sit amet, vix ut electram honestatis disputationi. Qui erant adolescens signiferumque ea, te mel deleniti eleifend, no sit ridens assentior. Quo vidit aliquid intellegat ut, ad sed discere dignissim consequuntur. Movet debitis delectus vel ad, te eripuit argumentum vel.",
    elementId = "ID"
  )

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
    val _ = Await.ready(system.terminate(), 15.seconds)
  }

  @Benchmark
  @OperationsPerInvocation(20000)
  def testAppendDataAggregation: Boolean = {
    val node = system.actorOf(DataTreeNode.props(Option("This-DataTreeNode-shall-append-a-lot-of-data.")))
    for (i <- 0 until 20000) {
      node ! DataTreeNodeMessages.AppendData(data)
    }
    true
  }

}
