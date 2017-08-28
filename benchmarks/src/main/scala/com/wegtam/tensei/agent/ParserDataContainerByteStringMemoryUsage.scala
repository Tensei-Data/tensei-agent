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

import akka.util.ByteString
import com.wegtam.tensei.agent.adt.ParserDataContainer
import net.openhft.hashing.LongHashFunction
import org.github.jamm.MemoryMeter

object ParserDataContainerByteStringMemoryUsage {

  def main(args: Array[String]): Unit = {
    val stringSize = 287
    val meter      = new MemoryMeter()
    val hasher     = LongHashFunction.xx()
    val builder    = Vector.newBuilder[ParserDataContainer]
    println(
      s"Going to create a Vector with $SAMPLE_SIZE ParserDataContainer elements using ByteString of $stringSize chars."
    )
    println(
      "Each ParserDataContainer will contain data, elementId, dfasdlId, sequenceRowCounter and dataElementHash."
    )
    for (cnt <- 1L to SAMPLE_SIZE) {
      builder += ParserDataContainer(
        data = ByteString(scala.util.Random.alphanumeric.take(stringSize).mkString),
        elementId = "AN-ID",
        dfasdlId = Option("DFASDL-ID"),
        sequenceRowCounter = cnt,
        dataElementHash = Option(hasher.hashBytes(s"Something-$cnt".getBytes("UTF-8")))
      )
      if (cnt % 250000 == 0)
        println(s"\tCreated $cnt elements.")
    }
    println("Calling .result() on VectorBuilder.")
    val r = builder.result()
    println("Checking length of generated collection.")
    require(r.length.toLong == SAMPLE_SIZE)
    println("Measuring memory usage. This may take a while.")
    val bytes  = meter.measureDeep(r)
    val mBytes = "%.2f".format(bytes.toDouble / 1024 / 1024)
    println(s"Vector allocates $bytes bytes ($mBytes MB).")
  }

}
