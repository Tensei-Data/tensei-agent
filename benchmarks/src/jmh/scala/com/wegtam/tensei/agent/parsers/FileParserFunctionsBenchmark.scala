package com.wegtam.tensei.agent.parsers

import java.nio.charset.Charset

import com.wegtam.tensei.agent.parsers.FileParserFunctions.{ExtractDataWithRegExResponse, FileParserReadElementOptions}
import com.wegtam.tensei.agent.parsers.FileParserFunctionsBenchmark.{BenchmarkFileParserFunctions, BenchmarkSourceData}

import org.openjdk.jmh.annotations._

import scala.util.matching.Regex

/**
  * This benchmark tests several functions of the fileparser regarding their
  * throughput.
  *
  * @todo Write benchmarks for `readNextByteElement` and `readNextStringElement`.
  */
@State(Scope.Benchmark)
@Fork(3)
@Warmup(iterations = 4)
@Measurement(iterations = 10)
@BenchmarkMode(Array(Mode.Throughput))
class FileParserFunctionsBenchmark {
  final private val f = new BenchmarkFileParserFunctions

  final private val biggerOptions = FileParserReadElementOptions(
    encoding = Charset.forName("UTF-8"),
    start_sign = "\"",
    stop_sign = "\"[,;]\"",
    format = "(-?\\d{0,4}\\.\\d{0,2})",
    isInChoice = false,
    sequence_stop_sign = "A|AB|ABC",
    correct_offset = 0L,
    preferSequenceStopSign = false
  )
  final private val biggerPattern = f.buildRegularExpression(biggerOptions)
  final private val biggerSource  = {
    val mediumFiller = (for (_ <- 0 to 14) yield scala.util.Random.alphanumeric.take(20).mkString).mkString(";")
    val largeFiller  = (for (_ <- 0 to 14) yield scala.util.Random.alphanumeric.take(20).mkString).mkString(";")

    val small  = "-1234.56;That should not be extracted."
    val medium = s"-1234.56;That should not be extracted.;$mediumFiller"
    val large  = s"-1234.56;That should not be extracted.;$largeFiller"

    BenchmarkSourceData(small, medium, large)
  }

  final private val commonOptions = FileParserReadElementOptions(
    encoding = Charset.forName("UTF-8"),
    start_sign = "",
    stop_sign = "\t",
    format = "",
    isInChoice = false,
    sequence_stop_sign = "",
    correct_offset = 0L,
    preferSequenceStopSign = false
  )
  final private val commonPattern = f.buildRegularExpression(commonOptions)
  final private val commonSource  = {
    val mediumFiller = (for (_ <- 0 to 14) yield scala.util.Random.alphanumeric.take(20).mkString).mkString("\t")
    val largeFiller  = (for (_ <- 0 to 30) yield scala.util.Random.alphanumeric.take(20).mkString).mkString("\t")

    val small  = "I am just a lonely string\tThat should not be extracted."
    val medium = s"I am just a lonely string\tThat should not be extracted.$mediumFiller"
    val large  = s"I am just a lonely string\tThat should not be extracted.$largeFiller"

    BenchmarkSourceData(small, medium, large)
  }

  @Benchmark
  def buildCommonRegularExpression: Regex = {
    f.buildRegularExpression(commonOptions)
  }

  @Benchmark
  def buildBiggerRegularExpression: Regex = {
    f.buildRegularExpression(biggerOptions)
  }

  @Benchmark
  def extractBiggerDataWithRegularExpressionSmall: ExtractDataWithRegExResponse = {
    f.extractDataWithRegularExpression(biggerOptions, biggerPattern, biggerSource.small)
  }

  @Benchmark
  def extractBiggerDataWithRegularExpressionMedium: ExtractDataWithRegExResponse = {
    f.extractDataWithRegularExpression(biggerOptions, biggerPattern, biggerSource.medium)
  }

  @Benchmark
  def extractBiggerDataWithRegularExpressionLarge: ExtractDataWithRegExResponse = {
    f.extractDataWithRegularExpression(biggerOptions, biggerPattern, biggerSource.large)
  }

  @Benchmark
  def extractCommonDataWithRegularExpressionSmall: ExtractDataWithRegExResponse = {
    f.extractDataWithRegularExpression(commonOptions, commonPattern, commonSource.small)
  }

  @Benchmark
  def extractCommonDataWithRegularExpressionMedium: ExtractDataWithRegExResponse = {
    f.extractDataWithRegularExpression(commonOptions, commonPattern, commonSource.medium)
  }

  @Benchmark
  def extractCommonDataWithRegularExpressionLarge: ExtractDataWithRegExResponse = {
    f.extractDataWithRegularExpression(commonOptions, commonPattern, commonSource.large)
  }

}

object FileParserFunctionsBenchmark {

  final case class BenchmarkSourceData(small: String, medium: String, large: String)

  class BenchmarkFileParserFunctions extends FileParserFunctions {

  }

}
