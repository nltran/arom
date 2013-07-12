 // (c) AROM processing

/*
 * Similar example than WordCountExample, reusing the Partition and
 * WordCounter operators, but reading the file from a web page 
 */

package org.arom.examples

import org.arom.core.InputOperator
import org.arom.core._
import org.arom.core.distributed.RemoteMaster.DistributedJob
import org.apache.pig.data.Tuple
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.HashMap


object HTTPWordCountJob {
  val distribution = 5

  def createHTTPReaderPlan:Plan = {
    val inputURL = "http://example/example.txt"
    println(inputURL)

    new HTTPFileReader(inputURL)
  }

  def main(args: Array[String]) {
    val inputPlan = HTTPWordCountJob.createHTTPReaderPlan

    val wordCountJobPlan = (inputPlan >= ((new Partition >> new WordCounter)^(distribution)) >> new PrintLN)

    val job = new DistributedJob(wordCountJobPlan)
    job.start
  }
}
