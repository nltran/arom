 // (c) AROM processing

/*
 * In this example we will read a text file stored on HDFS and count
 * the amount of amount of words contained in it.
 */

package org.arom.examples

import org.arom.core.InputOperator
import org.arom.core._
import org.arom.core.distributed.RemoteMaster.DistributedJob
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.JobID
import org.apache.hadoop.mapreduce.OutputCommitter
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.pig.data.Tuple
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.HashMap


class Partition extends ScalarOperator {
  override def copy = new Partition

  override def process(rt) = {
    case (_,t:String) => rt.emit(math.abs(t.hashCode % rt.nOutputs),t)
  }

}

class WordCounter extends ScalarOperator {
  override def copy = new WordCounter

  val mainTable = new HashMap[String, Int]
  override def process(rt) = {
    case (_, word:String) => {
      mainTable get word match {
        case None => mainTable update (word, 1)
        case Some(count:Int) => mainTable update(word, count+1)
      }
    }
  }
  override def finish(rt) {
    mainTable foreach {
      case (word: String, count: Int) => rt.emit("word \t " +count.toString())
    }

  }

}

object WordCountJob {
  val distribution = 5

  def createHDFSReaderPlan:Plan = {
    /*
     * In this function we are going to create the first stage of the job which is composed of reader
     * operators. Each of these operator will be reading a different spit of the file stored in hdfs,
     * using the HDFSFileReader operator defined in HDFSFileReader.scala
     */
    val inputPath = "hdfs://localhost:9000/YOURFILEPATH"
    println(inputPath)
    val inputFormat = new TextInputFormat
    val outputFormat = new TextOutputFormat
    val job = new Job()
    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileInputFormat.setMinInputSplitSize(job, 128 * 1024 * 1024)
    FileInputFormat.setMaxInputSplitSize(job, 130 * 1024 * 1024)

    val jobconf = job.getConfiguration
    jobconf set ("output.format.class", outputFormat.getClass.getCanonicalName)
    jobconf set ("input.format.class", inputFormat.getClass.getCanonicalName)

    val HDFSSplits: Seq[Plan] = HDFSFileReaderPlan.createHDFSFileReaderOperators(inputFormat, jobconf, inputPath)
    val HDFSInputPlan = HDFSSplits reduceLeft { _ || _ }
    HDFSInputPlan
  }

  def main(args: Array[String]) {
    /*
     * Create the HDFS reading plan
     */
    val inputPlan = WordCountJob.createHDFSReaderPlan
    /*
     * Create the plan for the job. In this example we simply print all the results using the PrintLn operator
     */
    val wordCountJobPlan = (inputPlan >= ((new Partition >> new WordCounter)^(distribution)) >> new PrintLN)

    val job = new DistributedJob(wordCountJobPlan)
    job.start
  }
}
