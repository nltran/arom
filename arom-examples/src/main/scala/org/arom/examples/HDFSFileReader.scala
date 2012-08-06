package org.arom.examples

/*
 * This is the core HDFS file reader operator. As the file stored in HDFS is split over multiple chunks and potentially stored on
 * different physical hosts, we need to spawn an operator for each split and precisely specify which split the operator needs to read from
 * This is done in the other part of the job, where the plan is recursively generated for each split
 */

import org.arom.core.Data
import org.arom.core.InputOperator
import org.arom.core.ScalarOperator
import org.arom.core.Locatable
import org.arom.util.Logging
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
import scala.collection.JavaConversions.asScalaBuffer
import org.xml.sax.helpers.NewInstance
import javax.xml.parsers.DocumentBuilderFactory
import org.xml.sax.InputSource
import java.io.StringReader
import org.w3c.dom.Element
import org.w3c.dom.CharacterData
import scala.collection.JavaConversions._

import scala.util.control.Breaks._
import java.util.regex.Matcher
import java.util.regex.Pattern
import org.apache.pig.builtin.IsEmpty
import scala.collection.mutable.HashSet
import scala.collection.mutable.Buffer

class HDFSFileReader(val splitnum: Int, val filepath: String, val locations: Seq[String]) extends InputOperator with Locatable with Logging {
  def locateAmong(hosts: Seq[String]): Seq[String] = {
    locations intersect hosts
  }
  def copy = throw new Error("This shouldn't happen")

  var conf: Configuration = null
  var split: InputSplit = null
  var reader: RecordReader[LongWritable, Text] = null

  override def init(rt) = {

    var job: Job = null

    var jobconf: Configuration = null

    job = new Job
    jobconf = job.getConfiguration
    
    /*
     * These are the input formats supported by Hadoop
     */
    val inputFormat = new TextInputFormat
    val outputFormat = new TextOutputFormat[Text, Text]
    jobconf set ("output.format.class", outputFormat.getClass.getCanonicalName)
    jobconf set ("input.format.class", inputFormat.getClass.getCanonicalName)
    conf = jobconf
 
    FileInputFormat.addInputPath(job, new Path(filepath))
    FileInputFormat.setMinInputSplitSize(job, 128 * 1024 * 1024)
    FileInputFormat.setMaxInputSplitSize(job, 130 * 1024 * 1024)

    val format = (Data.defaultClassLoader.loadClass(conf get "input.format.class") newInstance).asInstanceOf[TextInputFormat]
    val ctx = new TaskAttemptContext(conf, new TaskAttemptID)

    val splits = inputFormat.getSplits(new JobContext(jobconf, new JobID))
    //    println(splits)
    split = format getSplits ctx get splitnum
    reader = format createRecordReader (split, ctx)
    reader initialize (split, ctx)
  }

  private var _hasData = true
  def hasData = _hasData

  def sendData(rt): Unit = {
    
    var i = 0
    /*
     * We throttle the sending of the data. The framework call the sendData multiple times
     */
    while (_hasData && i < 1000) {
      if (reader nextKeyValue) {
        val words = reader.getCurrentValue().toString.split(" ")
        /*
         * We emit by default on the 0th output, as it will be a partition operator
         */
        words foreach(t => rt.emit(0, t))
        i += 1
      } else { 
        _hasData = false
        println("finished reading")
      }
    }
  }
}

object HDFSFileReaderPlan {
  def createHDFSFileReaderOperators(inputFormat: TextInputFormat, jobconf: Configuration, filepath: String) = {
    val splits = inputFormat.getSplits(new JobContext(jobconf, new JobID))
    var i = -1;
    splits map { split =>
      val locs: Seq[String] = split getLocations;
      i += 1
      new HDFSFileReader(i, filepath, locs)
    }
  }
}