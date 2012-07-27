package org.arom

import org.arom.Data
import org.arom.InputOperator
import org.arom.ScalarOperator
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

class HDFSReader(val splitnum: Int, val filepath: String) extends InputOperator {
  def copy = throw new Error("This shouldn't happen")

  var conf: Configuration = null
  var split: InputSplit = null
  var reader: RecordReader[LongWritable, Text] = null

  override def init(rt) = {

    var job: Job = null

    var jobconf: Configuration = null

    job = new Job
    jobconf = job.getConfiguration

    val inputFormat = new TextInputFormat
    val outputFormat = new TextOutputFormat[Text, Text]
    jobconf set ("output.format.class", outputFormat.getClass.getCanonicalName) //TextOutputFormat[Text,Text]
    jobconf set ("input.format.class", inputFormat.getClass.getCanonicalName)
    conf = jobconf

    FileInputFormat.addInputPath(job, new Path(filepath))
    FileInputFormat.setMinInputSplitSize(job, 128 * 1024 * 1024)
    FileInputFormat.setMaxInputSplitSize(job, 212 * 1024 * 1024)
    val fs = FileSystem get jobconf

    val format = (Data.defaultClassLoader.loadClass(conf get "input.format.class") newInstance).asInstanceOf[TextInputFormat]
    val ctx = new TaskAttemptContext(conf, new TaskAttemptID)

    val splits = inputFormat.getSplits(new JobContext(jobconf, new JobID))
    println(splits)
    split = format getSplits ctx get splitnum
    reader = format createRecordReader (split, ctx)
    reader initialize (split, ctx)
  }

  private var _hasData = true
  def hasData = _hasData

  def sendData(rt) = {
    var i = 0
    while (_hasData && i < 1000) {
      if (reader nextKeyValue) {
        //        rt.emit(0, reader.getCurrentValue.toString().replace("\t",","))
        rt.emit(0, text2Tuple(reader.getCurrentValue()))
        i += 1
      } else _hasData = false
    }
  }

  def text2Tuple(t: Text): Tuple = {
    val textString = t.toString().split("\t")
    var tuple = new java.util.ArrayList[String](textString.size)
    for (i <- 0 until textString.length) {
      tuple.add(textString(i))
    }
    Factories.tuple.newTupleNoCopy(tuple)
  }

}

class HDFSWriter(val taskNum:Int, val filepath:String) extends ScalarOperator {

  var conf: Configuration = null
  var writer: RecordWriter[Text, Text] = null
  var context: TaskAttemptContext = null
  var committer: OutputCommitter = null
  override def init(rt) = {
    var job: Job = null

    var jobconf: Configuration = null

    job = new Job
    jobconf = job.getConfiguration

    val inputFormat = new TextInputFormat
    val outputFormat = new TextOutputFormat[Text, Text]
    jobconf set ("output.format.class", outputFormat.getClass.getCanonicalName) //TextOutputFormat[Text,Text]
    jobconf set ("input.format.class", inputFormat.getClass.getCanonicalName)
    conf = jobconf
    
    FileOutputFormat.setOutputPath(job,new Path(filepath))
    val format = (Data.defaultClassLoader.loadClass(conf get "output.format.class") newInstance).asInstanceOf[OutputFormat[Text, Text]]
    context = new TaskAttemptContext(conf, new TaskAttemptID("arom-mr", 0, true, taskNum, 0))
    writer = format getRecordWriter context
    committer = format getOutputCommitter context
    committer setupTask context
  }
  override def finish(rt) = {
    writer close context
    if (committer needsTaskCommit context)
      committer commitTask context
  }
  def copy = throw new Error("doesn't happen")
  def process(runtime) = {
    case (i: Int, t:Tuple) =>
      writer write(new Text(Integer.toString(i)),new Text(t.toString()))
//      reduce(k, v) foreach { case (kout, vout) => writer write (kout, vout) }
    //reduce(k, v) foreach {x => println("reduce got: " + x)}
    case (i: Int,t:Array[String]) => {
      writer write (new Text(t(1).toString()),new Text(t(2).toString))
    }
  }

}

object HDFSPlan {
  def createHDFSInputOperators( inputFormat: TextInputFormat, jobconf: Configuration, filepath:String) = {
      val splits = inputFormat.getSplits(new JobContext(jobconf, new JobID))
//      println(splits)
      var i = -1;
      splits map { split =>
        val locs: Seq[String] = split getLocations;
        i += 1
        new HDFSReader(i, filepath)
      }
    }
}

 