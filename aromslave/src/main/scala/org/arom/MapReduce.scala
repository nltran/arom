// (c) 2010-2011 Arthur Lesuisse

package org.arom

import distributed.RemoteMaster.DistributedJob
import scala.collection.immutable.VectorBuilder
import scala.collection.JavaConversions._
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.conf.Configuration
import collection.mutable.{ArrayBuffer, Buffer}
import sbinary.{Output, Input, Format}

private object Util {
	def writable2bytes(w: Writable) = {
		val bytes = new java.io.ByteArrayOutputStream
		val out = new java.io.DataOutputStream(bytes)
		w write out
		out close;
		bytes toByteArray
	}
	def bytes2writable[T <: Writable](w: T, b: Array[Byte]) = {
		val in = new java.io.DataInputStream(new java.io.ByteArrayInputStream(b))
		w readFields in
		in close;
		w
	}
}

class MRMap[K1, V1, K2, V2](val map: (K1, V1, MRMap[K1, V1, K2, V2]) => Seq[(K2, V2)], val confbytes: Array[Byte], val splitnum: Int, val locations: Seq[String])
                           (implicit mk: Manifest[K2], mv: Manifest[V2]) extends InputOperator with Locatable {
	def copy = throw new Error("doesn't happen")
    //def copy = new MRMap(map)
	/*def process(runtime) = { case (i: Int, (k: K1, v: V1)) =>
		map(k, v) foreach runtime.emit[(K2, V2)]
	}*/
    var conf: Configuration = null
    var split: InputSplit = null
    var reader: RecordReader[K1, V1] = null
    override def init(rt) = {
        conf = Util bytes2writable(new Configuration, confbytes)
        val format = (Data.defaultClassLoader.loadClass(conf get "input.format.class") newInstance).asInstanceOf[InputFormat[K1, V1]]
        val ctx = new TaskAttemptContext(conf, new TaskAttemptID)
        split = format getSplits ctx get splitnum
        reader = format createRecordReader(split, ctx)
        reader initialize(split, ctx)
    }

    private var _hasData = true
    def hasData = _hasData

    def sendData(rt) = {
        val toread = 3000
        var i = 0
        while(i < toread && _hasData) {
            if(reader nextKeyValue) {
                i += 1
                map(reader getCurrentKey, reader getCurrentValue, this) foreach rt.emit[(K2, V2)]
            } else _hasData = false
        }
    }

    def locateAmong(hosts: Seq[String]) = hosts intersect locations
}

@serializable class MROrdering[K, V](implicit val keyord: Ordering[K]) extends Ordering[(K, V)] {
	def compare(l: (K, V), r: (K, V)) = {
        keyord compare(l._1, r._1)
    }
}

class MRLocalSort[K, V](implicit ord: MROrdering[K, V], mk: Manifest[K], mv: Manifest[V]) extends QuicksortMergesort[(K, V)] {
	override def copy = new MRLocalSort	
}

private class MRMerge[K, V](implicit keyord: Ordering[K], ord: MROrdering[K, V], mk: Manifest[K], mv: Manifest[V]) extends QuicksortMergesort[(K, V)](false) {
	override def copy = new MRMerge
	override def finish(runtime) = if(! data.isEmpty) {
        //println("Merge output: " + data)
        val iter = resultIterator
        val first = iter next
        var currentkey = first._1
        val currentvalues = new VectorBuilder[V]
        currentvalues += first._2;
        while(iter hasNext) {
            val next = iter.next
            if(keyord.compare(next._1, currentkey) != 0) {
                runtime.emit[(K, Seq[V])]((currentkey, currentvalues result))
                currentkey = next._1;
                currentvalues clear
            }
            currentvalues += next._2
        }
        runtime.emit[(K, Seq[V])]((currentkey, currentvalues result))
        currentvalues clear;
	    clear
	}
}

class MRPartition[K, V](implicit mk: Manifest[K], mv: Manifest[V]) extends Partition[(K, V)] {
	def copy = new MRPartition()(mk, mv)
	def func(t: (K, V)) = {t match {case (k: K, v: V) => k.hashCode}}
}

class MRReduce[K1, V1, K2, V2](reduce: (K1, Seq[V1]) => Seq[(K2, V2)], taskNum: Int, confbytes: Array[Byte])(implicit mk: Manifest[K2], mv: Manifest[V2]) extends ScalarOperator {
    var conf: Configuration = null
    var writer: RecordWriter[K2, V2] = null
    var context: TaskAttemptContext = null
    var committer: OutputCommitter = null
    override def init(rt) = {
        conf = Util bytes2writable(new Configuration, confbytes)
        val format = (Data.defaultClassLoader.loadClass(conf get "output.format.class") newInstance).asInstanceOf[OutputFormat[K2, V2]]
        context = new TaskAttemptContext(conf, new TaskAttemptID("arom-mr", 0, true, taskNum, 0))
        writer = format getRecordWriter context
        committer = format getOutputCommitter context
		committer setupTask context
    }
    override def finish(rt) = {
		writer close context
		if(committer needsTaskCommit context)
			committer commitTask context
	}
	def copy = throw new Error("doesn't happen")
	def process(runtime) = {case (i: Int, (k: K1, v: Seq[V1])) =>
		reduce(k, v) foreach {case (kout, vout) => writer write(kout, vout)}
        //reduce(k, v) foreach {x => println("reduce got: " + x)}
	}
}

abstract class MRDataOps[K <: Writable, V <: Writable](implicit mk: Manifest[K], mv: Manifest[V]) extends Data.DataOps[(K, V)] {
    import sbinary._
	import sbinary.Operations._
	import sbinary.DefaultProtocol._
	implicit object binFormat extends Format[Buffer[(K, V)]] {
		def reads(in : Input) = {
			val bytes = read[Array[Byte]](in)
			val in2 = new java.io.DataInputStream(new java.io.ByteArrayInputStream(bytes))
			val numitems = in2.readInt
			val result = new ArrayBuffer[(K, V)](numitems)
			1 to numitems foreach { _ =>
                val key = mk.erasure.newInstance.asInstanceOf[K]
                val value = mv.erasure.newInstance.asInstanceOf[V]
			    key readFields in2
                value readFields in2
			    result += ((key, value))
		    }
			in2 close;
			result
		}

		def writes(out: Output, value: Buffer[(K, V)]) = {
			val bytes = new java.io.ByteArrayOutputStream(value.size * memorySize(value.head))
			val out2 = new java.io.DataOutputStream(bytes)
			out2 writeInt value.size
			value foreach { case (k, v) =>
                k write out2
                v write out2
            }
			out2 close;
			write[Array[Byte]](out, bytes toByteArray)
		}
    }
}

abstract class MRGroupedDataOps[K <: Writable, V <: Writable](implicit mk: Manifest[K], mv: Manifest[V]) extends Data.DataOps[(K, Seq[V])] {
    import sbinary._
	import sbinary.Operations._
	import sbinary.DefaultProtocol._
	implicit object binFormat extends Format[Buffer[(K, Seq[V])]] {
		def reads(in : Input) = {
			def br: Int = 0xff & read[Byte](in).asInstanceOf[Int] 
			val stream = new java.io.InputStream {
				def read = br
			}
			//val bytes = read[Array[Byte]](in)
			val in2 = new java.io.DataInputStream(stream)
			val numitems = in2.readInt
			val result = new ArrayBuffer[(K, Seq[V])](numitems)
			1 to numitems foreach { _ =>
                val key = mk.erasure.newInstance.asInstanceOf[K]
			    key readFields in2
				val numvals = in2 readInt
				val vals = 1 to numvals map {_ => 
                	val value = mv.erasure.newInstance.asInstanceOf[V]
					value readFields in2
					value
				}
			    result += ((key, vals))
		    }
			in2 close;
			result
		}

		def writes(out: Output, value: Buffer[(K, Seq[V])]) = {
			def bw(b: Int) = write[Byte](out, b.toByte)
			val stream = new java.io.OutputStream {
				def write(b: Int) = bw(b)
			}
			//val bytes = new java.io.ByteArrayOutputStream(value.size * memorySize(value.head))
			val out2 = new java.io.DataOutputStream(stream)
			out2 writeInt value.size
			value foreach { case (k, vs) =>
                k write out2
				out2 writeInt vs.size
				vs foreach {_ write out2}
            }
			out2 close;
			//write[Array[Byte]](out, bytes toByteArray)
		}
    }
}

abstract class MapReduce[K1, V1, K2, V2, K3, V3](implicit mv2: Manifest[V2], mk2: Manifest[K2], mv3: Manifest[V3], mk3: Manifest[K3]) {
    implicit val keyOrdering: Ordering[K2]
    implicit object mrOrdering extends MROrdering[K2, V2]()(keyOrdering)
    val map: (K1, V1, MRMap[K1, V1, K2, V2]) => Seq[(K2, V2)]
    val reduce: (K2, Seq[V2]) => Seq[(K3, V3)]

    val inputFormat: InputFormat[K1, V1]
    val outputFormat: OutputFormat[K3, V3]
    val preconf = new Configuration
    preconf set("mapred.job.tracker", "local")
    val job = new Job(preconf)
    val jobconf = job.getConfiguration

    private def createPlan(reducers: Int) = {
        jobconf set("output.format.class", outputFormat.getClass.getCanonicalName)
        jobconf set("input.format.class", inputFormat.getClass.getCanonicalName)
        val confbytes = Util writable2bytes jobconf
        val maps: Seq[Plan] = createMaps(confbytes)
        val mappers = maps.size
        val mapplan = maps reduceLeft {_ || _}
        val reduces: Seq[Plan] = createReduces(reducers, confbytes)
        val reduceplan = reduces reduceLeft {_ || _}
        mapplan >= ((new MRLocalSort[K2, V2] >= new MRPartition[K2, V2]) ^ mappers) >>
		(new MRMerge[K2, V2] ^ reducers) >= reduceplan
    }

    private def createMaps(confbytes: Array[Byte]) = {
        val splits = inputFormat.getSplits(new JobContext(jobconf, new JobID))
        var i = -1;
        splits map { split =>
            val locs: Seq[String] = split getLocations;
            i += 1
            new MRMap(map, confbytes, i, locs)
        }
    }
    private def createReduces(nb: Int, confbytes: Array[Byte]) = 0 until nb map {new MRReduce(reduce, _, confbytes)}

    def createAromJob(reducers: Int) = {
        val plan = createPlan(reducers)
        outputFormat checkOutputSpecs job
        new DistributedJob(plan) {
            private val committer = outputFormat getOutputCommitter new TaskAttemptContext(jobconf, new TaskAttemptID)
            private val hints = plan.edges filter {
                case (from: MRMap[_, _, _, _], _) => true
                case (from: MRLocalSort[_, _]) => true
                case (from: MRMerge[_, _]) => true
                case _ => false
            }
            override def start = {
                committer setupJob job
                addHints(hints.toSet)
                super.start
            }
            override def waitDone = {
                super.waitDone;
                committer cleanupJob job
            }
        }
    }
}
