// (c) 2010-2011 Arthur Lesuisse

package org.arom.core

import org.arom.util
import scala.util.Random
import scala.collection.mutable.{Buffer, HashSet}
import scala.collection.JavaConversions._

class Pass[T](implicit m: Manifest[T]) extends ScalarOperator {
	def copy = new Pass
	def process(runtime) = { case (_: Int, t: T) => runtime.emit[T](t) }
}

class Filter[T](val predicate: T => Boolean)(implicit m: Manifest[T]) extends ScalarOperator {	
	def process(runtime) = { case (_: Int, t: T) =>
		predicate(t) match {	
			case true => runtime emit t
			case _ => ()
		}
	}	
	def copy = new Filter(predicate)
}

class Limit[T](val limit: Int)(implicit m: Manifest[T]) extends VectorOperator {
	private var seen = 0
	def process(runtime) = {case (_: Int, data: Seq[T]) =>
		val needed = limit - seen
		data take needed foreach runtime.emit[T]
		if(data.size >= needed) {			
			runtime.forceFinish
		} 
		seen = seen + data.size		
	}
	def copy = new Limit(limit)
}

class HashDistinct[T](implicit m: Manifest[T]) extends ScalarOperator {
	private val seen: collection.mutable.Set[T] = new HashSet[T]
	def process(runtime) = { case (_: Int, t: T) =>
		seen contains t match {	
			case false => seen add t; runtime emit t
			case _ => ()
		}
	}
	override def finish(rt) = seen clear;
	def copy = new HashDistinct[T]
}

abstract class Partition[T](implicit m: Manifest[T]) extends ScalarOperator {
	def func(t: T): Int
	def process(runtime) = { case(_: Int, t: T) =>		
		runtime emit (func(t).abs % runtime.nOutputs, t)
	}	
}

class HashPartition[T](implicit m: Manifest[T]) extends Partition[T] {
	def func(t: T) = t.hashCode
	def copy = new HashPartition
}

class Scatter[T](implicit m: Manifest[T]) extends Partition[T] {
	private var _nextout: Int = -1
	def func(t: T) = {_nextout = _nextout + 1; _nextout}
	def copy = new Scatter
}

class PrintLN extends ScalarOperator {
	def copy = new PrintLN
	def process(runtime) = { case(i: Int, t: Any) => println("received on input %d: %s" format (i, t)) }
}

class Forget extends VectorOperator {
	def copy = new Forget
	def process(runtime) = { case(_, _) => () }
}


/*class DummySource[T](data: Seq[T]) extends ScalarOperator {
	def copy = new DummySource(data)
	def process(runtime) = { case _ => () }
	override def finish(runtime) = {
		data foreach runtime.emit[T]
	}
}*/

class QuicksortMergesort[T](protected val doSort: Boolean = true)(implicit ord: Ordering[T], m: Manifest[T]) extends VectorOperator {
	protected val data = new java.util.LinkedList[T]
	def process(runtime) = {case (_: Int, tuples: Seq[T]) =>
        val currentLeft = data.listIterator();
		val currentRight = if(doSort) tuples sorted else tuples
		var r = 0
//		println("merging %s with %s" format(currentLeft, currentRight))
		while(currentLeft.hasNext && r < currentRight.size) {
			if(ord.gt(currentLeft.next, currentRight(r))) {
				currentLeft previous;
				currentLeft add currentRight(r)
				r += 1
			}
		}
		r until currentRight.size map {data addLast currentRight(_)}
		//println("merge result: %s" format(data))
	}
	
	override def finish(rt)	= {
		resultIterator foreach rt.emit[T]
        clear
	}

    protected def resultIterator = data iterator
    protected def clear = data clear
    protected def count = data size
	def copy = new QuicksortMergesort
}


class QuicksortMergesort2[T](protected val doSort: Boolean = true)(implicit ord: Ordering[T], m: Manifest[T]) extends VectorOperator {
    // something is wrong in there which makes it very slow
	protected val data = Buffer[Seq[T]]()
	def process(runtime) = {case (_: Int, tuples: Seq[T]) =>
		data += (if(doSort) tuples sorted else tuples)
	}

	override def finish(rt)	= resultIterator foreach rt.emit[T]

    protected def resultIterator = new Iterator[T] {
        implicit val headOrdering = new Ordering[Seq[T]] {
            def compare(x: Seq[T], y: Seq[T]) = ord compare(y head, x head)
        }
        val queue = new scala.collection.mutable.PriorityQueue[Seq[T]]
        queue enqueue(data: _*)
        def hasNext = ! queue.isEmpty
        def next = {
            val seq = queue dequeue;
            if(seq.size > 1)
                queue += seq.tail
            seq head
        }
    }
    protected def count = (0 /: data) {_ + _.size}
    protected def clear = data clear;

	def copy = new QuicksortMergesort2
}

class HeapSort[T](implicit ord: Ordering[T], m: Manifest[T]) extends ScalarOperator {
    var heap: util.LargePriorityQueue[T] = null
    override def init(rt) = {
        heap = new util.LargePriorityQueue(65536)(ord reverse)
    }
    def process(rt) = {case (_: Int, t: T) =>
        heap += t
    }
    override def finish(rt) = {
        while(! heap.isEmpty)
            rt.emit[T](heap dequeue)
    }
    def copy = new HeapSort
}

/**class SampleRanges[T <: AnyRef](nbranges: Int)(implicit ord: Ordering[T], m: Manifest[T]) extends QuicksortMergesort[T] {
    override def finish(rt) = rt.emit[Seq[T]](count match {
        case x if x <= nbranges => // not enough data => no samples
            Vector()
        case count =>
            val samples: Array[T] = new Array[T](nbranges-1)
            val iter = resultIterator
            var i = 0
            var nextsample = 0
            while(iter hasNext) {
                val cur = iter.next
                if(i == (count*(nextsample+1)) / nbranges) {
    				samples update(nextsample, cur)
    				nextsample += 1
	    		}
                i += 1
            }
            clear;
            Vector(samples: _*)
    })

    override def copy = new SampleRanges(nbranges)
}*/

class Sample[T](amount: Double)(implicit m: Manifest[T]) extends ScalarOperator {
    def process(rt) = {case (_: Int, t: T) =>
        rt.emit[T](0, t)
        if(Random.nextDouble <= amount) {
            rt.emit[T](1, t)
        }
    }
    def copy = new Sample(amount)
}

class SampleRanges[T <: AnyRef](nbranges: Int)(implicit ord: Ordering[T], m: Manifest[T]) extends VectorOperator {
	protected val data: java.util.LinkedList[T] = new java.util.LinkedList	
	private val samples: Array[T] = new Array[T](nbranges-1)
	
	def process(rt) = {case (_: Int, inputdata: Seq[T]) =>
		val totalsize = data.size + inputdata.size
		var currentLeft = data.listIterator();
		var currentRight = inputdata sorted;
		var i = 0
		
		var nextSample = 0
		def update(sample: T) = {
			if(i == (totalsize*(nextSample+1)) / nbranges) {
				samples update(nextSample, sample)
				nextSample = nextSample + 1
			}
		}
		
		while(currentLeft.hasNext && ! currentRight.isEmpty) {
			val nextLeft = currentLeft.next
			if(ord.gt(nextLeft, currentRight.head)) {
				currentLeft previous;
				currentLeft add currentRight.head
				update(currentRight.head)
				currentRight = currentRight.tail // TODO: ArrayBuffer.tail performs a copy and therefore sucks	
			} else update(nextLeft)
			i = i + 1
		}		
		currentRight foreach {tuple =>
			data addLast tuple
			update(tuple)
			i = i + 1
		}
	}
	
	override def finish(rt) = {		
		rt emit (
			if(samples exists {_ eq null}) // No data -> no samples
				Vector()
			else
				Vector(samples: _*)
		)
		data clear
	}
	
	def copy = new SampleRanges(nbranges)
}


class RangePartition[T](implicit ord: Ordering[T], m: Manifest[T]) extends SynchronousOperator {	
	def process(rt) = {case Array(data: T, samples: Seq[T]) =>
		// "samples" must be a sequence of samples as output by the SampleRanges operator
		val outnum = samples findIndexOf {case sample: T => ord.gt(sample, data)} match {
			case -1 => samples.size
			case num => num
		}
		rt emit(outnum, data)
		consume(0)
	}
	def copy = new RangePartition
}

class CompositeOperator[T](plan: Plan)(implicit m: Manifest[T]) extends Operator {	
	var job: LocalSubJob = null	
	override def init(rt) = {
		val sources: Seq[Plan] = 0 until rt.nInputs map {_ => new Pass[T]}
		val sinks: Seq[Plan] = 0 until rt.nOutputs map { o =>
			new VectorOperator {
				def process(localrt) = { case(_: Int, data: Seq[T]) =>
					data foreach {rt emit(o, _)}
				}
				def copy = this
			}
		} 
		job = new LocalSubJob((sources reduceLeft {_ || _}) >= plan >= (sinks reduceLeft {_ || _}))
		job start
	}
	def processWithRuntime(inputnum: Int, data: Seq[Any], rt: Operator.Runtime) = {
		job inject(inputnum, data)
	}
	override def finish(rt) = {
		job waitDone
	}
	def copy = new CompositeOperator[T](plan)
}


