// (c) 2010-2011 Arthur Lesuisse

package org.arom

import scala.collection.mutable.{Queue, HashMap, Buffer, ArrayBuffer}
import org.arom.util.Logging
import org.apache.commons.io.input.ClassLoaderObjectInputStream

@serializable trait Operator extends Node { 	
	def processWithRuntime(inputnum: Int, data: Seq[Any], rt: Operator.Runtime): Unit 

	def init(rt: Operator.Runtime) = ()
	def finish(rt: Operator.Runtime) = ()	
	
	override def toString() = (getClass() getName() split("\\.") last)
}

object Operator {	
	trait Runtime {
		val nInputs: Int
		val nOutputs: Int		
		def emit[T](outputnum: Int, data: T)(implicit m: Manifest[T]): Unit
		def emit[T](data: T)(implicit m: Manifest[T]): Unit = 0 to (nOutputs-1) foreach (emit(_, data))
		def emitEOF: Unit
		def forceFinish: Unit		
	}
}

object Data extends Logging {	
	trait DataOps[T] {				
		def memorySize(t: T): Int
		implicit val binFormat: sbinary.Format[Buffer[T]] 
	}
	class DefaultOps[T] extends DataOps[T] {
		import sbinary._
		import sbinary.Operations._
		import sbinary.DefaultProtocol._
		def memorySize(t: T) = 32
		implicit object binFormat extends sbinary.Format[Buffer[T]] {
			def reads(in: Input) = {
				val numitems = read[Int](in)
				val result = new ArrayBuffer[T](numitems)
				val bytes = new java.io.ByteArrayInputStream(read[Array[Byte]](in))
				val objin = new ClassLoaderObjectInputStream(defaultClassLoader, bytes)
				1 to numitems foreach {_ => 
					val obj = objin.readObject.asInstanceOf[T]					
					result += obj
				}
				objin close;
				result
			}
			def writes(out: Output, value: Buffer[T]) = {
				write(out, value.size)
				val bytes = new java.io.ByteArrayOutputStream
				val objout = new java.io.ObjectOutputStream(bytes)
				value foreach objout.writeObject
				objout close;
				write(out, bytes toByteArray)
			}
		}
	}
    var defaultClassLoader = getClass.getClassLoader
	private val registeredDataOps = HashMap[String, DataOps[_]]();
	
	def register[T](ops: DataOps[T])(implicit m: Manifest[T]) = registeredDataOps update(m toString, ops)
	def opsFor[T](manifest: String): DataOps[T] = registeredDataOps get manifest match {
		case Some(ops) => 
			ops.asInstanceOf[DataOps[T]]
		case None if manifest != "Any" && manifest != "Nothing" => 
			//log.slf4j warn ("Using default data format for manifest " + manifest)
			new DefaultOps[T]
	}
}

trait Locatable { self: Operator =>
	def locateAmong(hosts: Seq[String]): Seq[String]
}

trait Early { self: Operator =>
   def isEarly = true	
}

trait ScalarOperator extends Operator {
	protected def process(rt: Operator.Runtime): PartialFunction[(Int, Any), Unit]
	def processWithRuntime(inputnum: Int, data: Seq[Any], rt: Operator.Runtime) = {		
		data foreach (process(rt)(inputnum, _))
	}
}

trait VectorOperator extends Operator {
	protected def process(rt: Operator.Runtime): PartialFunction[(Int, Seq[Any]), Unit]
	def processWithRuntime(inputnum: Int, data: Seq[Any], rt: Operator.Runtime) = {
		process(rt)(inputnum, data)		
	}
}

trait SynchronousOperator extends Operator {
	private var inqueues: Seq[Queue[Any]] = null
	protected def process(rt: Operator.Runtime): PartialFunction[Array[Any], Unit]
	protected def consume(inputnum: Int) = {inqueues(inputnum) dequeue; ()} 
	def processWithRuntime(inputnum: Int, data: Seq[Any], rt: Operator.Runtime) = synchronized {
		if(inqueues eq null) inqueues = 1 to rt.nInputs map {_ => new Queue[Any]}
		inqueues(inputnum) enqueue(data: _*)
		while(inqueues forall {! _.isEmpty})
			process(rt)(Array(inqueues map {_ front}: _*))
	}
}

trait InputOperator extends Operator {
    def hasData: Boolean
    def sendData(rt: Operator.Runtime): Unit
    def processWithRuntime(inputnum: Int, data: Seq[Any], rt: Operator.Runtime) = {}
}

case object EOF
case object BOF

trait AromJob {
	def start: Unit
	val errors: scala.collection.Seq[Throwable]
	def finished: Boolean
	def waitDone: Unit
}

abstract class OperatorRunner(val op: Operator) extends akka.actor.Actor with Operator.Runtime {
	
	protected val flushAfterProcess = true
	
	private var eofReceived: Int = 0
	private var finished = false
	def receive = {
        case BOF if op.isInstanceOf[InputOperator] =>
            val input = op.asInstanceOf[InputOperator]
            if(input hasData) {
                input sendData this
                self ! BOF
            } else processEof
        case BOF => processEof
		case EOF => {
		  processEof
		  log.slf4j.debug("received EOF")
		}
		case (inputNum: Int, data: Seq[_]) => processData(inputNum, data)
	}
	
	private def processData(inputNum: Int, data: Seq[_]) = if(!finished) {
		log.slf4j.debug("%s(%d) received message on input %d" format(op, hashCode, inputNum))
		op.processWithRuntime(inputNum, data, this)
		if(flushAfterProcess)
			flush		
	}
	
	private def processEof = if(!finished) {
		eofReceived = eofReceived + 1
		if(eofReceived >= nInputs) {finish}
	}
	
	protected def flush: Unit
	protected def finish = {
		op finish this		
		flush
		emitEOF
		log.slf4j.debug("emitted eof")
		finished = true
	}
	
	def forceFinish = finish
	def isFinished: Boolean = finished
	
	self.lifeCycle = akka.config.Supervision.Temporary
}
