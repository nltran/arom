// (c) 2010-2011 Arthur Lesuisse

package org.arom.core.distributed

import org.arom.util.Logging

import scala.collection.mutable.{HashMap, Buffer, ArrayBuffer}
import scala.collection.JavaConversions._

import akka.actor.{Actor, ActorRef}
import akka.remote.MessageSerializer

import java.net.{URLClassLoader, URL, InetAddress}
import java.io.{File, FileOutputStream, FileInputStream, BufferedOutputStream, BufferedInputStream, ByteArrayInputStream, ByteArrayOutputStream}

import org.arom.core.{Node, Operator, OperatorRunner, EOF, Data}


object RemoteRuntime extends Logging {
	case object Unload
	
	lazy val classLoader = new URLClassLoader(Array(new URL("http://" + Config.master + ":" + Config.classpathPort + "/")), getClass.getClassLoader)

	
	case class RegisterTask(id: String, op: Node, ins: Int, outs: Int, outToInMapping: Seq[Int])
	case class SetOutput(outNum: Int, destHost: String, destId: String)	
	case class Finished(id: String)
	case class Stats(id: String, stats: List[(String, Any)])
	case class StaticInit(func: () => Unit)
    case class Failed(id: String, reason: Option[Throwable])
	
	private val master = Actor.remote actorFor("remote-master", Config.master, Config.port)	

	private var pendingMemorySize: Int = 0
	
	private class RemoteOperatorRunner( id: String, 
										op: Operator, 
										val nInputs: Int, 
										val nOutputs: Int,
										outToInMapping: Seq[Int]) extends OperatorRunner(op) {		
		
		protected override val flushAfterProcess = false
		
		private val buffers: Array[Option[DataMessage[_]]] = 1 to nOutputs map {_ => None} toArray
		private val pending = new java.util.LinkedList[(Int, DataMessage[_])]()
		private val spilled = new java.util.LinkedList[(Int, DataMessage[_])]()
		private val outputs: Array[Option[ActorRef]] = 1 to nOutputs map {_ => None} toArray

		self.id = id
        runners += this
		
		def emit[T](outputnum: Int, data: T)(implicit m: Manifest[T]) = buffers(outputnum) match {
			case None => 
				val newbuf = new DataMessage[T](m.toString, outToInMapping(outputnum))
				newbuf add data
				buffers update(outputnum, Some(newbuf))
				// is newbuf disposed?

			case Some(buf: DataMessage[T]) =>				
				buf add data
				if(buf.memorySize >= Config.Slave.maxMessageSize)
					flushCurrent(outputnum)
		}
		
		private var _receivedEof = false
		def emitEOF = {
			outputs foreach {
				case Some(actor) => actor ! EOF
				case _ => ()
			}
			_receivedEof = true
		}
		
		def flush = 0 until nOutputs foreach flushCurrent		
		
		private val flushedDataSize = Array[Int](1 to nOutputs map {_=>0}: _*)
		private def sendMessage(msg: DataMessage[_], actor: ActorRef, output: Int) = {			
			actor ! msg			
			flushedDataSize update(output, flushedDataSize(output) + msg.memorySize)			
			log.slf4j info("%s flushed message on output %d" format (id, output))			
		}
		
		private def flushCurrent(outNum: Int) = if(buffers(outNum) ne None) {
			// log.slf4j info ("%s: output sample: %s" format(id, buffers(outNum).get.data(0)))
			outputs(outNum) match {
				case Some(actor) =>
					sendMessage(buffers(outNum).get, actor, outNum)		
				case None => RemoteRuntime.this synchronized {
					pending add ((outNum, buffers(outNum).get))
					pendingMemorySize += buffers(outNum).get.compact

					if(pendingMemorySize >= Config.Slave.outputCacheHighWatermark) {
						log.slf4j info (id + ": output cache high watermark reached; spilling buffers to disk...")
                        runners foreach {_.spill}
						log.slf4j info (id + ": finished spilling.")
                    }}
			}
			buffers update(outNum, None)
			
		} else log.slf4j warn (id + ": attempted to flush empty buffer")

        protected def spill = {
            while(pendingMemorySize >= Config.Slave.outputCacheLowWatermark && !(pending isEmpty)) {
			    val (out, buf) = pending remove 0
			    // looks like pending is disposed
                var size = buf.compactSize
			    buf spill;
                var suffix = "bytes" :: "KiB" :: "MiB" :: "GiB" :: "TiB" :: Nil
				while((size >= 1024) && (suffix.size > 1)) {size = size / 1024; suffix = suffix tail}
                println("      ..." + size + " " + suffix.head)
			    pendingMemorySize -= buf.compactSize
			    spilled add ((out, buf))
                // is spilled disposed?
			}
        }
		
		private def flushLeftovers(outNum: Int) = outputs(outNum) match {
			case Some(actor) => RemoteRuntime.this synchronized {
                for(lst <- pending::spilled::Nil) {
                    val iter = lst iterator;
                    while(iter hasNext) {
//                    	log.slf4j info ("------------------------------- here 1")
                        val (out, buf) = iter next;
//     	                log.slf4j info ("------------------------------- here 2" + out + " , " + outNum )
                        if(out == outNum) {
//                        	log.slf4j info ("buf: " + buf.toString())
                            sendMessage(buf, actor, outNum)
                            iter remove;
                            if(lst eq pending) pendingMemorySize -= buf.compactSize
                        }
                    }
                }}
		}
		
		override def receive = {
			val func: PartialFunction[Any, Unit] = {		
			case d: DataMessage[_] => 
				log.slf4j info ("%s: got message on input %s" format(id, d.destInput))
//				log.slf4j info ("  sample: %s" format d.data.asInstanceOf[Seq[String]])
				super.receive((d.destInput, d.data))

			case SetOutput(outNum, desthost, destid) =>
				log.slf4j info ("%s: SetOutput no %d to %s" format(id, outNum, destid))
				val dest = _tasks.getOrElse(destid, Actor.remote actorFor(destid, desthost, Config.port))									
				outputs update(outNum, Some(dest))
				flushLeftovers(outNum)
				if(_receivedEof) {dest ! EOF}				
				
			case other if super.receive isDefinedAt other =>
				//log.slf4j info ("%s: received %s" format(id, other))
				super.receive(other)
			}
			// send stats to master after each message processed
			func andThen(_ => master ! Stats(id, ("outputSizes", 0 to nOutputs zip flushedDataSize)::Nil))
		}
		protected override def finish = {			
			super.finish
            log.slf4j info (id + ": finished.")
			master ! Finished(id)			
        }

        override def preRestart(reason: Throwable) = {
			log.slf4j info "Sending failure reason to master"
			reason match {
				case reason: Exception => master ! Failed(id, Some(reason))
				case _ => master ! Failed(id, None)
			}
		}
		
		op init this
		
	}
	
	private val _tasks: HashMap[String, ActorRef] = new HashMap
	val tasks: scala.collection.Map[String, ActorRef] = _tasks // read-only interface
    private val runners = Buffer[RemoteOperatorRunner]()
	
	def registerTask(task: RegisterTask) = task match {
		case RegisterTask(id, op: Operator, ins, outs, outToIn) =>
			log.slf4j info ("Received %s" format(id))
			val runner = Actor actorOf {
				Thread.currentThread setContextClassLoader classLoader
				val cl = classLoader loadClass "org.arom.distributed.RemoteRuntime$RemoteOperatorRunner"
				val cons = cl.getConstructors()(0)
				(cons newInstance(id, op, ins.asInstanceOf[AnyRef], outs.asInstanceOf[AnyRef], outToIn)
				).asInstanceOf[RemoteOperatorRunner]
				//new RemoteOperatorRunner(id, op, ins, outs, outToIn)
			}
			Actor.remote register(id, runner)
			_tasks update (id, runner)
			log.slf4j info ("Finished registerTask")
			runner
	}

	def staticInit(init: StaticInit) = init.func()

	def main(args: Array[String]) = {
        Data.defaultClassLoader = classLoader
		Actor.remote start(InetAddress.getLocalHost.getHostName, Config.port, classLoader)
		Actor.remote register("remote-runtime", Actor actorOf new Actor {
			def receive = {
				case task @ RegisterTask(id, _, _, _, _) =>
					registerTask(task)
					//self reply id
					master ! id
				
				case Unload =>
					_tasks.values foreach {_.stop}
					_tasks.values foreach Actor.remote.unregister
					_tasks.clear					
					Actor.remote shutdownClientModule;
					//MessageSerializer setClassLoader new MyClassLoader

				case init: StaticInit =>
					staticInit(init)
			}
		})
	}
	
}


import akka.serialization._
private case class DataMessage[T](manifest: String, destInput: Int) extends Serializable.SBinary[DataMessage[T]] {
	import sbinary._
	import sbinary.DefaultProtocol._
	import sbinary.Operations._	
	import sbinary.JavaIO._
		
	private var _data: Buffer[T] = new ArrayBuffer[T]()
	private var _memsize: Int = 0	
	
	def memorySize = _memsize
	
	def this() = this(null, -1)
	
	def add(t: T) = {
		_data += t
		_memsize += ops memorySize t
	}
	
	private val ops = manifest match {
		case null => null
		case sth => Data.opsFor[T](manifest)
	}
 
	implicit object MessageFormat extends Format[DataMessage[T]] {
		def reads(in : Input) = {
			val manifest = read[String](in)
			val destInput = read[Int](in)
			val result = new DataMessage[T](manifest, destInput)			
			result._memsize = read[Int](in)
			import result.ops.binFormat
			result._data = read[Buffer[T]](in)			
			result
		}					
			
		def writes(out: Output, value: DataMessage[T]) = value.spilledFile match {			
			case None => value.compactArray match {
				case None =>
					write(out, value.manifest)
					write(out, value.destInput)
					write(out, value._memsize)
					import value.ops.binFormat
					write(out, value._data)
				case Some(array) => // dump already-serialized data
					out writeAll(array, 0, array.size)
					compactArray = None}
			case Some(file) => // dump already-serialized data
				val bytes = new Array[Byte](8192)
				val ins = new FileInputStream(file)
				var read: Int = ins read bytes
                while(read > 0) {
                    out writeAll(bytes, 0, read)
                    read = ins read bytes
                }
				ins close;
                spilledFile.get delete;
                spilledFile = None
		}
	}
	
	 def fromBytes(bytes: Array[Byte]) = fromByteArray[DataMessage[T]](bytes) 
	 def toBytes: Array[Byte] = toByteArray(this)
	
	 private var compactArray: Option[Array[Byte]] = None
	 var compactSize: Int = -1

	 def compact = {
		 assert(spilledFile eq None)
		 assert(compactArray eq None)
		 val outs = new ByteArrayOutputStream(128*1024)
		 MessageFormat writes(outs, this)
		 outs close;
		 compactArray = Some(outs.toByteArray)
		 _data = null
		 compactSize = compactArray.get.size
		 compactSize
	 }

	 private var spilledFile: Option[File] = None 
	 
	 def spill = {
		 assert(spilledFile eq None)
		 val file = File createTempFile("arom", "spill")
		 val outs = new BufferedOutputStream(new FileOutputStream(file), 1024*1024)
		 if(compactArray eq None)
	         MessageFormat writes(outs, this)
		 else
			 outs write(compactArray.get, 0, compactArray.get.size)
		 outs close;
		 spilledFile = Some(file)
		 compactArray = None
		 _data = null
	 }
	 
	 def data: Seq[T] = {		
		if((_data eq null) && (spilledFile ne None)) { // recover spilled data
			val ins = new BufferedInputStream(new FileInputStream(spilledFile.get), 1024*1024)
			_data = MessageFormat reads ins _data;
			ins close;
            spilledFile.get delete;
            spilledFile = None
		} else if((_data eq null) && (compactArray ne None)) {
			val ins = new ByteArrayInputStream(compactArray.get)
			_data = MessageFormat reads ins _data;
			ins close;
			compactArray = None
		}
		_data
	}
	 
}


