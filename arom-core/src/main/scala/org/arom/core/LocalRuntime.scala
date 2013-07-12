// (c) 2010-2011 Arthur Lesuisse

package org.arom.core


import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer

import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props, SupervisorStrategy}
import org.arom.util.Logging
import org.arom.util._

private object LocalConfig {
	val maxBufferSize: Long = 4 * 1024 * 1024
	val initialBufferCapacity = 10000
}

class LocalJob(val plan: Plan) extends AromJob with Logging {
	// actor instantiation is a bit awkward:
	val runners = scala.collection.mutable.HashMap[Node, OperatorRunner]()
	private lazy val system = ActorSystem.create()
	val actors: Map[Node, ActorRef] = Map(plan.vertices map {
		case op: Operator => (op, system actorOf(Props({
			val runner = new LocalOperatorRunner(op, this)
			runners update(op, runner)
			runner
		})))
	}: _*)
	private var nbfinished = 0
	private var startTime: Long = -1
	def start = {
		startTime = new java.util.Date().getTime
		plan.inputs map actors foreach {_ ! BOF}
		import plan.PlanOps;
		val viewer = (new GraphAlgorithm(plan) viewer)
		viewer startup;
		viewer setNodeLabels { case node: Node => node.toString }
	}

	def view = {
		import plan.PlanOps;
		val viewer = (new GraphAlgorithm(plan) viewer)
		viewer startup;
		viewer setNodeLabels { case node: Node => node.toString }

	}

	def finished = runners.size == nbfinished
	val errors = scala.collection.mutable.ListBuffer[Exception]()
	def waitDone = {
		while(!finished) {
			Thread sleep 500
		}
		val ellapsed: Double = new java.util.Date().getTime - startTime
		log.slf4j info "Local job finished in %f seconds".format(ellapsed / 1000.0)
	}

	private class LocalOperatorRunner(op: Operator, val job: LocalJob) extends OperatorRunner(op) {
		override val supervisorStrategy = OneForOneStrategy() {
			case _: ArithmeticException => SupervisorStrategy.resume
			case _: NullPointerException => SupervisorStrategy.restart
			case _: IllegalArgumentException => SupervisorStrategy.stop
			case _: Exception => SupervisorStrategy.escalate
		}
		val nInputs = job.plan pred op size
		val nOutputs = job.plan succ op size

		override protected val flushAfterProcess = false
		private val buffers: Array[Buffer[Any]] =
			1 to nOutputs map {_ => new ArrayBuffer[Any](LocalConfig.initialBufferCapacity)} toArray
		private var memoryConsumed: Long = 0

		def emit[T](outputnum: Int, data: T)(implicit m: Manifest[T]): Unit = {
			buffers(outputnum) append data
			val dataops = Data.opsFor[T](m.toString)
			memoryConsumed = memoryConsumed + dataops.memorySize(data)
			if(memoryConsumed >= LocalConfig.maxBufferSize)
				flush
		}

		private lazy val successors = job.plan succ op
		private lazy val succinputs = successors map {job.plan pred _ indexOf op}

		protected def flush = {
			log.slf4j debug ("%s(%d): flushing %d MB of output data" format (op, hashCode, memoryConsumed/(1024*1024)))
			for(outnum <- 0 until nOutputs) { if(! buffers(outnum).isEmpty) {
				val targetop = successors(outnum)
				val targetrunner = job.actors(targetop)
				val targetinputnum = succinputs(outnum)
				targetrunner ! (targetinputnum, buffers(outnum))
				log.slf4j.debug("%s(%d) flushed data on output %d" format (op, hashCode, outnum))
				buffers update(outnum, new ArrayBuffer(buffers(outnum).size))
				//buffers(outnum) clear
			}}
			memoryConsumed = 0
		}

		def emitEOF = {
			for(outnum <- 0 until nOutputs) {
				val targetop = successors(outnum)
				val targetrunner = job.actors(targetop)
				targetrunner ! EOF
			}
		}

		override def preRestart(reason: Throwable, message: Option[Any]) = reason match {
			case reason: Exception => job.errors += reason
			case _ => ()
		}

		override def finish = {
			super.finish
			nbfinished = nbfinished + 1
		}

		op init this
	}
}

// Altered version for use as local sub-jobs inside a larger distributed job:
class LocalSubJob(p: Plan) extends LocalJob(p) {
	private val jobInputs = plan.inputs map actors toIndexedSeq
	def inject(inputNum: Int, data: Seq[Any]) = {
		jobInputs(inputNum) ! (0, data)
	}
	override def start = {
		//val supervised = actors.values map {Supervise(_, Temporary)} toList;
		//supervisor = Supervisor(SupervisorConfig(OneForOneStrategy(classOf[Throwable] :: Nil), supervised))
	}
	override def waitDone = {
		jobInputs foreach {_ ! BOF}
		while(!finished) {
			Thread sleep 500
		}
	}
}
