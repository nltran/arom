// (c) 2010-2011 Arthur Lesuisse

package org.arom.core.distributed

import java.net.{ URL, URLClassLoader, InetAddress }

import akka.actor.{ Actor, ActorRef }
import org.arom.util.Logging

import org.arom.core._
import org.arom.util._
import collection.mutable.{ Buffer, HashMap, HashSet, ListBuffer }

import java.util.Calendar

object RemoteMaster extends Logging {
  protected val jobs = new ListBuffer[DistributedJob]

  private lazy val service = Actor.remote start (Config.master, Config.port)

  var statusViewer: Thread = null

  case object Start

  class DistributedJob(plan: Plan) extends AromJob {
    service;
    jobs append this

    private case class Runner(val host: String) {
      var usage: Int = 0
      val actor: Option[ActorRef] = if (host equals InetAddress.getLocalHost.getHostName)
        None
      else
        Some(Actor.remote actorFor ("remote-runtime", host, Config.port))
    }

    private val runtimes = HashMap(Config.slaves map { host => (host, Runner(host)) }: _*)

    private val planAlgos = new GraphAlgorithm(plan)(plan.PlanOps)
    //private val runqueue = Queue(planAlgos topological { case node: Node => node }: _*)

    private val ready = HashSet[Node](plan.vertices filter { plan pred _ isEmpty }: _*)
    
    private val leaves = HashSet[Node](plan.vertices filter{plan succ _ isEmpty}: _*)
    log.slf4j info (Calendar.getInstance().getTime() + " Began initdescendents")
//    planAlgos.initDescendents3(leaves)
//    leaves foreach {n => planAlgos initDescendents4 n}
    val degrees = planAlgos allCumOutDegrees;
    log.slf4j info (Calendar.getInstance().getTime() + " Finished initdescendents")

//    degrees foreach (println _)
    private var totalUsage = 0
    private val totalCapacity = runtimes.size * Config.hostCapacity

    private abstract class TaskState
    private case object Pending extends TaskState
    private abstract case class Hosted(val runtime: Runner, val opactor: ActorRef) extends TaskState
    private case class Running(r: Runner, a: ActorRef) extends Hosted(r, a)
    private case class Finished(r: Runner, a: ActorRef) extends Hosted(r, a)
    private case class Failed(r: Runner, a: ActorRef) extends Hosted(r, a)
    private case class SchedulingInProgress(r: Runner) extends TaskState

    private val ids = Map[Node, String]((plan.vertices zipWithIndex) map { case (node, index) => (node, node.toString + index) }: _*) // contains (node, op. id) 
    private val nodes: Map[String, Node] = ids map { case (node, id) => (id, node) } // contains (op. id, node)

    private val states: HashMap[Node, TaskState] = HashMap(plan.vertices zip (1 to plan.vertices.length map { _ => Pending }): _*)
    private val stats = HashMap[Node, Map[String, Any]]()

    private type NodePolicy = Node => Double
    private type HostPolicy = (Node, Runner) => Double
    class ScoreOrdering[T, S](implicit o: Ordering[S]) extends Ordering[(T, S)] { def compare(a, b) = o compare (b _2, a _2) }
    private implicit val nodeScoreOrdering = new ScoreOrdering[Node, Double]
    private implicit val hostScoreOrdering = new ScoreOrdering[(Node, Runner), Double]

    private val hints = HashSet[(Node, Node)]()
    def addHints(h: Set[(Node, Node)]) = hints ++= h
    
    private def chooseNext: (Node, Runner) = {
      log.slf4j info (Calendar.getInstance().getTime() + " Began choosenext")
      val nodePolicies: Seq[NodePolicy] = Seq(
        // nodes with higher cumulated output degree are prioritized
        { node => 1.0 + (degrees.get(node).get.toDouble /plan.edges.size.toDouble) }, // ((planAlgos cumulatedOutDegree node).toDouble / plan.edges.size.toDouble) },
          
//          { node => 1.0 + (node.outputs.size.toDouble/plan.edges.size.toDouble) }, // ((planAlgos cumulatedOutDegree node).toDouble / plan.edges.size.toDouble) },
          
//          { node =>  log.slf4j info (Calendar.getInstance().getTime() + " Entering 1"); 1.0 + ((planAlgos cumulatedOutDegree2 node) / plan.edges.size.toDouble) },
          
        // synchronous operators shouldn't be started until all immediate predecessors are running
        {
          case node: SynchronousOperator if plan pred node exists { states(_) == Pending } => 0.0
          case _ => 1.0
        },
        // nodes with more running predecessors have priority
        { node => 
          val preds = planAlgos ancestors node
          if (preds isEmpty)
            1.0
          else
            1.0 + ((preds count { states(_: Node) != Pending }).toDouble / preds.size.toDouble)
        },
        // "early" nodes have higher priority
        {
          case _: Early => 2.0
          case _ => 1.0
        })

      // returns whether an operator is scheduled or running on a host
      def on(node: Node, host: String) = states(node) match {
        case Hosted(Runner(h), _) => h == host
        case SchedulingInProgress(Runner(h)) => h == host
        case _ => false
      }
      def hintsFrom(node: Node) = hints collect { case (from, to) if from eq node => to }
      def hintsTo(node: Node) = hints collect { case (from, to) if to eq node => from }
      val hostPolicies: Seq[HostPolicy] = Seq(
        // A source node that can't be scheduled next to its input data loses priority
        { (node, runner) =>
          node match {
            case op: Locatable if !(op locateAmong Config.slaves contains runner.host) => 0.5
            case op: Locatable if runner.usage == 0 => 10.0
            case _ => 1.0
          }
        },
        // A node which can be scheduled next to one of its immediate predecessors is prioritized
        { (node, runner) => if (plan pred node exists { on(_, runner.host) }) 5.0 else 1.0 },
        // less busy hosts are encouraged
        { (_, runner) =>
          if (runner.usage >= Config.hostCapacity) -100.0 //0.25 
          else (1.5 - runner.usage.toDouble / Config.hostCapacity.toDouble)
        },
        // scheduling hints are taken into account
        { (node, runner) =>
          if (hintsTo(node) forall { on(_, runner.host) }) 1.0 else 0.0
        },
        { (node, runner) =>
          if (hintsFrom(node) forall { succ =>
            hintsTo(succ) - node forall { n => on(n, runner.host) || (states(n) eq Pending) }
          }) 1.0 else 0.0
        })
      val nodes = ready.toSeq
      log.slf4j info (Calendar.getInstance().getTime() +" Constructed nodes")

      val nodeScores = nodes zip (nodes map { n => (1.0 /: nodePolicies) { _ * _(n) } })
      log.slf4j info (Calendar.getInstance().getTime() +" Constructed nodeScore (nodePolicies)")
      println("brolprio")
      println((nodeScores.min) _2)
      val hosts = runtimes.values filter { _.usage < Config.hostCapacity } // filter was commented initially
      log.slf4j info (Calendar.getInstance().getTime() +" Constructed hosts")
      val nodeHostPairs = nodeScores.sorted take 100 flatMap { case (n, s) => hosts map { (n, s, _) } }
      log.slf4j info (Calendar.getInstance().getTime() +" Constructed nodeHostPairs")
      val hostScores = nodeHostPairs map { case (n, s, h) => ((n, h), (s /: hostPolicies) { _ * _(n, h) }) }
      log.slf4j info (Calendar.getInstance().getTime() +" Constructed hostScores")

      //            nodeScores.sorted foreach println
      // hostScores.sorted foreach println
      //			println((hostScores.min) _2)
      log.slf4j info (Calendar.getInstance().getTime() +" Finished choosenext")

      (hostScores.sorted head) _1

    }

    private def scheduleNext: Unit = {
      val (node, finalRT) = chooseNext
      states update (node, SchedulingInProgress(finalRT))
      val predecessors = plan pred node
      val successors = plan succ node

      ready -= node
      // a node becomes ready when all its predecessors are scheduled (or at least one if the node is declared "early"):
      for (succ <- successors if (states(succ) eq Pending) && ((plan pred succ map states forall { _ ne Pending }) || succ.isInstanceOf[Early]))
        ready += succ

      // send operator to worker host
      val nIns = predecessors size
      val nOuts = successors size
      val outToInMapping = successors map { plan pred _ indexOf node } toIndexedSeq
      val id = ids(node)

      finalRT.usage = finalRT.usage + 1
      if (finalRT.usage <= Config.hostCapacity)
        totalUsage = totalUsage + 1

      log.slf4j info ("Sending %s to %s with usage %s" format (id, finalRT.host, finalRT.usage))
      val newtask = RemoteRuntime.RegisterTask(id, node, nIns, nOuts, outToInMapping)

      if (finalRT.actor eq None) {
        finishScheduling(id, RemoteRuntime registerTask newtask)
      } else {
        finalRT.actor.get ! newtask
      }
      log.slf4j info ("finished scheduleNext")
    }

    def addStaticInit(func: () => Unit) = runtimes.values map { _.actor } foreach {
      case None => func()
      case Some(actor) => actor ! RemoteRuntime.StaticInit(func)
    }

    //private val predsToUpdate = HashMap[Node, List[Node]]()
    private val edgesToUpdate = HashSet[(Node, Node)](plan.edges: _*)
    private def finishScheduling(id: String, actor: ActorRef) = {
      val node = nodes(id)
      val rt = states(node) match { case SchedulingInProgress(rt) => rt }
      log.slf4j info ("Finishing scheduling of %s on %s" format (id, rt.host))
      states update (node, Running(rt, actor))
      edgesToUpdate.toSeq foreach {
        case edge @ (from, to) => (states(from), states(to)) match {
          case (Hosted(_, actor), Hosted(Runner(host), _)) =>
            val outnum = plan succ from indexOf to
            log.slf4j info ("SetOutput %s of %s to %s" format (outnum, ids(from), ids(to)))
            actor ! RemoteRuntime.SetOutput(outnum, host, ids(to))
            edgesToUpdate -= edge
          case _ => ()
        }
      }

      // if node is an input, send BOF to trigger processing
      if (plan pred node isEmpty) { actor ! BOF }

      log.slf4j info ("Scheduled %s on %s" format (id, rt.host))
    }

    private var jars: Seq[URL] = Nil
    def setJars(newjars: Seq[URL]) = jars = newjars

    private var startTime: Long = -1
    def start = {
      ClasspathServer.start;
      if (Config.swingStatus) {
        statusViewer = new SwingStatusViewer;
        statusViewer start;
      }
      if (Config.httpStatus)
        initStatusServer;

      val masterActor = Actor actorOf new Actor {
        def receive = {
          case Start =>
            while (totalUsage < totalCapacity && !ready.isEmpty)
              scheduleNext
          case RemoteRuntime.Finished(id) =>
            val node = nodes(id)
            val (rt, act) = states(node) match { case Running(rt, act) => (rt, act) }
            states.update(node, Finished(rt, act))
            rt.usage = rt.usage - 1
            totalUsage = totalUsage - 1
            log.slf4j info ("Node %s on %s finished" format (id, rt.host))
            val numfinished = states collect { case (_, Finished(_, _)) => 1 } size
            val percent = (100 * numfinished) / states.size
            log.slf4j info ("%d%% completed" format percent)
            if (!ready.isEmpty) { scheduleNext }
            else if (numfinished == states.size) {
              runtimes.values foreach { rt =>
                log.slf4j info ("Unloading %s" format (rt))
                if (rt.actor ne None)
                  rt.actor.get ! RemoteRuntime.Unload
              }
              _finished = true
              ClasspathServer.stop
              //self.stop							
              //Actor.remote shutdown;							
            }
          case RemoteRuntime.Stats(id, statlst) =>
            stats update (nodes(id), Map(statlst: _*))

          case RemoteRuntime.Failed(id, reason) =>
            val node = nodes(id)
            states(node) match { case Hosted(runner, actor) => states update (node, Failed(runner, actor)) }
            reason foreach errors.+=

          case id: String =>
            val rt = states(nodes(id)) match { case SchedulingInProgress(rt) => rt }
            finishScheduling(id, Actor.remote actorFor (id, rt.host, Config.port))
        }
      }
      startTime = new java.util.Date().getTime
      Actor.remote register ("remote-master", masterActor)
      masterActor ! Start
    }

    val errors = Buffer[Throwable]()
    private var _finished = false
    def finished = _finished
    def waitDone = {
      while (!_finished) { Thread sleep 500 }
      val ellapsed: Double = new java.util.Date().getTime - startTime
      log.slf4j info "Distributed job finished in %f seconds".format(ellapsed / 1000.0)
      if (Config.dumpStatus) {
        (new SwingStatusViewer) start //TODO: better design for that
      }
    }

    def statusGraph = {
      Graphviz.toDot(plan, { node =>
        states(node) match {
          case Pending => """[label="%s"]""" format ids(node)
          case SchedulingInProgress(rt) => """[style=filled, fillcolor=%s, label="%s@%s"]""" format ("grey", ids(node), rt.host)
          case Running(rt, _) => """[style=filled, fillcolor=%s, label="%s@%s"]""" format ("yellow", ids(node), rt.host)
          case Finished(rt, _) => """[style=filled, fillcolor=%s, label="%s@%s"]""" format ("green", ids(node), rt.host)
          case Failed(rt, _) => """[style=filled, fillcolor=%s, label="%s@%s"]""" format ("red", ids(node), rt.host)
        }
      }, {
        case (from, to) => stats get from match {
          case Some(stat) => stat get "outputSizes" match {
            case Some(sizes: Seq[(Int, Int)]) =>
              val outnum = plan.succ(from) indexOf to
              var size = sizes collect { case (o, s) if o == outnum => s } head
              var suffix = "bytes" :: "KiB" :: "MiB" :: "GiB" :: "TiB" :: Nil
              while ((size >= 1024) && (suffix.size > 1)) { size = size / 1024; suffix = suffix tail }
              """[label="%s %s"]""" format (size, suffix head)
            case None => ""
          }
          case _ => ""
        }
      })
    }

    class SwingStatusViewer extends Thread {
      override def run = {
        val displayer = planAlgos viewer;
        displayer setNodeColors {
          case node: Node => states(node) match {
            case Pending => java.awt.Color.WHITE
            case SchedulingInProgress(rt) => java.awt.Color.GRAY
            case Running(_, _) => java.awt.Color.YELLOW
            case Finished(_, _) => java.awt.Color.GREEN
            case Failed(_, _) => java.awt.Color.RED
          }
        }
        displayer setNodeLabels {
          case node: Node => states(node) match {
            case Hosted(rt, _) => "%s@%s" format (ids(node), rt.host)
            case _ => ids(node)
          }
        }
        displayer setEdgeLabels {
          case edge @ (from: Node, to: Node) => stats get from match {
            case Some(stat) => stat get "outputSizes" match {
              case Some(sizes: Seq[(Int, Int)]) =>
                val outnum = plan succ from indexOf to
                var size = sizes collect { case (o, s) if o == outnum => s } head
                var suffix = "bytes" :: "KiB" :: "MiB" :: "GiB" :: "TiB" :: Nil
                while ((size >= 1024) && (suffix.size > 1)) { size = size / 1024; suffix = suffix tail }
                "%s %s" format (size, suffix head)
              case None => ""
            }
            case _ => ""
          }
        }
        displayer startup;
        while (true) {
          Thread sleep 1000
          displayer repaint
        }
        //displayer shutdown
      }
    }

    import org.eclipse.jetty.server.Request
    import org.eclipse.jetty.server.handler.AbstractHandler
    import javax.servlet.http.{ HttpServletRequest, HttpServletResponse }
    import org.apache.commons.io.IOUtils
    import org.eclipse.jetty.server.Server
    import org.eclipse.jetty.util.thread.QueuedThreadPool

    // This is a HTTP server used to serve classes to cluster nodes for remote class loading. Jetty 7.3
    object ClasspathServer extends Server(Config.classpathPort) {
      this setThreadPool new QueuedThreadPool(400)
      this setHandler new AbstractHandler {
        lazy val loader = new URLClassLoader(jars.toArray, getClass() getClassLoader);
        def handle(target: String, baseReq: Request, req: HttpServletRequest, resp: HttpServletResponse) = {
          //print("ASKED FOR CLASS " + target + "... "); System.out.flush
          loader getResourceAsStream (target substring 1) match {
            case null => resp setStatus 404 //; println("NOT SERVED")
            case input => //println("SERVED")
              resp setContentType "application/x-java-class"
              resp setStatus 200
              val output = resp getOutputStream;
              IOUtils copy (input, output)
              input close;
              output close
          }
          baseReq setHandled true
        }
      }
    }

    // This HTTP server serves status informations about running jobs
    def initStatusServer = {
      statusServer stop;
      statusServer setHandler new AbstractHandler {
        def handle(target: String, baseReq: Request, req: HttpServletRequest, resp: HttpServletResponse) = {
          target match {
            case _ =>
              val graph = statusGraph
              val compiled = Graphviz.compileDot(graph, "svg")
              resp setContentType "image/svg+xml"
              resp setStatus 200
              IOUtils.copy(compiled, resp.getOutputStream)
              compiled close;
              resp.getOutputStream close;

            //case _ => resp setStatus 404
          }
          baseReq setHandled true
        }
      }
      statusServer start
    }
  }

  import org.eclipse.jetty.server.Server
  lazy val statusServer = { val server = new Server(Config.statusPort); server start; server }
}

