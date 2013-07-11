// (c) 2010-2011 Arthur Lesuisse

package org.arom.util
import org.arom.core.Node

import scala.collection.mutable.{ ListBuffer, Stack, HashMap, Queue, HashSet }

trait GraphOps[GraphType, NodeType] {
  //type NodeType
  def roots(g: GraphType): Seq[NodeType]
  def vertices(g: GraphType): Seq[NodeType]
  def successors(g: GraphType, v: NodeType): Seq[NodeType]
  def predecessors(g: GraphType, v: NodeType): Seq[NodeType]
}

class GraphAlgorithm[GraphType, NodeType](g: GraphType)(implicit val ops: GraphOps[GraphType, NodeType]) {
  def topological[T](func: NodeType => T) = {
    val result = new ListBuffer[T]
    val stack = new Stack[NodeType]
    val indegrees = new HashMap[NodeType, Int]
    stack pushAll (ops roots g)
    while (!stack.isEmpty) {
      val current = stack pop;
      result append func(current)
      stack pushAll (ops successors (g, current) filter { n =>
        indegrees getOrElse (n, ops predecessors (g, n) size) match {
          case 1 => true
          case k => indegrees update (n, k - 1); false
        }
      })
    }
    result
  }
  
  def allCumOutDegrees = {
       val nodes = topological(x => x) reverse;
       val desc = new HashMap[NodeType, Set[NodeType]]
       val degrees = new HashMap[NodeType, Int]
       for(node <- nodes) {
         val succ = ops successors(g, node) toSet;
         desc update(node, succ ++ (succ flatMap desc))
         degrees update(node, desc(node).size)
       }
       degrees
   }
  
//  def initDescendents4(n: NodeType) = {
////    val anc = HashSet[NodeType]()
//    val queue = Queue[NodeType]( ops predecessors(g,n): _*)
//    while (!queue.isEmpty) {
//      val tmp = queue dequeue;
////      anc += tmp
//      tmp.asInstanceOf[Node].descendents += 1
//      println(tmp.asInstanceOf[Node].descendents)
//      queue enqueue (ops predecessors(g,n): _*)
//    }
////    anc.toSeq
//  }
//  
//  def initDescendents3(nodes: HashSet[Node]) {
//    val queue = Queue[Node]()
//    nodes foreach{n => queue enqueue(n.inputs: _*)}
////    nodes foreach{n => n.inputs.foreach(queue enqueue(_))}
//    while(!queue.isEmpty) {
////      println("I went heree")
//      val tmp = queue dequeue;
//      tmp.descendents += 1
//      println(tmp.descendents)
//      queue enqueue (tmp.inputs: _*)
//    }
//  }
//
//  def initDescendents2(nodes: HashSet[Node]):Unit = {
//    println("I went here")
//    val anc = HashSet[Node]()
//    nodes foreach { nn =>
//      nn.inputs.foreach { nnn =>
//        nnn.descendents += 1
//        anc += nnn
//      }
//    }
//    if (!anc.isEmpty){
//      initDescendents2(anc)
//    }
//  }
//
//  def initDescendents(n: Node, c: Int): Unit = {
//    n.asInstanceOf[Node].descendents += (1 + c)
//    println("I went here " + n.asInstanceOf[Node].descendents)
//    if (!n.asInstanceOf[Node].inputs.isEmpty) {
//      n.asInstanceOf[Node].inputs.foreach { nn => initDescendents(nn, n.asInstanceOf[Node].descendents) }
//    }
//
//  }
//
//  def cumulatedOutDegree2(n: NodeType) = {
//    println(n.asInstanceOf[Node].descendents)
//    n.asInstanceOf[Node].descendents
//  }

  def cumulatedOutDegree(n: NodeType) =
    descendents(n).size

  def descendents(n: NodeType) = {
    val desc = HashSet[NodeType]()
    val queue = Queue[NodeType](ops successors (g, n): _*)
    while (!queue.isEmpty) {
      val tmp = queue dequeue;
      desc += tmp
      queue enqueue (ops successors (g, tmp): _*)
    }
    desc.toSeq
  }

  def ancestors(n: NodeType) = {
    val anc = HashSet[NodeType]()
    val queue = Queue[NodeType](ops predecessors (g, n): _*)
    while (!queue.isEmpty) {
      val tmp = queue dequeue;
      anc += tmp
      queue enqueue (ops predecessors (g, tmp): _*)
    }
    anc.toSeq
  }

  def viewer = new scala.swing.SimpleSwingApplication {
    import scala.swing.MainFrame
    import scala.swing.Component
    import edu.uci.ics.jung.graph._
    import edu.uci.ics.jung.algorithms.layout._
    import edu.uci.ics.jung.visualization._
    import edu.uci.ics.jung.visualization.control._
    import edu.uci.ics.jung.visualization.decorators._
    import org.apache.commons.collections15.Transformer

    override def quit = shutdown

    private implicit def func2trans[T, U](func: T => U) = new Transformer[T, U] {
      def transform(node: T) = func(node)
    }

    private implicit def java2scala(jcomp: javax.swing.JComponent): Component = new Component {
      override lazy val peer = jcomp
    }

    private val jungGraph = new DirectedSparseGraph[NodeType, (NodeType, NodeType)]
    for (v <- ops vertices g) {
      jungGraph addVertex v
    }
    for (from <- ops vertices g; to <- ops successors (g, from)) {
      jungGraph addEdge ((from, to), from, to)
    }

    private val layout = new StaticLayout(jungGraph) {
      // This is kinda stupid, but Jung doesn't have a rank-based layout engine suitable
      // for visualizing execution graphs. Thus we use DOT to calculate vertex positions instead.
      import java.io.{ BufferedReader, InputStreamReader }
      import java.awt.Point
      private val verts = Map(ops vertices g map { v => (v.hashCode, v) }: _*)
      private val dot = new StringBuilder
      dot append "digraph g {\n\trankdir=BT;\n\tnode [shape=point];\n"
      for (from <- ops vertices g; to <- ops successors (g, from)) {
        dot append ("\t%s -> %s;\n" format (from.hashCode, to.hashCode))
      }
      dot append "}\n"
      private val reader = new BufferedReader(new InputStreamReader(Graphviz compileDot (dot.result, "dot")))
      private var line = reader.readLine
      private val pattern = """\t(\d+)\s\[pos="(\d+),(\d+)".*""".r
      while (line ne null) {
        line match {
          case pattern(node, x, y) => setLocation(verts(node.toInt), new Point(30 + x.toInt * 4, 30 + y.toInt * 2))
          case _ => ()
        }
        line = reader readLine
      }
      reader close
    }

    private val viewer = new VisualizationViewer(layout)
    private val mousectl = new DefaultModalGraphMouse[NodeType, (NodeType, NodeType)]
    mousectl setMode ModalGraphMouse.Mode.PICKING
    viewer setGraphMouse mousectl
    viewer.getRenderContext setVertexLabelTransformer new ToStringLabeller
    viewer.getRenderContext setEdgeShapeTransformer new EdgeShape.Line

    def top = new MainFrame {
      title = "Graph viewer"
      contents = new GraphZoomScrollPane(viewer)
      size = new java.awt.Dimension(1200, 800)
    }

    def setNodeColors(colors: NodeType => java.awt.Color) =
      viewer.getRenderContext setVertexFillPaintTransformer colors

    def setNodeLabels(labels: NodeType => String) =
      viewer.getRenderContext setVertexLabelTransformer labels

    def setEdgeLabels(labels: ((NodeType, NodeType)) => String) =
      viewer.getRenderContext setEdgeLabelTransformer labels

    def repaint = viewer.repaint()

    def startup = super.startup(Array())
  }
}

