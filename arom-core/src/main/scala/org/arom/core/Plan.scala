// (c) 2010-2011 Arthur Lesuisse

package org.arom.core

import scala.collection.mutable.{HashMap}
import org.arom.util._

trait Plan {	
	val vertices: List[Node]
	val edges: List[(Node, Node)]
	val inputs: List[Node]
    val outputs: List[Node]

    private def makeMultiMap(edges: Seq[(Node, Node)]): Map[Node, Seq[Node]] = {
        val grouped = edges groupBy {case (from, to) => from}
        val emptys = vertices filterNot grouped.contains map {(_, Nil)}
        (grouped map {case (from, tos) => (from, tos map {_._2})}) ++ emptys
    }

    lazy val succ = makeMultiMap(edges)
    lazy val pred = makeMultiMap(edges map {case (from, to) => (to, from)})
	
	def ^(n: Int): Plan = new ^(this, n)
	def >=(p: Plan): Plan = new >=(this, p)
	def >>(p: Plan): Plan = new >>(this, p)
	def ||(p: Plan): Plan = new ||(this, p)
	
	def copy: Plan		
	
	def decompose: String = "(" + (this match {
		case l >> r => l.decompose + " >> " + r.decompose
		case l >= r => l.decompose + " >= " + r.decompose
		case l || r => l.decompose + " || " + r.decompose
		case l ^ n => l.decompose + " ^ " + n
		case x => x.toString
	}) + ")"
	
	def find(func: PartialFunction[Plan, Unit]): Unit = {
		if(func isDefinedAt this)
			func(this)		
		this match {
			case p: CompositePlan => p.subplans foreach {_ find func}
			case _ => ()
		}
	}
	
	implicit object PlanOps extends GraphOps[Plan, Node] { // type class for use with generic graph-algorithms
	    def roots(g: Plan) = g.inputs
	    def vertices(g: Plan) = g.vertices
	    def successors(g: Plan, v: Node) = g succ v
	    def predecessors(g: Plan, v: Node) = g pred v
	}
}

trait Node extends Plan {
	val vertices = List(this)
	val edges = Nil
	val inputs = vertices
	val outputs = vertices
	override def copy: Node
}

object NoPlan extends Plan {
	val vertices = Nil
	val edges = Nil
	val inputs = Nil
	val outputs = Nil
	def copy = this
}

sealed trait CompositePlan extends Plan {
	def copy: Plan = doCopy(this, new HashMap)	
	
	private def doCopy(current: Plan, copies: HashMap[Node, Node]): Plan = {
		// special copy behavior is required for composite plans so that nodes that are present
		// in both parts of a composite plan are copied only once as required.
		current match {
			case n: Node =>				
				if(copies contains n) {
					copies(n)
				} else {
					val mycopy = n.copy
					copies.update(n, mycopy)
					mycopy
				}
			case l >> r => doCopy(l, copies) >> doCopy(r, copies)
			case l >= r => doCopy(l, copies) >= doCopy(r, copies)
			case l || r => doCopy(l, copies) || doCopy(r, copies)
			case p ^ n => doCopy(p, copies) ^ n			
		}
	}
	
	val subplans: Seq[Plan]
}

case class ^(subplan: Plan, n: Int) extends CompositePlan {	
	private val copies = subplan :: ((2 to n) map {_ => subplan.copy} toList);	
	val vertices = copies flatMap {_ vertices}
	val edges = copies flatMap {_ edges}
	val inputs = copies flatMap {_ inputs}
	val outputs = copies flatMap {_ outputs}
	val subplans = subplan :: Nil
}

case class >=(src: Plan, dest: Plan) extends CompositePlan { // TODO: vérifier si bien disjoints
	val vertices = src.vertices ::: dest.vertices
	val edges = {
		val ins = dest.inputs
		val outs = src.outputs
		src.edges ++ dest.edges ++ (	
			if(outs.size <= ins.size)				
				ins.zipWithIndex map {case (op, i) => (outs(i % outs.size), op)}
			else
				outs.zipWithIndex map {case (op, i) => (op, ins(i % ins.size))}	
		)
	}
	val inputs = src.inputs
	val outputs = dest.outputs
	val subplans = src :: dest :: Nil
}

case class >>(src: Plan, dest: Plan) extends CompositePlan { // TODO: vérifier si bien disjoints
	val vertices = src.vertices ::: dest.vertices
	val edges = src.edges ++ dest.edges ++ (
		src.outputs flatMap {from => dest.inputs map {to => (from, to)}}
	)
	val inputs = src.inputs
	val outputs = dest.outputs
	val subplans = src :: dest :: Nil
}

case class ||(src: Plan, dest: Plan) extends CompositePlan { // TODO: vérifier si bien acyclique
	val vertices = src.vertices ::: dest.vertices distinct
	val edges = src.edges ::: dest.edges distinct
	val inputs = (src.inputs filter {in => dest.inputs.contains(in) || ! dest.vertices.contains(in)}) ::: 
				 (dest.inputs filter {in => src.inputs.contains(in) || ! src.vertices.contains(in)}) distinct
	val outputs = (src.outputs filter {out => dest.outputs.contains(out) || ! dest.vertices.contains(out)}) ::: 
				  (dest.outputs filter {out => src.outputs.contains(out) || ! src.vertices.contains(out)}) distinct
	val subplans = src :: dest :: Nil
}


