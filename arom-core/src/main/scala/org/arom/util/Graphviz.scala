// (c) 2010-2011 Arthur Lesuisse

package org.arom.util

import java.io.InputStream

import org.arom.Plan
import org.arom.Node

object Graphviz {
	def toDot(plan: Plan,			 
			  nodeprops: Node => String = {_ => ""},
			  edgeprops: ((Node, Node)) => String = {_ => ""}): String = {
		val sb = new StringBuilder
		//sb append "digraph G {\n\tsize=\"20,12\"\n\tratio=\"fill\"\n"
        sb append "digraph G {\n"
		plan.vertices foreach {
			op => sb append ("\t%d %s\n" format(op hashCode, nodeprops(op)))				
		}
		plan.edges foreach {
			case (lop, rop) => sb append ("\t%d -> %d %s\n" format(lop hashCode, rop hashCode, edgeprops(lop, rop)))
		}
		sb append "}\n"
		sb.result
	}
	
	def compileDot(graph: String, format: String): InputStream = {		
		val process = Runtime getRuntime() exec ("dot -T" + format)
		process.getOutputStream write graph.getBytes
		process.getOutputStream close;		
		process.getInputStream		
	}
	
}










