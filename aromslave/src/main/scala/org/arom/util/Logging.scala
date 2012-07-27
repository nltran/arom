package org.arom.util

trait Logging { // quick-and-dirty replacement for the akka.util.Logging trait removed in 1.1
	object log {
		object slf4j {
			def info(msg: String) = println("INFO: " + msg)
			def warn(msg: String) = println("WARN: " + msg)
			def error(msg: String) = println("ERROR: " + msg)
			def debug(msg: String) = println("DEBUG: " + msg)
		}
	}
}
