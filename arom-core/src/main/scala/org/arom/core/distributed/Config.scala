// (c) 2010-2011 Arthur Lesuisse

package org.arom.core.distributed


object Config {
	import akka.config.Config.config
	
	val master = config getString "akka.arom.master" get
	val slaves = config getList "akka.arom.slaves"
	val port = config getInt "akka.arom.port" get
	val classpathPort = config getInt "akka.arom.classpath-port" get
	val statusPort = config getInt "akka.arom.status-port" get
	val hostCapacity = config getInt "akka.arom.host-capacity" get
	
	val httpStatus = config getBool "akka.arom.http-status" get
	val swingStatus = config getBool "akka.arom.swing-status" get
	val dumpStatus = config getBool "akka.arom.swing-end-status" getOrElse false
	
	object Slave {
		private val MiB = 1024 * 1024
		val maxMessageSize = (config getInt "akka.arom.slave.max-message-mb" get) * MiB
		val outputCacheLowWatermark = (config getInt "akka.arom.slave.output-cache-low-water-mb" get) * MiB
		val outputCacheHighWatermark = (config getInt "akka.arom.slave.output-cache-high-water-mb" get) * MiB
	}
}
