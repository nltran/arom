// (c) 2010-2011 Arthur Lesuisse

package org.arom.core.distributed

import java.net.{ NetworkInterface, InetAddress }
import org.arom.util.Logging
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object Config extends Logging {
	import com.typesafe.config._
	val configFile = System.getProperty("config.file")

	val config = ConfigFactory.load(if (configFile == null) "arom.conf" else configFile)

	def toScalaList[T](l : ConfigList) : List[T] = {
		l.unwrapped().asScala.toList.map((s: java.lang.Object) => s.asInstanceOf[T])
	}

	val master = config getString "akka.arom.master"
	val slaves = toScalaList[String](config getList "akka.arom.slaves")
	val port = config getInt "akka.arom.port"
	val slavePort = config getInt "akka.arom.slave-port"
	val classpathPort = config getInt "akka.arom.classpath-port"
	val statusPort = config getInt "akka.arom.status-port"
	val hostCapacity = config getInt "akka.arom.host-capacity"
	
	val httpStatus = config getBoolean "akka.arom.http-status"
	val swingStatus = config getBoolean "akka.arom.swing-status" 
	val dumpStatus =
		try {
			config getBoolean "akka.arom.swing-end-status"
		} catch {
			case _ => false
		}

	/**
	 * The local IP that is inside the slave list. It is on this IP
	 * that this slave will need to listen.
	 */
	val slaveAddressConfig = {
		val addresses = NetworkInterface.getNetworkInterfaces() flatMap ((e: NetworkInterface) => e.getInetAddresses)
		val slave = addresses filter ((a: InetAddress) => slaves contains (a getHostAddress))
		val address = if (slave hasNext)
				slave.next.getHostAddress
			else
				InetAddress.getLocalHost.getHostAddress
		log.slf4j info "This slave will listen on %s (define slave.akka.remote.netty.hostname to avoid this)".format(address)
		ConfigFactory.parseString("akka.remote.netty.hostname=\"%s\"".format(address))
	}

	val slaveConfig = config.getConfig("slaves").withFallback(slaveAddressConfig).withFallback(config)

	object Slave {
		private val MiB = 1024 * 1024
		val maxMessageSize = (config getInt "akka.arom.slave.max-message-mb") * MiB
		val outputCacheLowWatermark = (config getInt "akka.arom.slave.output-cache-low-water-mb") * MiB
		val outputCacheHighWatermark = (config getInt "akka.arom.slave.output-cache-high-water-mb") * MiB
	}
}
