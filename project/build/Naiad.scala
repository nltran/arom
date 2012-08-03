import sbt._
import com.github.retronym.OneJarProject
//import de.element34.sbteclipsify._


abstract class AromProject(info: ProjectInfo) extends DefaultProject(info) with AkkaProject with ProguardProject {// with OneJarProject {//with Eclipsify{
	val akkaRemote = akkaModule("remote")
	override def compileOptions = super.compileOptions ++ compileOptions("-Yinfer-argument-types", "")
    override def proguardOptions = proguardKeepMain(mainClass.get) :: "-keep public class *" :: Nil

    override def proguardInJars = {
        val ret = (super.proguardInJars filter {s => !(s.toString endsWith "pig.jar")}) +++ scalaLibraryPath
        println(ret)
        println("-----")
        ret
    }

    override def mainClass = Some("acme.FizzleBlaster")
}

class Arom(info: ProjectInfo) extends DefaultProject(info) {
    override def shouldCheckOutputDirectories = false

	lazy val slave = project("arom-core", "aromslave", new AromSlave(_))
	
	override def mainClass = Some("org.pig.arom.PigNaiadCompiler")

	class AromSlave(info: ProjectInfo) extends AromProject(info) {
		override def mainClass = Some("org.arom.core.distributed.RemoteRuntime")
		val a = "org.scala-lang" % "scala-swing" % "2.8.1"
		val b = "org.eclipse.jetty" % "jetty-server" % "7.3.0.v20110203"
		val c = "net.sf.jung" % "jung-visualization" % "2.0.1"
		val d = "net.sf.jung" % "jung-graph-impl" % "2.0.1"
	}


}

