import sbt._
 
class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
    val akkaRepo = "Akka Repo" at "http://repo.akka.io/repository"
    val akkaPlugin = "se.scalablesolutions.akka" % "akka-sbt-plugin" % "1.0"
    val proguard = "org.scala-tools.sbt" % "sbt-proguard-plugin" % "0.0.5"
    val retronymSnapshotRepo = "retronym's repo" at "http://retronym.github.com/repo/releases"
    val onejarSBT = "com.github.retronym" % "sbt-onejar" % "0.2"
    //lazy val eclipse = "de.element34" % "sbt-eclipsify" % "0.7.0"
}

