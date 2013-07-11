import sbt._
import Keys._

object AROMBuild extends Build {
    def project(id : String, base : String, settings : Seq[sbt.Project.Setting[_]] = Seq()) : Project =
        settings match {
            case Seq() => Project(id = id, base = file(base))
            case s => Project(id = id, base = file(base), settings = Defaults.defaultSettings ++ s)
        }

    lazy val root = project("arom", ".") aggregate(core, examples)

    lazy val core = project("aromslave", "arom-core",
                            Seq(mainClass in (Compile, run) := Some("org.arom.core.distributed.RemoteRuntime")))

    lazy val examples = project("arom-examples", "arom-examples") aggregate(hellojob, wordcount)

    lazy val hellojob = project("hellojob", "arom-examples",
                                Seq(mainClass in (Compile, run) := Some("org.arom.examples.HelloWorldJob"))) dependsOn(core)

    lazy val wordcount = project("wordcountjob", "arom-examples",
                                 Seq(mainClass in (Compile, run) := Some("org.arom.examples.WordCountJob"))) dependsOn(core)

    lazy val httpwordcount = project("httpwordcountjob", "arom-examples",
                                     Seq(mainClass in (Compile, run) := Some("org.arom.examples.HTTPWordCountJob"))) dependsOn(core)
}
