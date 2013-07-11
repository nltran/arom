resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "retronym's repo" at "http://retronym.github.com/repo/releases"

libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.5"

libraryDependencies += "com.typesafe.akka" % "akka-remote" % "2.0.5"

libraryDependencies += "com.typesafe" % "config" % "1.0.1"

libraryDependencies += "org.scala-tools.sbinary" %% "sbinary" % "0.4.0"

libraryDependencies += "org.eclipse.jetty" % "jetty-server" % "7.3.0.v20110203"

libraryDependencies += "org.scala-lang" % "scala-swing" % "2.9.0"

libraryDependencies += "net.sf.jung" % "jung-visualization" % "2.0.1"

libraryDependencies += "net.sf.jung" % "jung-graph-impl" % "2.0.1"

scalacOptions += "-Yinfer-argument-types"

