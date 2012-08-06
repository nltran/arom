#!/bin/sh
java -Dakka.config=conf/akka.conf -Xmx1096M -jar sbt-launch.jar "$@"

