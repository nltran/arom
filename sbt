#!/bin/sh
java -Dconf.file=arom.conf -Xmx1096M -jar sbt-launch.jar "$@"

