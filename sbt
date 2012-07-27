#!/bin/sh
java -Dakka.config=/home/nltran/naiad/akka.conf  -Xmx12096M -XX:+HeapDumpOnOutOfMemoryError -Dcom.sun.management.jmxremote.port=3333 \
     -Dcom.sun.management.jmxremote.ssl=false \
     -Dcom.sun.management.jmxremote.authenticate=false \
-jar sbt-launch.jar "$@"

