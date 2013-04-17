#!/bin/sh

TC_CLASSPATH=$CLASSPATH:target/terracotta-sql-1.0-SNAPSHOT-jar-with-dependencies.jar

$TC_HOME/bin/dso-java.sh -classpath $TC_CLASSPATH -server -Xms256m -Xmx256m -Dtc.install-root=$TC_HOME -Dtc.config=tc-config.xml org.ken.terracotta.main.StartSqlWriter
