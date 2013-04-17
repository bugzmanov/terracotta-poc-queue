#!/bin/sh

TC_CLASSPATH=$CLASSPATH:target/terracotta-sql-1.0-SNAPSHOT-jar-with-dependencies.jar

$JAVA_HOME/bin/java -classpath $TC_CLASSPATH org.ken.terracotta.main.MysqlCrawler
