3 demo apps are here. to run them please set TC_HOME (terracotta location) and JAVA_HOME(jre location) enviroment variables


1 - crawler.sh - this will just create dummy terra_test table and will check amount of new records added in it in last 10 secs

2 - master.sh - this is sql statements publisher. it accepts one parameter - the amount of thread to run.
    the perfomance can be seen in perfstat.log. Tha publishers tag is "publisher"
    it is assumed that there will be only one publisher per cluster
3 - worker.sh - this is sql writer, that registers its own queue and then collects records to execute against database in cycle.
    the number of workers is not limited. perfomance can be seen in perfstat.log with tag "writer-ClientID[xx]" where xx - is id of the writers node


project requires java 6, maven 2

to rebuild use

mvn assembly:assembly