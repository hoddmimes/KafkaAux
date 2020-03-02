#!/bin/bash
ps -aux | grep zookeeper.server | grep -v "grep" | awk {'print "zookeeper " $2'}
ps -aux | grep kafka.Kafka | grep -v "grep" | awk {'print "kafka " $2'}
