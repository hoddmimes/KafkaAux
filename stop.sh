#!/bin/bash
sudo kill -9 $(ps -aux | grep kafka.Kafka | grep -v "grep" | awk {'print "kafka " $2'})
sudo kill -9 $(ps -aux | grep zookeeper.server | grep -v "grep" | awk {'print "zookeeper " $2'})

