#!/bin/bash
cd
KAFKADIR=/usr/local/kafka_2.12-2.4.0
sudo $KAFKADIR/bin/zookeeper-server-start.sh $KAFKADIR/config/zookeeper.properties &
sudo $KAFKADIR/bin/kafka-server-start.sh $KAFKADIR/config/server.properties &

