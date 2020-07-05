#!/usr/bin/env bash

/root/soft/bigdata/kafka/bin/zookeeper-server-start.sh -daemon /root/soft/bigdata/kafka/config/zookeeper.properties

/root/soft/bigdata/kafka/bin/kafka-server-start.sh /root/soft/bigdata/kafka/config/server.properties

/root/soft/bigdata/kafka/bin/kafka-topics.sh --zookeeper 192.168.23.129:2181 --create --replication-factor 1 --topic click-trace --partitions 1
/root/soft/bigdata/kafka/bin/kafka-topics.sh --zookeeper 192.168.23.129:2181 --create --replication-factor 1 --topic new-article --partitions 1
