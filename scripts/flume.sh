#!/usr/bin/env bash

export JAVA_HOME=/root/soft/bigdata/jdk
export HADOOP_HOME=/root/soft/bigdata/hadoop
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin

/root/soft/bigdata/flume/bin/flume-ng agent -c /root/soft/bigdata/flume/conf -f /root/soft/bigdata/flume/conf/collect_click.conf -Dflume.root.logger=INFO,console -name a1
