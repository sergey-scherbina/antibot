#!/usr/bin/env bash

sbt package && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,\
com.datastax.spark:spark-cassandra-connector_2.11:2.4.2,com.redislabs:spark-redis:2.4.0,\
com.github.pureconfig:pureconfig_2.11:0.12.2\
  target/scala-2.11/antibot_2.11-0.1.jar \
  $*
