resolvers += Resolver.mavenLocal

name := "antibot"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "com.github.pureconfig" %% "pureconfig" % "0.12.2",
  "com.google.guava" % "guava" % "19.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.1",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2",
  "com.redislabs" % "spark-redis" % "2.4.0",

  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.3" % Test,
  "com.github.sebruck" %% "scalatest-embedded-redis" % "0.2.0" % Test,
  "com.datastax.spark" %% "spark-cassandra-connector-embedded" % "2.4.2" % Test,
  "org.apache.cassandra" % "cassandra-all" % "3.11.4" % Test,
  "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" % Test,
  "org.apache.zookeeper" % "zookeeper" % "3.4.14" % Test,
).map(_.exclude("org.slf4j", "log4j-over-slf4j")
  .exclude("ch.qos.logback", "logback-classic"))

parallelExecution in Test := false

fork := true

javaOptions ++= Seq("-Dspark.master=local[*]")