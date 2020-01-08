package antibot

import pureconfig._
import pureconfig.generic.auto._
import DataFormat._

object Config {
  lazy val config = ConfigSource.default.load[Config]
    .left.map(System.err.println)
    .left.map(_ => System.exit(1))
    .right.get

  case class Config(kafka: Kafka, cassandra: Cassandra, redis: Redis) {
    override def toString: String = ConfigWriter[Config].to(this).unwrapped().toString

    def redisFormat[F: DataFormat](f: F) = f.redisFormat(redis.table, redis.key, redis.ttl)
    def kafkaFormat[F: DataFormat](f: F) = f.kafkaFormat(kafka.brokers, kafka.topic)
  }

  case class Kafka(brokers: String, topic: String)

  case class Cassandra(keyspace: String, table: String)

  case class Redis(host: String, port: String, table: String, key: String, ttl: String)

  def setProperties(redisPort: Int, kafkaPort: Int, kafkaTopic: String): Unit = {
    System.setProperty("redis.host", "localhost")
    System.setProperty("redis.port", s"$redisPort")
    System.setProperty("redis.table", "bots")
    System.setProperty("redis.key", "ip")
    System.setProperty("redis.ttl", "600")
    System.setProperty("kafka.brokers", s"localhost:$kafkaPort")
    System.setProperty("kafka.topic", kafkaTopic)
  }
}