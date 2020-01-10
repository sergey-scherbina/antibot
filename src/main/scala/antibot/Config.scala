package antibot

import pureconfig._
import pureconfig.generic.auto._
import DataFormat._
import org.apache.spark.sql.SparkSession

object Config {
  lazy val config = ConfigSource.default.load[Config]
    .left.map(System.err.println)
    .left.map(_ => System.exit(1))
    .right.get

  case class Config(kafka: KafkaConf, cassandra: CassandraConf, redis: RedisConf) {
    override def toString: String = ConfigWriter[Config].to(this).unwrapped().toString
  }

  case class KafkaConf(brokers: String, topic: String) {
    def apply[F: DataFormat](f: F) = f.kafkaFormat(brokers, topic)
  }

  case class CassandraConf(keyspace: String, table: String)

  case class RedisConf(host: String, port: String, table: String, key: String, ttl: Int) {
    def apply[F: DataFormat](f: F) = f.redisFormat(table, key, ttl)
    def read(spark: SparkSession) = apply(spark.read)
    def apply(b: SparkSession.Builder) = b
      .config("spark.redis.host", config.redis.host)
      .config("spark.redis.port", config.redis.port)
      .config("spark.redis.timeout", "30000")
  }

  def setProperties(redisPort: Int, kafkaPort: Int, kafkaTopic: String): Unit = {
    System.setProperty("redis.port", s"$redisPort")
    System.setProperty("kafka.brokers", s"localhost:$kafkaPort")
    System.setProperty("kafka.topic", kafkaTopic)
  }
}