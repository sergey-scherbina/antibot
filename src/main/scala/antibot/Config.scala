package antibot

import pureconfig._
import pureconfig.generic.auto._
import DataFormat._
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration._

object Config {

  lazy val config = ConfigSource.default.load[Config]
    .left.map(System.err.println)
    .left.map(_ => System.exit(1))
    .right.get

  case class Config(kafka: KafkaConf = KafkaConf(), cassandra: CassandraConf = CassandraConf(),
                    redis: RedisConf = RedisConf(), threshold: Threshold = Threshold(),
                    checkpointLocation: String = "/tmp/antibot") {
    override def toString: String = ConfigWriter[Config].to(this).unwrapped().toString
    def apply(b: SparkSession.Builder) = b
      .config("spark.cassandra.connection.host", cassandra.host)
      .config("spark.cassandra.connection.port", cassandra.port)
      .config("spark.redis.host", redis.host)
      .config("spark.redis.port", redis.port)
      .config("spark.redis.timeout", "30000")
  }

  case class Threshold(count: Int = 20, window: Duration = 10 seconds,
                       slide: Duration = 1 second, expire: Duration = 10 minutes)

  case class KafkaConf(brokers: String = "localhost:9092", topic: String = "events",
                       groupId: String = "antibot", failOnDataLoss: Boolean = true) {
    def apply[F: DataFormat](f: F) = f.kafkaFormat(brokers, topic, failOnDataLoss)
  }

  case class CassandraConf(host: String = "localhost", port: Int = 9042,
                           keyspace: String = "antibot", table: String = "events")

  case class RedisConf(host: String = "localhost", port: Int = 6379,
                       table: String = "events", ttl: Duration = 10 minutes) {
    def apply[F: DataFormat](f: F) = f.redisFormat(table, "ip", ttl.toSeconds)
  }

}