package antibot

import pureconfig._
import pureconfig.generic.auto._
import DataFormat._

case class Config(kafka: (String, String), cassandra: (String, String),
                  redis: (String, String, String, String, String)) {
  override def toString: String = ConfigWriter[Config].to(this).unwrapped().toString
  
  def redisFormat[F: DataFormat](f: F) = f.redisFormat(redis._3, redis._4, redis._5)
  def kafkaFormat[F: DataFormat](f: F) = f.kafkaFormat(kafka._1, kafka._2)
}

object Config {
  lazy val config = ConfigSource.default.load[Config]
    .left.map(System.err.println)
    .left.map(_ => System.exit(1))
    .right.get
}