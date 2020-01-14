package antibot

import org.apache.spark.sql._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

import scala.Function._

trait DataFormat[F] {
  def format(f: String): F => F
  def option(n: String, v: String): F => F
}

object DataFormat {

  def structType(fs: (String, DataType)*) = StructType(fs map tupled(StructField(_, _)))

  def apply[F: DataFormat]: DataFormat[F] = implicitly[DataFormat[F]]

  implicit class Syntax[F: DataFormat](f: F) {
    def format(n: String) = DataFormat[F].format(n)(f)
    def option(n: String, v: String) = DataFormat[F].option(n, v)(f)

    import redis._

    def redisFormat(table: String, key: String, ttl: Long) =
      f.format(RedisFormat)
        .option(SqlOptionTableName, table)
        .option(SqlOptionKeyColumn, key)
        .option(SqlOptionTTL, s"$ttl")

    def kafkaFormat(brokers: String, topic: String, failOnDataLoss: Boolean = true) =
      f.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("failOnDataLoss", s"$failOnDataLoss")
  }

  implicit object reader extends DataFormat[DataFrameReader] {
    override def format(n: String) = _.format(n)
    override def option(n: String, v: String) = _.option(n, v)
  }

  implicit def writer[T]: DataFormat[DataFrameWriter[T]] = new DataFormat[DataFrameWriter[T]] {
    override def format(n: String) = _.format(n)
    override def option(n: String, v: String) = _.option(n, v)
  }

  implicit object streamReader extends DataFormat[DataStreamReader] {
    override def format(n: String) = _.format(n)
    override def option(n: String, v: String) = _.option(n, v)
  }

  implicit def streamWriter[T]: DataFormat[DataStreamWriter[T]] = new DataFormat[DataStreamWriter[T]] {
    override def format(n: String) = _.format(n)
    override def option(n: String, v: String) = _.option(n, v)
  }

}
