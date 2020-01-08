package antibot

import org.apache.spark.sql._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import DataFormat._
import Config._

object AntiBot {

  val eventStruct = structType(
    "type" -> StringType,
    "ip" -> StringType,
    "event_time" -> StringType,
    "url" -> StringType
  )

  val botsStruct = structType(
    "ip" -> StringType,
    "start" -> TimestampType,
    "count" -> IntegerType
  )

  def main(args: Array[String] = Array()): Unit = {
    println(s"Started AntiBot with config: $config")

    val spark = SparkSession.builder
      .appName("AntiBot")
      .config("spark.redis.host", config.redis.host)
      .config("spark.redis.port", config.redis.port)
      .config("spark.redis.timeout", "10000")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    def readRedis() = config
      .redisFormat(spark.read).schema(botsStruct).load()

    readRedis() // warm up redis, seems like it's essential

    val events = config.kafkaFormat(spark.readStream).load()
      .select(from_json($"value".cast(DataTypes.StringType), eventStruct).as("e"))
      .na.drop("any").where($"e.type" === "click")
      .select($"e.ip", $"e.url",
        to_timestamp(from_unixtime($"e.event_time")).as("event_time"))
      .withWatermark("event_time", "10 minutes")

    // events.writeStream.format("console").outputMode(OutputMode.Append()).start()

    events.groupBy($"ip", window($"event_time", "10 seconds"))
      .count().where($"count" > 20)
      .withWatermark("window", "10 minutes")
      .select($"ip", $"window.start".as("start"), $"count")
      .writeStream.outputMode(OutputMode.Complete())
      .foreachBatch { (b: DataFrame, _: Long) =>
        config.redisFormat(b.write.mode(SaveMode.Overwrite)).save()
      } start()

    events.writeStream.outputMode(OutputMode.Append())
      .foreachBatch { (e: DataFrame, _: Long) =>
        val b = readRedis()
        val r = e.join(b, e("ip") === b("ip"), "left")
          .select(e("ip"), e("event_time"), e("url"),
            b("count").isNotNull.as("is_bot"))
        r.write.mode(SaveMode.Append)
          .cassandraFormat(config.cassandra.table, config.cassandra.keyspace)
          .save()
        // r.show()
      } start()

    spark.streams.awaitAnyTermination()
  }

}
