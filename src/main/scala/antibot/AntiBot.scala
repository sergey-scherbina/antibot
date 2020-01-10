package antibot

import org.apache.spark.sql._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import DataFormat._
import Config._
import com.datastax.driver.core.utils.UUIDs

object AntiBot {

  val eventStruct = structType(
    "type" -> StringType,
    "ip" -> StringType,
    "event_time" -> StringType,
    "url" -> StringType
  )

  val botsStruct = structType(
    "ip" -> StringType,
    "count" -> IntegerType,
    "start" -> IntegerType,
    "end" -> IntegerType
  )

  val timeUUID = udf(() => UUIDs.timeBased().toString)

  def readRedis(spark: SparkSession) =
    config.redis.read(spark).schema(botsStruct).load()

  def main(args: Array[String] = Array()): Unit = {
    println(s"Started AntiBot with config: $config")

    val spark = config.redis(SparkSession.builder
      .appName("AntiBot")).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    readRedis(spark) // warm up redis, seems like it's essential

    val events = config.kafka(spark.readStream).load()
      .select(from_json($"value".cast(DataTypes.StringType), eventStruct).as("e"))
      .na.drop("any").where($"e.type" === "click")
      .select($"e.ip", $"e.url", $"e.event_time"
        , to_timestamp(from_unixtime($"e.event_time")).as("event_ts")
      )
    //.withWatermark("event_ts", "10 minutes")

    events.groupBy($"ip", window($"event_ts", "10 seconds"))
      .count()
      //.where($"count" > 20)
      // .withWatermark("window", "10 minutes")
      .select($"ip", $"count",
        unix_timestamp($"window.start").as("start"),
        unix_timestamp($"window.end").as("end")
      ).writeStream.outputMode(OutputMode.Complete())
      .foreachBatch { (b: DataFrame, _: Long) =>
        config.redis(b.write.mode(SaveMode.Overwrite)).save()
      } start()

    events.writeStream.outputMode(OutputMode.Append())
      .foreachBatch { (e: DataFrame, n: Long) =>
        val b = readRedis(spark)
        e.join(b, e("ip") === b("ip"), "left")
          .select(e("ip"), e("event_time"), e("url"),
            b("count").isNotNull.as("is_bot"),
            lit("click").as("type"),
            timeUUID().as("time_uuid")
          ).write.mode(SaveMode.Append)
          .cassandraFormat(config.cassandra.table, config.cassandra.keyspace)
          .save()
      } start()

    spark.streams.awaitAnyTermination()
  }

}
