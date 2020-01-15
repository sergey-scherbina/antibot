package antibot

import java.util.concurrent.atomic.AtomicBoolean

import antibot.AntiBot._
import antibot.Config._
import antibot.DataFormat._
import org.apache
import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object AntiBotSS {
  val queryName = this.getClass.getCanonicalName
  val logger = LoggerFactory.getLogger(this.getClass)

  def trace(n: Long, s: String, d: DataFrame) = Function.const(d) {
    logger.trace(s"$s($n): ${d.columns.mkString(", ")}")
    d.foreach(r => logger.trace(s"$s($n): $r"))
  }

  def readCache(spark: SparkSession, schema: StructType = cacheSchema) = {
    import spark.implicits._

    val c = config.redis(spark.read).schema(schema).load()
      .where($"event_time" > (unix_timestamp() -
        config.threshold.expire.toSeconds))

    // ugly hack for ugly bug in redis-spark
    spark.createDataFrame(spark.sparkContext
      .parallelize(c.collect()), schema)
  }

  def writeCache(d: DataFrame) = Function.const(d) {
    config.redis(d.write.mode(SaveMode.Overwrite)).save()
  }

  val started = new AtomicBoolean(false)

  lazy val spark = config(SparkSession.builder)
    .appName("AntiBot")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  def main(args: Array[String] = Array()): Unit = {
    logger.info(s"Starting AntiBot Structured Streaming with config: $config")

    import spark.implicits._

    config.kafka(spark.readStream).load()
      .select(from_json($"value".cast(StringType), eventSchema).as("e"))
      .na.drop("any").where($"e.type" === "click")
      .select($"e.type", $"e.ip", $"e.url", $"e.event_time".cast(IntegerType),
        to_timestamp(from_unixtime($"e.event_time")).as("time"),
        timeUUID().as("time_uuid"))
      .withWatermark("time", duration(config.threshold.expire))
      .writeStream.outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(0))
      .foreachBatch { (e: DataFrame, n: Long) =>
        logger.trace(s"started batch #$n")

        val cache = trace(n, "cache", writeCache {
          e.groupBy($"ip", window($"time",
            duration(config.threshold.window), duration(config.threshold.slide)))
            .agg(count("*").as("count"),
              max("event_time").as("event_time"))
            .sort("count").groupBy("ip")
            .agg(max("count").as("count"),
              last("event_time").as("event_time"))
            .unionByName(readCache(spark)).groupBy("ip")
            .agg(sum("count").as("count"),
              max("event_time").as("event_time"))
        })

        trace(n, "output",
          e.as("e").join(cache.where(
            $"count" >= config.threshold.count).as("c"),
            $"e.ip" === $"c.ip", "left")
            .select(e("type"), e("ip"), e("event_time"),
              $"c.count".isNotNull.as("is_bot"),
              e("time_uuid"), e("url")))
          .write.mode(SaveMode.Append)
          .cassandraFormat(config.cassandra.table,
            config.cassandra.keyspace)
          .save()

        logger.trace(s"finished batch #$n")

      } queryName queryName option
      ("checkpointLocation", config.checkpointLocation) start()

    started.lazySet(true)
    spark.streams.awaitAnyTermination()
    logger.info(s"Stopped AntiBot")
  }

  def stop(): Unit = {
    if (started.get()) {
      spark.streams.active.foreach(_.stop())
    }
  }

}
