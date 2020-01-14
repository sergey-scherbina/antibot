package antibot

import antibot.Config._
import antibot.DataFormat._
import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object AntiBot {
  val queryName = this.getClass.getCanonicalName
  val logger = LoggerFactory.getLogger(this.getClass)

  val eventStruct = structType(
    "type" -> StringType,
    "ip" -> StringType,
    "event_time" -> StringType,
    "url" -> StringType
  )

  val redisSchema = structType(
    "ip" -> StringType,
    "count" -> IntegerType,
    "event_time" -> IntegerType
  )

  def readRedis(spark: SparkSession, schema: StructType = redisSchema) = {
    import spark.implicits._

    val r = config.redis.read(spark).schema(schema).load()
      .where($"event_time" > (unix_timestamp() -
        config.threshold.expire.toSeconds))

    // ugly hack for ugly bug in redis-spark
    spark.createDataFrame(spark.sparkContext
      .parallelize(r.collect()), redisSchema)
  }


  def writeRedis(d: DataFrame) = Function.const(d) {
    config.redis(d.write.mode(SaveMode.Overwrite)).save()
  }

  def trace(n: Long, s: String, d: DataFrame) = Function.const(d) {
    logger.trace(s"$s($n): ${d.columns.mkString(", ")}")
    d.foreach(r => logger.trace(s"$s($n): $r"))
  }

  val timeUUID = udf(() => UUIDs.timeBased().toString)

  def main(args: Array[String] = Array()): Unit = {
    logger.info(s"Starting AntiBot with config: $config")

    val spark = config.redis(SparkSession.builder
      .appName("AntiBot")).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    config.kafka(spark.readStream).load()
      .select(from_json($"value".cast(DataTypes.StringType),
        eventStruct).as("e"))
      .na.drop("any").where($"e.type" === "click")
      .select($"e.type", $"e.ip", $"e.url",
        $"e.event_time".cast(IntegerType).as("event_time"),
        to_timestamp(from_unixtime($"e.event_time")).as("time"),
        timeUUID().as("time_uuid"))
      .withWatermark("time", config.threshold.expire.toMillis + " millisecond")
      .writeStream.outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(config.threshold.slide.toMillis))
      .foreachBatch { (e: DataFrame, n: Long) =>
        logger.trace(s"started batch #$n")

        val redisData = trace(n, "cache", writeRedis {
          e.groupBy($"ip", window($"time",
            config.threshold.window.toMillis + " millisecond",
            config.threshold.slide.toMillis + " millisecond"))
            .agg(count("*").as("count"),
              max("event_time").as("event_time"))
            .sort($"count").groupBy("ip")
            .agg(max($"count").as("count"),
              last("event_time").as("event_time"))
            .unionByName(readRedis(spark))
            .groupBy("ip").agg(sum($"count").as("count"),
            max("event_time").as("event_time"))
        })

        val cassData = trace(n, "output",
          e.as("e").join(redisData.where(
            $"count" >= config.threshold.count).as("b"),
            $"e.ip" === $"b.ip", "left")
            .select(e("type"), e("ip"), e("event_time"),
              $"b.count".isNotNull.as("is_bot"),
              e("time_uuid"), e("url")))

        cassData.write.mode(SaveMode.Append)
          .cassandraFormat(config.cassandra.table,
            config.cassandra.keyspace)
          .save()

        logger.trace(s"finished batch #$n")

      } queryName queryName start()

    spark.streams.awaitAnyTermination()
    logger.info(s"Stopped AntiBot")
  }

}
