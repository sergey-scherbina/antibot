package antibot

import antibot.Config._
import antibot.DataFormat._
import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

object AntiBot {

  val queryName = getClass.getCanonicalName

  val eventStruct = structType(
    "type" -> StringType,
    "ip" -> StringType,
    "event_time" -> StringType,
    "url" -> StringType
  )

  val redisSchema = structType(
    "ip" -> StringType,
    "count" -> IntegerType
  )

  def readRedis(spark: SparkSession, schema: StructType = redisSchema) =
  // ugly hack for ugly bug in redis-spark
    spark.createDataFrame(spark.sparkContext.parallelize(
      config.redis.read(spark).schema(schema).load().collect()), redisSchema)


  def writeRedis(d: DataFrame) = Function.const(d) {
    config.redis(d.write.mode(SaveMode.Overwrite)).save()
  }

  def debug(n: Long, s: String, d: DataFrame) = Function.const(d) {
    println(s"$s#$n:${d.columns.mkString(", ")}\n" +
      d.collect().map(r => s"$s#$n:$r").mkString("\n"))
  }

  val timeUUID = udf(() => UUIDs.timeBased().toString)

  def main(args: Array[String] = Array()): Unit = {
    println(s"Starting AntiBot with config: $config")

    val spark = config.redis(SparkSession.builder
      .appName("AntiBot")).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    config.kafka(spark.readStream).load()
      .select(from_json($"value".cast(DataTypes.StringType),
        eventStruct).as("e"))
      .na.drop("any").where($"e.type" === "click")
      .select($"e.type", $"e.ip", $"e.url", $"e.event_time",
        timeUUID().as("time_uuid"),
        to_timestamp(from_unixtime($"e.event_time"))
          .as("event_ts"))
      .withWatermark("event_ts", "10 minutes")
      .writeStream.outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(1000))
      .foreachBatch { (e: DataFrame, n: Long) =>
        e.as("e").join(writeRedis(
          e.groupBy($"ip", window($"event_ts",
            "10 seconds", "1 second"))
            .count().groupBy("ip")
            .agg(max($"count").as("count"))
            .as("g").join(readRedis(spark).as("r"),
            $"g.ip" === $"r.ip", "full")
            .select(coalesce($"g.ip", $"r.ip").as("ip"),
              (coalesce($"r.count", lit(0)) +
                coalesce($"g.count", lit(0)))
                .as("count")))
          .where($"count" >= 20).as("b"),
          $"e.ip" === $"b.ip", "left")
          .select(e("type"), e("ip"), e("event_time"),
            $"b.count".isNotNull.as("is_bot"),
            e("time_uuid"), e("url"))
          .write.mode(SaveMode.Append)
          .cassandraFormat(config.cassandra.table,
            config.cassandra.keyspace)
          .save()
      } queryName queryName start()

    spark.streams.awaitAnyTermination()
  }

}
