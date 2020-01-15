package antibot

import antibot.Config._
import antibot.DataFormat._
import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.concurrent.duration.Duration

object AntiBot {

  val eventSchema = structType(
    "type" -> StringType,
    "ip" -> StringType,
    "event_time" -> StringType,
    "url" -> StringType
  )

  val cacheSchema = structType(
    "ip" -> StringType,
    "count" -> IntegerType,
    "event_time" -> IntegerType
  )

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

  def duration(d: Duration) = s"${d.toMillis} millisecond"

  val timeUUID = udf(() => UUIDs.timeBased().toString)

  lazy val spark = config(SparkSession.builder)
    .appName("AntiBot").getOrCreate()

  def main(args: Array[String] = Array()): Unit = {
    if (args != null && args.contains("--ds"))
      AntiBotDS.main(args) else AntiBotSS.main(args)
  }

}
