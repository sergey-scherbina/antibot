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

  def duration(d: Duration) = s"${d.toMillis} millisecond"

  val timeUUID = udf(() => UUIDs.timeBased().toString)

  def main(args: Array[String] = Array()): Unit = {
    if (args != null && args.contains("--ds"))
      AntiBotDS.main(args) else AntiBotSS.main(args)
  }

}
