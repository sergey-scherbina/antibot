package antibot

import antibot.Config._
import antibot.DataFormat._
import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.concurrent.duration.Duration

object AntiBot {

  def main(args: Array[String] = Array()): Unit = {
    if (args != null && args.contains("--ds"))
      AntiBotDS.main(args) else AntiBotSS.main(args)
  }

}
