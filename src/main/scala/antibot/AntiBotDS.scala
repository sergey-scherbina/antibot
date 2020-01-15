package antibot

import antibot.AntiBot._
import antibot.Config._
import antibot.DataFormat._
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object AntiBotDS {

  val logger = LoggerFactory.getLogger(this.getClass)

  def trace(n: Long, s: String, d: DataFrame) = Function.const(d) {
    logger.trace(s"$s($n): ${d.columns.mkString(", ")}")
    d.foreach(r => logger.trace(s"$s($n): $r"))
  }

  def main(args: Array[String] = Array()): Unit = {
    logger.info(s"Starting AntiBot DStreams with config: $config")

  }
}
