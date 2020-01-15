package antibot

import java.util.concurrent.atomic.AtomicBoolean

import antibot.Config._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming._
import org.slf4j.LoggerFactory

object AntiBotDS {

  val logger = LoggerFactory.getLogger(this.getClass)

  def trace[T](n: Long, s: String, d: RDD[T]) = Function.const(d) {
    d.foreach(r => logger.trace(s"$s($n): $r"))
  }

  val started = new AtomicBoolean(false)

  lazy val spark = config(SparkSession.builder)
    .appName("AntiBot")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  lazy val streamingContext = StreamingContext.getActiveOrCreate(
    config.checkpointLocation, () => {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> config.kafka.brokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "antibot",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      val stream = KafkaUtils.createDirectStream[String, String](
        ssc, PreferConsistent, Subscribe[String, String](Array(config.kafka.topic), kafkaParams)
      )

      stream.map(record => (record.key, record.value))
        .foreachRDD((rdd, time) => trace(0, "kafka", rdd))

      ssc
    }
  )

  def stop(): Unit = {
    if (started.get())
      streamingContext.stop(false, true)
  }

  def main(args: Array[String] = Array()): Unit = {
    logger.info(s"Starting AntiBot DStreams with config: $config")
    streamingContext.start()
    started.lazySet(true)
    streamingContext.awaitTermination()
  }
}
