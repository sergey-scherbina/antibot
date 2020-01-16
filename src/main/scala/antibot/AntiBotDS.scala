package antibot

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import antibot.Config._
import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

object AntiBotDS {

  implicit val formats = DefaultFormats

  val logger = LoggerFactory.getLogger(this.getClass)

  def trace[T](n: Long, s: String, d: RDD[T]) = Function.const(d) {
    d.foreach(r => logger.trace(s"$s($n): $r"))
  }

  val started = new AtomicBoolean(false)

  lazy val spark = config(SparkSession.builder)
    .appName("AntiBot")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  case class Event(`type`: String, ip: String,
                   event_time: String, url: String,
                   is_bot: Option[Boolean],
                   time_uuid: Option[UUID])

  lazy val streamingContext = StreamingContext.getActiveOrCreate(
    config.checkpointLocation, () => {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
      ssc.checkpoint(config.checkpointLocation)

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> config.kafka.brokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "antibot",
        "auto.offset.reset" -> "latest"
      )

      def handleEvents(key: String, events: Option[Iterable[Event]],
                       state: State[Int]): Iterable[Event] = (events map { es =>
        val total = state.getOption().getOrElse(0) + es.count(_ => true)
        state.update(total)
        val isBot = Some(total >= config.threshold.count)
        es.map(_.copy(is_bot = isBot))
      } toIterable) flatMap identity

      KafkaUtils.createDirectStream[String, String](
        ssc, PreferConsistent, Subscribe[String, String](
          Array(config.kafka.topic), kafkaParams))
        .flatMap(r => parse(r.value()).extractOpt[Event])
        .filter(_.`type` == "click").map(e =>
        (e.ip, e.copy(time_uuid = Some(UUIDs.timeBased()))))
        .groupByKey().mapWithState(StateSpec.function(handleEvents _)
        .timeout(Milliseconds(config.threshold.expire.toMillis)))
        .flatMap(identity).foreachRDD { rdd =>
        rdd.saveToCassandra(config.cassandra.keyspace, config.cassandra.table)
        rdd.foreach(e => logger.debug(e.toString))
      }

      ssc
    }

  )

  def awaitComplete(cb: Unit => Unit) = {
    streamingContext.addStreamingListener(new StreamingListener {
      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit =
        if (batchCompleted.batchInfo.numRecords == 0) cb(())
    })
  }

  def stop(): Unit = {
    if (started.get())
      streamingContext.stop(true, true)
  }

  def main(args: Array[String] = Array()): Unit = {
    logger.info(s"Starting AntiBot DStreams with config: $config")
    streamingContext.start()
    started.lazySet(true)
    streamingContext.awaitTermination()
  }
}
