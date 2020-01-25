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
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.scheduler._
import org.json4s.JsonAST.{JInt, JString}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.util.{Failure, Try}

object AntiBotDS {
  type Time = Long

  case class Event(`type`: String, ip: String, event_time: Time, url: String,
                   is_bot: Boolean = false, time_uuid: UUID = UUIDs.timeBased())

  case class Activity(lastTime: Time = 0, count: Long = 0, detected: Option[Time] = None) {
    def isBot(time: Time): Boolean = detected.exists(t =>
      time <= t + config.threshold.expire.toSeconds)

    def +(newTime: Time): Activity = {
      val (last, first) = (newTime max lastTime, newTime min lastTime)
      if (last - first <= config.threshold.window.toSeconds) {
        if (count >= config.threshold.count) Activity(last, 1, Some(last))
        else Activity(last, count + 1, detected)
      } else Activity(last, 1)
    }
  }

  implicit val formats = DefaultFormats + new CustomSerializer[Long](_ => ( {
    case JString(x) => x.toLong
  }, {
    case x: Long => JInt(x)
  }))

  val logger = LoggerFactory.getLogger(this.getClass)

  def trace[T](n: Long, s: String, d: RDD[T]) = Function.const(d) {
    d.foreach(r => logger.trace(s"$s($n): $r"))
  }

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> config.kafka.brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> config.kafka.groupId
  )

  val started = new AtomicBoolean(false)

  lazy val spark = config(SparkSession.builder)
    .appName("AntiBot")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  def eventsStream(ssc: StreamingContext): DStream[Event] =
    KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config.kafka.topic), kafkaParams))
      .flatMap(r => Try(parse(r.value()).extract[Event]).recoverWith { case err =>
        Function.const(Failure[Event](err))(logger.error("Parse error", err))
      }.toOption).filter(_.`type` == "click")

  def detectBots(key: String, events: Option[Iterable[Event]],
                 state: State[Activity]): Iterable[Event] = (events map { es =>
    val s = es.map(_.event_time).foldLeft(state.getOption().getOrElse(Activity()))(_ + _)
    state.update(s)
    es.map(e => e.copy(is_bot = s.isBot(e.event_time)))
  } toIterable) flatten

  lazy val streamingContext = StreamingContext.getActiveOrCreate(config.checkpointLocation, () => {
    val ssc = new StreamingContext(spark.sparkContext, Milliseconds(config.threshold.slide.toMillis))
    ssc.checkpoint(config.checkpointLocation)

    eventsStream(ssc).map(e => (e.ip, e))
      .groupByKey().mapWithState(StateSpec.function(detectBots _)
      .timeout(Milliseconds(config.threshold.expire.toMillis)))
      .flatMap(identity).foreachRDD { rdd =>
      rdd.saveToCassandra(config.cassandra.keyspace, config.cassandra.table)
      if (logger.isTraceEnabled) rdd.foreach(e => logger.trace(e.toString))
    }

    ssc
  })

  def await(started: Long => Unit = _ => (), completed: BatchInfo => Unit = _ => ()) =
    streamingContext.addStreamingListener(new StreamingListener {
      override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit =
        started(streamingStarted.time)
      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit =
        completed(batchCompleted.batchInfo)
    })

  def stop(): Unit = if (started.get())
    streamingContext.stop(true, true)

  def main(args: Array[String] = Array()): Unit = {
    logger.info(s"Starting AntiBot DStreams with config: $config")
    streamingContext.start()
    started.lazySet(true)
    streamingContext.awaitTermination()
  }
}
