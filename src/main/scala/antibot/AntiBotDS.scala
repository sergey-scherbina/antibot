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
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.scheduler._
import org.json4s.JsonAST.{JInt, JString}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.util.{Failure, Try}

object AntiBotDS {

  implicit val formats = DefaultFormats + new CustomSerializer[Long](_ => ( {
    case JString(x) => x.toLong
  }, {
    case x: Long => JInt(x)
  }))

  val logger = LoggerFactory.getLogger(this.getClass)

  def trace[T](n: Long, s: String, d: RDD[T]) = Function.const(d) {
    d.foreach(r => logger.trace(s"$s($n): $r"))
  }

  case class Event(`type`: String, ip: String, event_time: Long, url: String,
                   is_bot: Boolean = false, time_uuid: UUID = UUIDs.timeBased())

  def mergeState(x: Option[(Long, Int)],
                 y: Option[(Long, Int)]): Option[(Long, Int)] =
    (for (m1@(t1, c1) <- x; m2@(t2, c2) <- y) yield
      if ((t1 - t2).abs <= config.threshold.window.toSeconds)
        (t1 max t2, c1 + c2) else if (t1 > t2) m1
      else m2) orElse (x) orElse (y)

  def handleEvents(key: String, events: Option[Iterable[Event]],
                   state: State[(Long, Int)]): Iterable[Event] = (events map { es =>
    val st = mergeState(state.getOption(), es.map(_.event_time)
      .scanLeft(List[Long]())((q, a) =>
        a :: q.filter(_ + config.threshold.window.toSeconds > a))
      .filterNot(_.isEmpty).map(x => (x.max, x.size))
      .reduceOption((a, b) => if (a._1 > b._1) a else b))
    st.fold(state.remove())(state.update)
    st.filter(_._2 >= config.threshold.count)
      .fold(es.map(_.copy(is_bot = false))) { s =>
        val expire = s._1 + config.threshold.expire.toSeconds
        es.map(e => e.copy(is_bot = e.event_time < expire))
      }
  } toIterable) flatten

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

  lazy val streamingContext = StreamingContext.getActiveOrCreate(config.checkpointLocation, () => {
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    ssc.checkpoint(config.checkpointLocation)

    KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config.kafka.topic), kafkaParams))
      .flatMap(r => Try(parse(r.value()).extract[Event]).recoverWith { case err =>
        Function.const(Failure[Event](err))(logger.error("Parse error", err))
      }.toOption).filter(_.`type` == "click").map(e => (e.ip, e))
      .groupByKey().mapWithState(StateSpec.function(handleEvents _)
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
