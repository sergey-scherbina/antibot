package antibot

import java.io.IOException
import java.net.ServerSocket

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.github.sebruck.EmbeddedRedis
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.scalacheck._
import org.scalatest._
import redis.embedded.RedisServer

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util._

trait AntibotSuite extends Suite with BeforeAndAfterAll with SparkTemplate
  with EmbeddedCassandra with EmbeddedRedis with EmbeddedKafka {

  System.setProperty("baseDir", ".") // for embedded cassandra ports directory

  val clickTopic = "click"
  val initCassandra = Seq(
    """
      |create keyspace if not exists antibot
      | with replication = {'class' : 'SimpleStrategy',
      | 'replication_factor' : 1 };""".stripMargin,
    """
      |create table if not exists antibot.events(
      |  "type" text,
      |  ip inet,
      |  is_bot boolean,
      |  event_time int,
      |  url text,
      |  time_uuid timeuuid,
      |  primary key ((ip, event_time), time_uuid)
      |);""".stripMargin
  )

  val kafka = EmbeddedKafka.start()
  val redis = RedisServer.builder().port(freePort()).setting("bind 127.0.0.1").build()
  val sparkConf = defaultConf.set("spark.redis.port", redis.ports().get(0).toString)
  val cassandra = CassandraConnector(sparkConf)
  lazy val antiBot = Future(AntiBot.main())

  sys.addShutdownHook(() => stopRedis(redis))

  val octet = Gen.choose(0, 255)
  val IP = for {x1 <- octet; x2 <- octet;
                x3 <- octet; x4 <- octet} yield s"$x1.$x2.$x3.$x4"
  val clicks = for {ip <- IP; n <- Gen.choose(20, 30)} yield ip -> n

  def click(ip: String) = {
    val event_time = (System.currentTimeMillis / 1000) // + Random.nextInt(10)
    publishStringMessageToKafka(clickTopic,
      s"""{"type": "$clickTopic", "ip": "${ip}", "event_time": "$event_time",
         | "url": "https://blog.griddynamics.com/in-stream-processing-service-blueprint"}"""
        .stripMargin)
    event_time
  }

  def getEvents(isBot: Boolean) = cassandra.withSessionDo(
    _.execute(s"select * from antibot.events where is_bot = $isBot ALLOW FILTERING;"))

  override def clearCache(): Unit = CassandraConnector.evictCache()

  override protected def beforeAll(): Unit = {
    println("===--- Starting Antibot ... ---===")
    Try {
      Config.setProperties(redis.ports().get(0),
        implicitly[EmbeddedKafkaConfig].kafkaPort, clickTopic)
      useCassandraConfig(Seq(YamlTransformations.Default))
      useSparkConf(sparkConf)
      redis.start()
      createCustomTopic(clickTopic)
      cassandra.withSessionDo(cass =>
        initCassandra.foreach(cass.execute))
    }.failed.map(_.printStackTrace())
      .foreach(_ => System.exit(1))
    antiBot.failed.map(_.printStackTrace())
      .foreach(_ => System.exit(1))
    println("===--- ... Antibot started! ---===")
    showPorts()
    waitStreams(true)
  }

  override protected def afterAll(): Unit = {
    println("===--- Stopping Antibot ... ---===")
    Try(Await.ready(antiBot, 10 second))
    stopRedis(redis)
    kafka.stop(true)
    println("===--- ... Antibot stopped! ---===")
  }

  @tailrec
  final def freePort(): Int =
    Try(new ServerSocket(0)) match {
      case Success(socket) =>
        val port = socket.getLocalPort
        socket.close()
        port
      case Failure(_: IOException) => freePort()
      case Failure(e) => throw e
    }

  def showPorts(): Unit = {
    println(s"Cassandra port : ${cassandra.port}")
    println(s"Zookeeper port : ${kafka.config.zooKeeperPort}")
    println(s"Kafka port : ${kafka.config.kafkaPort}")
    println(s"Redis ports : ${redis.ports()}")
  }

  def showFail(s: String, b: Boolean): Boolean = {
    if (!b)
      println("Fail: " + s)
    b
  }

  def waitStreams(start: Boolean = false, time: Duration = Duration.Inf) = {
    println("Waiting streams ...")
    val latch = Promise[Unit]()
    sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =
        if (start) unlock()
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit =
        if (!start && event.progress.numInputRows == 0) unlock()
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =
        unlock()

      private def unlock(): Unit = {
        println("Streams are ready.")
        sparkSession.streams.removeListener(this)
        if (!latch.isCompleted) latch.success()
      }
    })
    Await.ready(latch.future, time)
  }

}
