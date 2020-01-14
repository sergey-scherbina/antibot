package antibot

import java.io.IOException
import java.net.ServerSocket

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.github.sebruck.EmbeddedRedis
import net.manub.embeddedkafka._
import org.apache.spark.sql.streaming._
import org.scalacheck._
import org.scalatest._
import org.slf4j.LoggerFactory
import redis.embedded.RedisServer

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util._

trait AntibotSuite extends Suite with BeforeAndAfterAll with SparkTemplate
  with EmbeddedCassandra with EmbeddedRedis with EmbeddedKafka {

  lazy val THRESHOLD = Config.config.threshold

  val logger = LoggerFactory.getLogger(this.getClass)

  override def clearCache(): Unit = CassandraConnector.evictCache()
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
      |  event_time int,
      |  is_bot boolean,
      |  url text,
      |  time_uuid timeuuid,
      |  primary key ((ip, event_time), time_uuid)
      |);""".stripMargin
  )

  val kafka = EmbeddedKafka.start()

  val redis = RedisServer.builder().port(freePort)
    .setting("bind 127.0.0.1").build()
  sys.addShutdownHook(() => stopRedis(redis))

  val sparkConf = defaultConf.setMaster("local[*]")
    .set("spark.redis.port", redis.ports().get(0).toString)

  val cassandra = CassandraConnector(sparkConf)

  lazy val antiBot = Future(AntiBot.main())

  val octet = Gen.choose(1, 255)
  val IP = for {x1 <- octet; x2 <- octet; x3 <- octet; x4 <- octet} yield s"$x1.$x2.$x3.$x4"
  val clicks = for {ip <- IP; n <- Gen.choose(1, THRESHOLD.count * 2)} yield ip -> n

  def timestamp() = System.currentTimeMillis() / 1000

  def click(ip: String, rand: Boolean = false) = {
    val event_time = timestamp() + (if (rand) Random.nextInt(10) *
      (if (Random.nextBoolean()) 1 else -1) else 0)
    publishStringMessageToKafka(clickTopic,
      s"""{"type": "$clickTopic", "ip": "${ip}", "event_time": "$event_time",
         | "url": "https://blog.griddynamics.com/in-stream-processing-service-blueprint"}"""
        .stripMargin)
    event_time
  }


  override protected def beforeAll(): Unit = {
    showPorts()
    logger.trace("===--- Starting Antibot ... ---===")
    Try {
      Config.setProperties(redis.ports().get(0),
        implicitly[EmbeddedKafkaConfig].kafkaPort, clickTopic)
      useCassandraConfig(Seq(YamlTransformations.Default))
      useSparkConf(sparkConf)
      redis.start()
      createCustomTopic(clickTopic)
      cassandra.withSessionDo(cass => initCassandra.foreach(cass.execute))
    }.failed.map(_.printStackTrace()).foreach(_ => sys.exit(1))
    antiBot.failed.map(_.printStackTrace()).foreach(_ => sys.exit(1))
    waitStreams()
    logger.trace("===--- ... Antibot started! ---===")
  }

  override protected def afterAll(): Unit = {
    logger.trace("===--- Stopping Antibot ... ---===")
    Try(Await.ready(antiBot, 5 second))
    stopRedis(redis)
    kafka.stop(true)
    logger.trace("===--- ... Antibot stopped! ---===")
  }

  def waitStreams(queryName: String = AntiBot.queryName, time: Duration = Duration.Inf) = {
    logger.trace(s"Waiting stream: $queryName ...")
    val latch = Promise[Unit]()
    sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = ()
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
      private def unlock(): Unit = {
        logger.trace(s"Stream $queryName complete")
        sparkSession.streams.removeListener(this)
        if (!latch.isCompleted) latch.success()
      }
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit =
        if (queryName == event.progress.name && event.progress.numInputRows == 0) unlock()
    })
    Await.result(latch.future, time)
  }

  @tailrec
  final def freePort: Int =
    Try(new ServerSocket(0)) match {
      case Success(socket) =>
        val port = socket.getLocalPort
        socket.close()
        port
      case Failure(_: IOException) => freePort
      case Failure(e) => throw e
    }

  def showPorts(): Unit = {
    println(s"Cassandra port : ${cassandra.port}")
    println(s"Zookeeper port : ${kafka.config.zooKeeperPort}")
    println(s"Kafka port : ${kafka.config.kafkaPort}")
    println(s"Redis ports : ${redis.ports()}")
  }

  def showFail(s: String, b: Boolean) = Function.const(b) {
    if (!b)
      println("Fail: " + s)
  }

}
