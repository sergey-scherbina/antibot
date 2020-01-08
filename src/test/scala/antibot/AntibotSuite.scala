package antibot

import java.io.IOException
import java.net.ServerSocket

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.github.sebruck.EmbeddedRedis
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalacheck._
import org.scalatest._
import redis.embedded.RedisServer

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success, Try}

trait AntibotSuite extends Suite with BeforeAndAfterAll with SparkTemplate
  with EmbeddedCassandra with EmbeddedRedis with EmbeddedKafka {

  val kafka = EmbeddedKafka.start()
  val redis = RedisServer.builder().port(freePort()).setting("bind 127.0.0.1").build()
  val sparkConf = defaultConf.set("spark.redis.port", redis.ports().get(0).toString)
  val cassandra = CassandraConnector(sparkConf)
  lazy val antiBot = Future(AntiBot.main())

  val octet = Gen.choose(0, 255)
  val IP = for {x1 <- octet; x2 <- octet;
                x3 <- octet; x4 <- octet} yield s"$x1.$x2.$x3.$x4"
  val clicks = for {ip <- IP; n <- Gen.choose(1, 30)} yield ip -> n
  val clickTopic = "click"

  def click(ip: String) = publishStringMessageToKafka(clickTopic,
    s"""{"type": "$clickTopic", "ip": "${ip}",
       | "event_time": "${(System.currentTimeMillis / 1000) + Random.nextInt(10)}",
       | "url": "https://blog.griddynamics.com/in-stream-processing-service-blueprint"}"""
      .stripMargin)

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
      cassandra.withSessionDo { cass =>
        cass.execute(
          """
            |create keyspace if not exists antibot with replication = {
            |    'class' : 'SimpleStrategy', 'replication_factor' : 1 } ;
            |""".stripMargin)
        cass.execute(
          """
            |create table if not exists antibot.events(
            |  ip inet,
            |  is_bot boolean,
            |  event_time timestamp,
            |  url text,
            |  primary key (ip, event_time)
            |);
            |""".stripMargin)
      }
    }.failed.map(_.printStackTrace())
      .foreach(_ => System.exit(1))
    antiBot.failed.map(_.printStackTrace())
      .foreach(_ => System.exit(1))

    println("===--- ... Antibot started! ---===")
  }

  override protected def afterAll(): Unit = {
    println("===--- Stopping Antibot ... ---===")
    Try(Await.ready(antiBot, 10 second))
    stopRedis(redis)
    kafka.stop(true)
    println("===--- ... Antibot stopped! ---===")
  }

  @tailrec
  protected final def freePort(): Int =
    Try(new ServerSocket(0)) match {
      case Success(socket) =>
        val port = socket.getLocalPort
        socket.close()
        port
      case Failure(_: IOException) => freePort()
      case Failure(e) => throw e
    }

}
