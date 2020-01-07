package antibot

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.github.sebruck.EmbeddedRedis
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalacheck._
import org.scalatest._
import redis.embedded.RedisServer

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Random, Try}

trait AntibotSuite extends Suite with BeforeAndAfterAll with SparkTemplate
  with EmbeddedCassandra with EmbeddedRedis with EmbeddedKafka {

  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)
  val cassandra = CassandraConnector(defaultConf)
  val redis = RedisServer.builder().setting("bind 127.0.0.1").build()
  val kafka = EmbeddedKafka.start()
  lazy val antiBot = Future(Try(AntiBot.main())
    .transform(Try(_), t => Try(t.printStackTrace())))

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
    redis.start()
    config()
    createCustomTopic(clickTopic)
    cassandra.withSessionDo { cass =>
      cass.execute(
        """
          |create keyspace if not exists antibot with replication = {
          |    'class' : 'SimpleStrategy', 'replication_factor' : 1 } ;
          |
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
    antiBot.isCompleted
    println("===--- ... Antibot started! ---===")
  }

  private def config(): Unit = {
    val redisPort = redis.ports().get(0)
    val kafkaPort = implicitly[EmbeddedKafkaConfig].kafkaPort
    System.setProperty("redis.0", "localhost")
    System.setProperty("redis.1", s"$redisPort")
    System.setProperty("redis.2", "bots")
    System.setProperty("redis.3", "ip")
    System.setProperty("redis.4", "600")
    System.setProperty("kafka.0", s"localhost:$kafkaPort")
    System.setProperty("kafka.1", clickTopic)
  }

  override protected def afterAll(): Unit = {
    println("===--- Stopping Antibot ... ---===")
    Try(Await.ready(antiBot, 10 second))
    stopRedis(redis)
    kafka.stop(true)
    println("===--- ... Antibot stopped! ---===")
  }

}
