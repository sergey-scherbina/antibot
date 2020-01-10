package antibot

import org.scalacheck._
import org.scalatest.funsuite._

class AntibotTest extends AnyFunSuite with AntibotSuite {

  test("sanity check") {

    var bots = Map[String, List[Long]]()
      .withDefaultValue(List())

    Prop.forAll(clicks) { case (ip, times) =>
      println(s"$ip clicks $times times ...")
      for (n <- 1 to times) {
        Thread.sleep(100)
        if (n <= 20) click(ip) else
          bots = bots.updated(ip,
            click(ip) :: bots(ip))
      }
      true
    } check {
      _ withMinSuccessfulTests 10
    }

    println("Bots:")
    bots.foreach(println)
    println()

    waitStreams()

    println("Redis:")
    AntiBot.readRedis(sparkSession)
      .collect().foreach(println)
    println()

    /*
        println("Cassandra:")
        sparkSession.read
          .cassandraFormat("events", "antibot")
          .load().collect().foreach(println)
        println()
    */

    val res = cassandra.withSessionDo { cass =>
      bots.forall {
        case (ip, event_times) => event_times.forall(event_time =>
          showFail(s"$ip $event_time", cass.execute(
            s"""
               |select is_bot from antibot.events
               | where ip = '$ip' and event_time = $event_time
               | and is_bot = true allow filtering
               |""".stripMargin).iterator().hasNext)
        )
      }
    }

    assert(res)

  }
}
