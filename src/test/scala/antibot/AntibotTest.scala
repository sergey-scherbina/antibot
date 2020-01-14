package antibot

import org.scalacheck._
import org.scalatest.funsuite._

class AntibotTest extends AnyFunSuite with AntibotSuite {

  test("sanity check") {

    var bots = Map[String, List[(Long, Boolean)]]()
      .withDefaultValue(List())

    def bot(ip: String, times: Port) = {
      logger.trace(s"$ip clicks $times times ...")
      for (n <- 1 to times)
        if (times >= THRESHOLD && n < THRESHOLD) click(ip)
        else bots = bots.updated(ip, click(ip) -> (times >= 20) :: bots(ip))
      true
    }

    Prop.forAll(clicks)(Function.tupled(bot)) check

    logger.trace(s"Bots:\n${bots.mkString("\n")}\n")

    waitStreams()

    assert {
      cassandra.withSessionDo { cass =>
        bots.forall {
          case (ip, event_times) => event_times.forall {
            case (event_time, is_bot) =>
              showFail(s"$ip $event_time $is_bot", cass.execute(
                s"""
                   |select is_bot from antibot.events
                   | where ip = '$ip' and event_time = $event_time
                   | and is_bot = $is_bot allow filtering
                   |""".stripMargin
              ).iterator().hasNext)
          }
        }
      }
    }
  }

}
