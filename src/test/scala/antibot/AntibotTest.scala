package antibot

import org.scalacheck._
import org.scalatest.funsuite._

class AntibotTest extends AnyFunSuite with AntibotSuite {

  test("sanity check") {

    Prop.forAll(clicks) { case (ip, times) =>
      println(s"click $ip $times times ...")
      for (_ <- 1 to times) click(ip)
      true
    } check {
      _ withMinSuccessfulTests 300
    }

    {
      val bots = cassandra.withSessionDo(
        _.execute("select * from antibot.events" +
          " where is_bot = true ALLOW FILTERING;"))

      assert(bots.iterator().hasNext)
    }

    {
      val notBots = cassandra.withSessionDo(
        _.execute("select * from antibot.events" +
          " where is_bot = false ALLOW FILTERING;"))

      assert(notBots.iterator().hasNext)
    }
  }

}
