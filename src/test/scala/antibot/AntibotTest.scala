package antibot

import org.scalacheck._
import org.scalatest.funsuite._

class AntibotTest extends AnyFunSuite with AntibotSuite {

  test("sanity check") {

    Prop.forAll(clicks) { case (ip, times) =>
      println(s"$ip clicks $times times ...")
      for (_ <- 1 to times) click(ip)
      true
    } check {
      _ withMinSuccessfulTests 300
    }

    assert(getEvents(false).iterator().hasNext)
    assert(getEvents(true).iterator().hasNext)

  }

}
