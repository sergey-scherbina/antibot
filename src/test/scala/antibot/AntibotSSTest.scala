package antibot

import org.scalatest.funsuite._

class AntibotSSTest extends AnyFunSuite with AntibotSuite {
  override def startAntiBot(): Unit = AntiBotSS.main(mainArgs())
  override def stopAntibot(): Unit = AntiBotSS.stop()
  override def beforeStart() = waitStreams(AntiBotSS.queryName)
  override def beforeAssert() = waitStreams(AntiBotSS.queryName)

  test("proof test Structured Stream") {

    val bot1 = "1.2.3.4"
    val bot2 = "5.6.7.8"

    val notBot1 = "10.20.30.40"
    val notBot2 = "50.60.70.80"

    beforeStart()
    bot(bot1)
    notBot(notBot1)
    bot(bot2)
    notBot(notBot2)

    Thread.sleep(1000)
    beforeAssert()

    assertBot(bot1)
    assertBot(bot2)
    assertNotBot(notBot1)
    assertNotBot(notBot2)

    val timePassed = eventTime() + (THRESHOLD.expire.toSeconds * 2)

    click(bot1, timePassed)
    click(bot2, timePassed)
    click(bot1, timePassed + 1)
    click(bot2, timePassed + 1)

    beforeAssert()

    assertClick(bot1, timePassed, false)
    assertClick(bot2, timePassed, false)
    assertClick(bot1, timePassed + 1, false)
    assertClick(bot2, timePassed + 1, false)

  }

}
