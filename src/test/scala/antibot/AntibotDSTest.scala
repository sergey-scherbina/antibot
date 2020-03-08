package antibot

import org.scalatest.funsuite._

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.Try

class AntibotDSTest extends AnyFunSuite with AntibotSuite {
  override def startAntiBot(): Unit = AntiBotDS.main(mainArgs())
  override def stopAntibot(): Unit = AntiBotDS.stop()

  def awaitStarted() = {
    logger.trace("Await started")
    val latch = Promise[Unit]()
    AntiBotDS.await(started = _ =>
      if (!latch.isCompleted) latch.complete(Try()))
    Await.result(latch.future, Duration.Inf)
    logger.trace("Started")
  }

  def awaitCompleted() = {
    logger.trace("Await completed")
    val latch = Promise[Unit]()
    AntiBotDS.await(completed = _ =>
      if (!latch.isCompleted) latch.complete(Try()))
    Await.result(latch.future, Duration.Inf)
    logger.trace("Completed")
  }

  override def beforeStart(): Unit = {
    awaitStarted()
    awaitCompleted()
  }
  override def beforeAssert(): Unit = awaitCompleted()

  test("proof test DStream") {

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
