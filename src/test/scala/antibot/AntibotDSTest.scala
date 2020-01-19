package antibot

import org.scalatest.funsuite._

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits._
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

  test("dstreams sanity check") {
    testAntibot()
  }
}
