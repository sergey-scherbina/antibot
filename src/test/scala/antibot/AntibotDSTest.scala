package antibot

import org.scalatest.funsuite._

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.Duration
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits._

class AntibotDSTest extends AnyFunSuite with AntibotSuite {

  override def mainArgs(): Array[String] = Array("--ds")

  override def stopAntibot(): Unit = AntiBotDS.stop()

  def awaitComplete() = {
    val latch = Promise[Unit]()
    AntiBotDS.awaitComplete { _ =>
      if (!latch.isCompleted)
        latch.complete(Try())
    }
    Await.result(latch.future, Duration.Inf)
  }
  
  override def beforeStart(): Unit = awaitComplete()
  override def beforeAssert(): Unit = awaitComplete()

  test("dstreams sanity check") {
    testAntibot()
  }
}
