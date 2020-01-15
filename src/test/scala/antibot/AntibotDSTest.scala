package antibot

import org.scalatest.funsuite._

class AntibotDSTest extends AnyFunSuite with AntibotSuite {

  override def mainArgs(): Array[String] = Array("--ds")

  override def stopAntibot(): Unit = {
    AntiBotDS.stop()
  }

  override def beforeAssert(): Unit = Thread.sleep(1000)

  test("dstreams sanity check") {
    testAntibot()
  }
}
