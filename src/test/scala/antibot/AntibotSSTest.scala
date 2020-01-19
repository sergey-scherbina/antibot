package antibot

import org.scalatest.funsuite._

class AntibotSSTest extends AnyFunSuite with AntibotSuite {
  override def startAntiBot(): Unit = AntiBotSS.main(mainArgs())
  override def stopAntibot(): Unit = AntiBotSS.stop()
  override def beforeStart() = waitStreams(AntiBotSS.queryName)
  override def beforeAssert() = waitStreams(AntiBotSS.queryName)

  test("structured streaming sanity check") {
    testAntibot()
  }

}
