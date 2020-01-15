package antibot

import org.scalatest.funsuite._

class AntibotSSTest extends AnyFunSuite with AntibotSuite {

  override def beforeStart() = waitStreams(AntiBotSS.queryName)
  override def beforeAssert() = waitStreams(AntiBotSS.queryName)

  test("structured streaming sanity check") {
    testAntibot()
  }

}
