package antibot

import org.scalatest.funsuite._

class AntibotDSTest extends AnyFunSuite with AntibotSuite {

  override def mainArgs(): Array[String] = Array("--ds")

  test("dstreams sanity check") {
    testAntibot()
  }

}
