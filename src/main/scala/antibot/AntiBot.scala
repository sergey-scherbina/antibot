package antibot

object AntiBot {

  def main(args: Array[String] = Array()): Unit = {
    if (args != null && args.contains("--ss"))
      AntiBotSS.main(args) else AntiBotDS.main(args)
  }

}
