package helper

import org.apache.commons.cli._

object ParamHelper {
  val options = new Options()
  options.addOption("i", "input", true, "Path to the input file")
  options.addOption("s", "support", true, "Min support")

  def parseArguments(args: Array[String]): CommandLine = {
    new DefaultParser().parse(options, args)
  }

}
