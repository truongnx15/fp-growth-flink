package helper

import scala.collection.mutable.HashMap

object ParamHelper {

  val requiredParams = List("--input" , "--support")

  def parseArguments(args: Array[String]): HashMap[String, String] = {
    val paramValues = HashMap[String, String]()

    //Simple parse argument
    var index = 0
    while (index < args.length) {
      val argument = args(index).toLowerCase.trim
      if (requiredParams.contains(argument)) {
        val paramValue = args(index + 1).trim
        paramValues += (argument -> paramValue)
        index += 1
      }
      index += 1
    }

    paramValues
  }
}
