package pfp

import fpgrowth.FPGrowth
import helper.{IOHelper, ParamHelper}

/**
  * Class as example to run local FPGrowth
  */

object LocalFPGrowth {
  def main(args: Array[String]): Unit = {

    println("STARTING LOCAL FPGROWTH")

    val parameter = ParamHelper.parseArguments(args)
    val itemDelimiter = " "

    //Parse input parameter
    val input = parameter.getOrElse("--input", null)
    val minSupport = parameter.getOrElse("--support", null)

    println("input: " + input + " support: " + minSupport)

    if (input == null || input == "" || minSupport == null) {
      println("Please indicate input file and support: --input inputFile --support minSupport")
      return
    }

    val transactions = IOHelper.readInput(input, itemDelimiter)
    val minCount: Long = math.ceil(minSupport.toDouble * transactions.size).toLong

    val starTime = System.currentTimeMillis()

    //Init and run FPGrowth
    val sorting: Boolean = true
    val fpGrowthLocal: FPGrowth = new FPGrowth(transactions, minCount, sorting)

    //Result result
    val result = fpGrowthLocal.getFrequentItemsets()
    println("TIME: " + (System.currentTimeMillis() - starTime) / 1000.0)
    println("LOCAL FPGROWTH: " + result.size)
  }
}
