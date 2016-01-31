package pfp

import fpgrowth.{FPGrowth => FPGrowthLocal}
import org.apache.flink.api.java.utils.ParameterTool

object LocalFPGrowth {
  def main(args: Array[String]): Unit = {

    println("STARTING LOCAL FPGROWTH")

    val parameter = ParameterTool.fromArgs(args)
    val itemDelimiter = " "

    //Parse input parameter
    val input = parameter.get("input")
    val minSupport = parameter.get("support")

    if (input == null || input == "" || minSupport == null) {
      println("Please indicate input file and support: --input inputFile --support minSupport")
      return
    }

    val transactions = IOHelper.readInput(input, itemDelimiter)
    val minCount: Long = math.ceil(minSupport.toDouble * transactions.size).toLong

    val starTime = System.currentTimeMillis()

    //Init and run FPGrowth
    val sorting: Boolean = true
    val fpGrowthLocal: FPGrowthLocal = new FPGrowthLocal(transactions, minCount, sorting)

    //Result result
    val result = fpGrowthLocal.getFrequentItemsets()
    println("TIME: " + (System.currentTimeMillis() - starTime) / 1000.0)
    println("LOCAL FPGROWTH: " + result.size)
  }
}
