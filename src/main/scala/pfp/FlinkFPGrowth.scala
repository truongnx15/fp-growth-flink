

package pfp


import helper.IOHelperFlink
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * The Example to run Flink FPGrowth
  */

object FlinkFPGrowth {

  def main(args: Array[String]) {

    println("STARTING FPGROWTH IN FLINK")

    //Global variables for Flink and parameter parser
    val parameter = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val itemDelimiter = " "

    //Parse input parameter
    val input = parameter.get("input")
    val minSupport = parameter.get("support")
    val numGroup = parameter.get("group")

    println("input: " + input + " support: " + minSupport + " numGroup: " + numGroup)

    //input and support are required
    if (input == null || input == "" || minSupport == null) {
      println("Please indicate input file and support: --input inputFile --support minSupport")
      return
    }

    //For measuring running time
    val starTime = System.currentTimeMillis()

    val pfp = new PFPGrowth(env, minSupport.toDouble)

    //Set number of group
    if (numGroup != null && numGroup.toInt >=0 ) {
      pfp.numGroup = numGroup.toInt
    }

    //Run FLink FPGrowth
    val data = IOHelperFlink.readInput(env, input, itemDelimiter)
    val frequentItemsets = pfp.run(data)

    //Print number of result
    println("FLINK FPGROWTH: " + frequentItemsets.count())
    println("TIME: " + (System.currentTimeMillis() - starTime) / 1000.0)
  }
}