

package pfp


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object PFPGrowthExample {

  def main(args: Array[String]) {

    //Global variables for Flink and parameter parser
    val parameter = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val itemDelimiter = " "
    val lineDelimiter = "\n"

    
    //Parse input parameter
    //var input: String = parameter.get("input")
    //var minSupport: Double = parameter.get("minSupport").toDouble
    
    //Init PFPGrowth algorithm

    //Set minSupport to test
    val minSupport = 0.15

    var pfp = new PFPGrowth(env, minSupport)

    val starTime = System.currentTimeMillis()
    //Read dataset
    val data = IOHelper.readInput(env, "testdata/transactions-3.txt", itemDelimiter)

    //Run the PFPGrowth and get list of frequent itemsets
    val frequentItemsets = pfp.run(data)

    println("TIME: " + (System.currentTimeMillis() - starTime) / 1000.0)

    //frequentItemsets.foreach(println(_))
    
    println(frequentItemsets.size)
  }
}