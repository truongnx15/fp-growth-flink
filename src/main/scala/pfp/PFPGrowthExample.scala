

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
    val minSupport = 5.0/100

    val starTime = System.currentTimeMillis()

    var pfp = new PFPGrowth(env, minSupport)

    //Read dataset
    val data = IOHelper.readInput(env, "T40I10D100K.dat", itemDelimiter)
    println("DATA: " + data.count())

    //Run the PFPGrowth and get list of frequent itemsets
    val frequentItemsets = pfp.run(data)

    println("TIME: " + (System.currentTimeMillis() - starTime) / 1000.0)

    //frequentItemsets.foreach(println(_))
    
    println(frequentItemsets.size)
  }
}