

package pfp


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

object PFPGrowthExample {

  def main(args: Array[String]) {

    //Global variables for Flink and parameter parser
    val parameter = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val itemDelimiter = " "
    val lineDelimiter = "\n"
    
    //Parse input parameter
    var input: String = parameter.get("input")
    var minSupport: Double = parameter.get("minSupport").toDouble
    
    //Init PFPGrowth algorithm

    //Set minSupport to test
    minSupport = 0.25

    var pfp = new PFPGrowth(env, minSupport)

    //Read dataset
    val data = IOHelper.readInput(env, input, itemDelimiter)

    //Run the PFPGrowth and get list of frequent itemsets
    val frequentItemsets = pfp.run(data)

    //frequentItemsets.foreach(println(_))
    
    println(frequentItemsets.size)
  }
}