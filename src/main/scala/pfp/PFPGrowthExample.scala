

package pfp


import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet
import fpgrowth.Itemset
import fpgrowth.Item
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object PFPGrowthExample {

  def main(args: Array[String]) {

    var a = new ListBuffer[Int]
    a += 1
    a += 2
    println("LIST: " + a)
    var b = a.dropRight(1)
    println("LIST: " + a)
    println("LIST: " + b)

    //Global variables for Flink and parameter parser
    val parameter = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val itemDelimiter = " "
    val lineDelimiter = "\n"
    
    //Parse input parameter
    var input: String = parameter.get("input")
    var topK: Int = parameter.get("topk").toInt
    var minSupport: Double = parameter.get("minSupport").toDouble
    
    //Init PFPGrowth algorithm
    
    var pfp = new PFPGrowth(env, topK, minSupport)

    //Read dataset
    val data = IOHelper.readInput(env, input, itemDelimiter)
      
    //Run the PFPGrowth and get list of frequent itemsets
    val frequentItemsets = pfp.run(data)
    
    //frequentItemsets.print()
    
    //TODO: IF HAVE TIME extract association rule
  }
}