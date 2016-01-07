

package pfp


import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet
import fpgrowth.Itemset
import fpgrowth.Item
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

object PFPGrowthTest {

  def main(args: Array[String]) {

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
    val data: DataSet[Itemset] = env.readTextFile(input)
      .flatMap(new FlatMapFunction[String, Itemset] {
        override def flatMap(line: String, out: Collector[Itemset]): Unit = {
          var itemset: Itemset = new Itemset()
          val items = line.split(itemDelimiter)

          if (items.length > 0) {
            items.foreach { x =>
              itemset.addItem(new Item(x, 0))
            }
            out.collect(itemset)
          }
        }
      })
      
    //Run the PFPGrowth and get list of frequent itemsets
    val frequentItemsets = pfp.run(data)
    
    //frequentItemsets.print()
    
    //TODO: IF HAVE TIME extract association rule
  }
}