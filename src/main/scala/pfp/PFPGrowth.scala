
package pfp

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.util.Collector

import fpgrowth.Item
import fpgrowth.Itemset

/**
 * Class to run Parallel FPGrowoth algorithm in flink
 * @param: env ExecutionEnvironment Execution environment of flink
 * @param: topK Int get top K frequent itemset(this is parameter of Parallel FPGrowth)
 * @param: minSupport Double minimum support of result itemsets
 * 
 */

class PFPGrowths(env: ExecutionEnvironment, var topK: Int, var minSupport: Double)  {
  
  def run(data: DataSet[Itemset]): DataSet[Item] = {
    var frequentItemsets: Array[Itemset] = Array()
    
    var fList = data
      .flatMap(ParalellCounting.ParalellCountingFlatMap)
      .reduceGroup(ParalellCounting.ParalellCountingGroupReduce)
    
    return fList
  }
}