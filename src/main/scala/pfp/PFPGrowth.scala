
package pfp

import org.apache.flink.api.common.operators.Order
import org.apache.hadoop.util.hash.Hash

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.util.Collector

import fpgrowth.Item
import fpgrowth.Itemset

import scala.collection.mutable

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
    
    //STEP 2: parallel counting step
    var unsortedList = data
      .flatMap(ParallelCounting.ParallelCountingFlatMap)
      .groupBy(0)
      .reduceGroup(ParallelCounting.ParallelCountingGroupReduce)
      .collect()



    var FList = unsortedList.sortWith(_ > _)

    //STEP 3: Grouping items step
    var numPartition = env.getParallelism

    //glist map between item and
    var gList = mutable.HashMap.empty[Item, Long]
    FList.map { x => gList.add(new Item(x.name, x.frequency), x.hashCode % numPartition)}

    //STEP 4: Parallel FPGrowth




    //STEP 5:


    return sortedItems
  }
}