
package pfp

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.api.scala._

import fpgrowth.Item
import fpgrowth.Itemset

import scala.collection.mutable

/**
 * Class to run Parallel FPGrowth algorithm in flink
 * param: env ExecutionEnvironment Execution environment of flink
 * param: topK Int get top K frequent itemset(this is parameter of Parallel FPGrowth)
 * param: minSupport Double minimum support of result itemsets
 * 
 */

class PFPGrowth(env: ExecutionEnvironment, var minSupport: Double)  {
  
  def run(data: DataSet[Itemset]): List[Itemset] = {

   //STEP 2: parallel counting step
    val unsortedList = data
      .flatMap(ParallelCounting.ParallelCountingFlatMap)
      .groupBy(0)
      .reduceGroup(ParallelCounting.ParallelCountingGroupReduce)
      .collect()

   val minCount: Long = math.ceil(minSupport * data.count()).toLong

    val FList = unsortedList.sortWith(_ > _)
    //STEP 3: Grouping items step
    val numPartition = env.getParallelism

    //glist maps between item and its hashcode
    val gList = mutable.HashMap.empty[Item, Long]

    FList.foreach { x => gList.put(new Item(x.name, x.frequency, 1), x.hashCode % numPartition)}
    val broadcastGList = env.fromCollection(gList)


    //STEP 4: Parallel FPGrowth: default null key is not necessary
    val step4output: DataSet[Itemset] = data
      .flatMap(new ParallelFPGrowth.ParallelFPGrowthflatMap(gList)).withBroadcastSet(broadcastGList, "gList")
      .groupBy(0)
      .reduceGroup(new ParallelFPGrowth.ParallelFPGrowthGroupReduce(gList, minCount)).withBroadcastSet(broadcastGList, "gList")

    //STEP 5:
    val frequentItemsets: List[Itemset] = step4output
      .flatMap(Aggregation.AggregationFlatMap)
      .groupBy(0)
      .reduceGroup(Aggregation.AggregationGroupReduce)
      .collect()
      .toList


    return null
  }
}