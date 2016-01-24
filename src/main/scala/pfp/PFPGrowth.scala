
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
 * param: minSupport Double minimum support of result itemsets
 * 
 */

class PFPGrowth(env: ExecutionEnvironment, var minSupport: Double)  {

  var numPartition = env.getParallelism

  def run(data: DataSet[Itemset]) = {

   //STEP 2: parallel counting step
    val unsortedList = data
      .flatMap(ParallelCounting.ParallelCountingFlatMap)
      .groupBy(0)
      .reduceGroup(ParallelCounting.ParallelCountingGroupReduce)
      .collect()

    val minCount: Long = math.ceil(minSupport * data.count()).toLong

    val FList = unsortedList.sortWith(_.frequency > _ .frequency)


    //STEP 3: Grouping items step

    //glist maps between item and its hashcode
    val gList = mutable.HashMap.empty[Item, Long]

    /*
    val numItemParition = FList.size / numPartition
    var currentPartition: Long = 0
    var currentItem: Long = 0
    FList.foreach(
      x => {
        gList += (x -> currentPartition)
        currentItem += 1
        if (currentItem % numItemParition == 0) {
          currentPartition += 1
        }
      }
    )
    */

    var partitionCount: Long = 0
    FList.foreach(
      x => {
        gList += (x -> (partitionCount % numPartition))
        partitionCount += 1
      }
    )

    //val order = FList.zipWithIndex.toMap

    //STEP 4: Parallel FPGrowth: default null key is not necessary
    val frequentItemsets = data
      .flatMap(new ParallelFPGrowth.ParallelFPGrowthFlatMap(gList, minCount))
      .groupBy(0)
      .reduceGroup(new ParallelFPGrowth.ParallelFPGrowthGroupReduce(gList, minCount))


    //STEP 5:
   /*
    val frequentItemsets: List[Itemset] = step4output
      .flatMap(Aggregation.AggregationFlatMap)
      .groupBy(0)
      .reduceGroup(Aggregation.AggregationGroupReduce)
      .collect()
      .toList
    */

    frequentItemsets.collect()
  }
}