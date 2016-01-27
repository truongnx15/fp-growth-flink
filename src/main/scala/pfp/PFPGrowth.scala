
package pfp

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.api.scala._

import fpgrowth.Item

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer}

/**
 * Class to run Parallel FPGrowth algorithm in flink
 * param: env ExecutionEnvironment Execution environment of flink
 * param: minSupport Double minimum support of result itemsets
 * 
 */

class PFPGrowth(env: ExecutionEnvironment, var minSupport: Double)  {

  var numPartition = env.getParallelism

  def run(data: DataSet[ListBuffer[Item]]) = {

    val minCount: Long = math.ceil(minSupport * data.count()).toLong

    env.setParallelism(numPartition)

   //STEP 2: parallel counting step
    val unsortedList = data
      .flatMap(ParallelCounting.ParallelCountingFlatMap)
      .groupBy(0)
      .reduceGroup(ParallelCounting.ParallelCountingGroupReduce)
      .filter(_.frequency >= minCount)
      .collect()

    val FList = unsortedList.sortWith(_.frequency > _ .frequency)

    //glist maps between item and its hashcode
    val gList = mutable.HashMap.empty[Item, Int]

    var partitionCount: Long = 0
    FList.foreach(
      x => {
        gList += ((x -> (partitionCount % numPartition).toInt))
        partitionCount += 1
      }
    )

    val order = gList.keySet.zipWithIndex.toMap
    val idToGroupMap = mutable.HashMap.empty[Int, Int]
    val idToItemMap = order.map(_.swap)

    gList.foreach(
      mapEntry => {
        idToGroupMap += (order(mapEntry._1) -> mapEntry._2)
      }
    )

    //do not need gList and FList any more
    gList.clear()

    //STEP 4: Parallel FPGrowth: default null key is not necessary
    val frequentItemIdsets = data
      .flatMap(new ParallelFPGrowth.ParallelFPGrowthFlatMap(idToGroupMap, order))
      .groupBy(0)
      .reduceGroup(new ParallelFPGrowth.ParallelFPGrowthGroupReduce(idToGroupMap, minCount))
      //Map back from itemId to real item


    var frequentItemsets = ListBuffer.empty[(ListBuffer[Item], Int)]

    frequentItemIdsets.collect().foreach(
      frequentIdSet => {
        val itemset = frequentIdSet._1.flatMap(idToItemMap.get)
        frequentItemsets += ((itemset, frequentIdSet._2))
      }
    )

    frequentItemsets
  }
}