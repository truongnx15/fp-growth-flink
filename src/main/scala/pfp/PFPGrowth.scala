
package pfp

import fpgrowth.Item
import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Class to run Parallel FPGrowth algorithm in flink
 * param: env ExecutionEnvironment Execution environment of flink
 * param: minSupport Double minimum support of result itemsets
 * 
 */

class PFPGrowth(env: ExecutionEnvironment, var minSupport: Double)  {

  //Number of item groups. For our experiment, this should be set equal to number of items However, if number of items are too large
  //It would be set to smaller value. Maybe number of item / 10 or number of item / 20
  //bigger numGroup => smaller items in each group => the computation for each group reduce function in step 4 will be reduced

  var numGroup = env.getParallelism

  def run(data: DataSet[ListBuffer[Item]]) = {

    val minCount: Long = math.ceil(minSupport * data.count()).toLong

   //STEP 2: parallel counting step
    val unsortedList = data
      .flatMap(ParallelCounting.ParallelCountingFlatMap)
      .groupBy(0)
      .reduceGroup(ParallelCounting.ParallelCountingGroupReduce)
      .filter(_.frequency >= minCount)
      .collect()

    var FList = unsortedList.sortWith(_.frequency > _ .frequency)

    println("FLINK ITEM: " + FList.size)
    println("FLINK NumGroup: " + numGroup)

    //glist maps between item and its hashcode
    val gList = mutable.HashMap.empty[Item, Int]

    //Divide items into groups
    var partitionCount: Long = 0
    FList.foreach(
      x => {
        gList += (x -> (partitionCount % numGroup).toInt)
        partitionCount += 1
      }
    )

    //Order is a map from Item to its position(ItemId) in the sorted list of item(in decreasing order of frequency)
    val order = gList.keySet.zipWithIndex.toMap
    //Map from ItemId => Group
    val idToGroupMap = mutable.HashMap.empty[Int, Int]
    //Map from ItemId => Item
    val idToItemMap = order.map(_.swap)

    //Build the idToGroupMap
    gList.foreach(
      mapEntry => {
        idToGroupMap += (order(mapEntry._1) -> mapEntry._2)
      }
    )

    //do not need gList and FList any more
    gList.clear()
    FList = null

    //STEP 4: Parallel FPGrowth: default null key is not necessary
    val frequentItemsets = data
      //Map transaction from list of Item to list Of ItemId(position in the sorted list of item(in decreasing order of frequency)
      .map(x => x.flatMap(order.get))
      //generate conditional transactions for each group
      .flatMap(new ParallelFPGrowth.ParallelFPGrowthExtract(idToGroupMap))
      //Group by group
      .groupBy(0)
      //Build local FP-Tree for each group and mine frequent itemsets
      .reduceGroup(new ParallelFPGrowth.ParallelFPGrowthGroupReduce(idToGroupMap, minCount))
      //Map back from itemId to real item
      .map(new ParallelFPGrowth.ParallelFPGrowthIdToItem(idToItemMap))

    //Return result
    frequentItemsets
  }
}