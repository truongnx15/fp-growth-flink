package pfp

import java.lang.Iterable

import fpgrowth.{FPGrowth => FPGrowthLocal, Item}
import org.apache.flink.api.common.functions.{FlatMapFunction, GroupReduceFunction}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object ParallelFPGrowth {

  /**
    * Mapper in step4. The idea is to generate independent conditional based itemset. Each itemset has its own order based on frequency
    * Item with highest frequency has order 0, Item with second frequency has order 1 .....
    * From now on, item is represented by their order(called ItemId). An itemset is a list of itemId
    * => Sorting by frequency is sorting itemset itemId
    * @param idToGroupMap
    * @param order
    * @param gList
    * @param minCount
    */
  class ParallelFPGrowthFlatMap(val idToGroupMap : mutable.HashMap[Int, Int], val order : Map[Item, Int], val gList: mutable.HashMap[Item, Int], val minCount: Long) extends FlatMapFunction[ArrayBuffer[Item], (Int, ArrayBuffer[Int])] {

    override def flatMap(itemset: ArrayBuffer[Item], collector: Collector[(Int, ArrayBuffer[Int])]): Unit = {

      //Check if the current group has been processed
      var outputGroup = Set[Int]()
      //Extract itemId from itemset and sort frequency in increasing order
      val itemIds = itemset.flatMap(order.get).sortWith( _ > _)

      for(j <- (itemIds.size - 1) to (0, -1)) {
        val itemId = itemIds(j)
        val groupId = idToGroupMap(itemId)

        if (!outputGroup.contains(groupId)) {
          outputGroup += groupId
          val tmp = itemIds.slice(0, j + 1)
          collector.collect(groupId, itemIds.slice(0, j + 1))
        }
      }
    }
  }

  class ParallelFPGrowthGroupReduce(val idToGroupMap: mutable.HashMap[Int, Int], val minCount: Long) extends GroupReduceFunction[(Int, ArrayBuffer[Int]), (ArrayBuffer[Int], Int)] {
    override def reduce(iterable: Iterable[(Int, ArrayBuffer[Int])], collector: Collector[(ArrayBuffer[Int], Int)]): Unit = {

      var groupId: Long = 0

      val fpGrowthLocal: FPGrowthLocal = new FPGrowthLocal(null, minCount, false)

      iterable.foreach(
        tuple => {
          groupId = tuple._1
          fpGrowthLocal.fptree.addTransaction(tuple._2, 1)
        }
      )

      //Extract now group
      val nowGroup = {
        idToGroupMap.filter(_._2 == groupId).keys
      }

      nowGroup.foreach(
        item => {
          val frequentSets = fpGrowthLocal.extractPattern(fpGrowthLocal.fptree, null, item)
          frequentSets.foreach(collector.collect(_))
        }
      )
    }
  }

}