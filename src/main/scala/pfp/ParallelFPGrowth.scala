package pfp

import java.lang.Iterable

import fpgrowth.{FPGrowth => FPGrowthLocal, Item, Itemset}
import org.apache.flink.api.common.functions.{FlatMapFunction, GroupReduceFunction}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ParallelFPGrowth {
  class ParallelFPGrowthFlatMap(val gList : mutable.HashMap[Item, Long], val minCount: Long) extends FlatMapFunction[Itemset, (Long, Itemset)] {

    override def flatMap(itemset: Itemset, collector: Collector[(Long, Itemset)]): Unit = {

      val order = gList.keySet.zipWithIndex.toMap

      itemset.sortItems(order)

      var checkingHashMap = gList

      for(j <- (itemset.items.size - 1) to (0, -1)) {

        val hashNum = checkingHashMap.getOrElse(itemset.items(j), null)

        if (hashNum != null) {
          val hashNumLong: Long = hashNum.toString.toLong

          checkingHashMap = checkingHashMap.filter(_._2 != hashNumLong)

          val newItemset = new Itemset()

          newItemset.items = itemset.items.slice(0, j + 1)

          collector.collect((hashNumLong, newItemset))
        }
      }
    }
  }

  class ParallelFPGrowthGroupReduce(val gList: mutable.HashMap[Item, Long], val minCount: Long) extends GroupReduceFunction[(Long, Itemset), Itemset] {
    override def reduce(iterable: Iterable[(Long, Itemset)], collector: Collector[Itemset]): Unit = {

      var transactions = new ListBuffer[Itemset]()
      var groupId: Long = 0

      iterable.foreach(
        tuple => {
          groupId = tuple._1
          transactions += tuple._2
        }
      )

      //Extract now group
      val nowGroup = {
        gList.filter(_._2 == groupId).keys
      }

      val fpGrowthLocal: FPGrowthLocal = new FPGrowthLocal(transactions, minCount, false)

      nowGroup.foreach(
        item => {
          val frequentSets = fpGrowthLocal.extractPattern(fpGrowthLocal.fptree, null, item)
          frequentSets.foreach(collector.collect(_))
        }
      )
    }
  }

}