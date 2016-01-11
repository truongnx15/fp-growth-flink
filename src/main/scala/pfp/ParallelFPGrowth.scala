import java.lang.Iterable

import fpgrowth.{FPGrowth => FPGrowthLocal, Itemset, Item}
import org.apache.flink.api.common.functions.{GroupReduceFunction, FlatMapFunction}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import org.apache.flink.api.scala._

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

object ParallelFPGrowth {
  class ParallelFPGrowthflatMap(var hashMap : HashMap[Item, Long]) extends FlatMapFunction[Itemset, (Long, Itemset)] {

    override def flatMap(itemset: Itemset, collector: Collector[(Long, Itemset)]): Unit = {

      for(j <- (itemset.items.size - 1) to (0, -1)) {
        var hashNum = hashMap.getOrElse(itemset.items(j), null)
        if (hashNum != null) {

          hashMap = hashMap.filter(_._2 != hashNum)
          var newItemset = new Itemset()

          for(i <- 0 to j) {
            newItemset.addItem(itemset.items(i))
          }

          collector.collect((hashNum.toString.toLong, newItemset))

        }
      }
    }
  }

  class ParallelFPGrowthGroupReduce(var hashMap: HashMap[Item, Long], var minCount: Long) extends GroupReduceFunction[(Long, Itemset), Itemset] {
    override def reduce(iterable: Iterable[(Long, Itemset)], collector: Collector[Itemset]): Unit = {

      var transactions = new ListBuffer[Itemset]()
      var hashValue: Long = 0

      iterable.foreach(
        tuple => {
          hashValue = tuple._1
          transactions += tuple._2
        }
      )

      var nowGroup = hashMap.filter(_._2 == hashValue).map(_._1)

      val fpGrowthLocal: FPGrowthLocal = new FPGrowthLocal(transactions, minCount, true);

      nowGroup.foreach(
        item => {
          val frequentSets = fpGrowthLocal.extractPattern(fpGrowthLocal.fptree, null, item)
          frequentSets.foreach(
            itemset => {collector.collect(itemset)}
          )
        }
      )
    }
  }

}