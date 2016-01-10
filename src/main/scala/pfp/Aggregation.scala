package pfp

import java.lang.Iterable

import fpgrowth.{Item, Itemset}
import org.apache.flink.api.common.functions.{GroupReduceFunction, FlatMapFunction}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable


object Aggregation {

  /**
    * Map function for Aggregation algorithm of Parallel FPGrowth
    * @return
    */
  def AggregationFlatMap = new FlatMapFunction[Itemset, (Item, Itemset)] {
    override def flatMap(item: Itemset, collector: Collector[(Item, Itemset)]): Unit = {
      val itemset = item.getItems
      itemset.foreach { x => collector.collect((x, item))}
    }
  }

  class AggregationGroupReduce(var topK: Int) extends GroupReduceFunction[(Item, Itemset), Itemset] {
    override def reduce(iterable: Iterable[(Item, Itemset)], collector: Collector[Itemset]): Unit = {
      val priorityQueue = new mutable.PriorityQueue[Itemset]()

      //Select topK frequent itemset
      iterable.foreach{
        x => {
          val itemset = x._2
          if (priorityQueue.size < topK || topK <= 0) {
            priorityQueue += itemset
          }
          else {
            val topQueue = priorityQueue.head
            if (topQueue.getSupport() < itemset.getSupport()) {
              priorityQueue.dequeue()
              priorityQueue += itemset
            }
          }
        }
      }

      while (!priorityQueue.isEmpty) {
        collector.collect(priorityQueue.dequeue())
      }
    }
  }
}
